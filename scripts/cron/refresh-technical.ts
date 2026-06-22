/**
 * テクニカル指標スナップショット 再計算スクリプト（GH Actions用）
 *
 * @description
 * equity_bar_daily(adj系) から全銘柄のテクニカル指標「最新値」を計算し、
 * analytics.technical_metrics へ upsert する。最新 as_of_date のみ保持（過去日は削除）。
 * Cron A（生データ同期）完了後に cron-a.yml の continue-on-error ステップとして実行する。
 *
 * - チャートの指標「時系列」は価格から Portfolio フロントで都度計算するため保存しない。
 *   本テーブルはスクリーナー横断比較＆銘柄カードの「現在の状態」表示専用。
 * - job_runs / job_locks は使わない（upsert は冪等。cron-a-direct.ts と同方針）。
 * - 読みは jquants_core（READ のみ）、書きは analytics の新テーブルのみ → 既存に非干渉。
 *
 * 公開の原子性（部分スナップショットを見せない）:
 *   1. 全銘柄を計算してから一括 upsert（continueOnError=false で fail-fast）。
 *   2. upsert 全成功時のみ旧 as_of_date を削除（前日完全スナップショットを温存）。
 *   3. upsert 途中失敗時は本日分(asOfDate)を削除してロールバックし、前日分を残す。
 *   4. fetch エラーは skip せず throw。完全性チェック（件数下限）に満たなければ公開しない。
 *
 * 実行: npx tsx scripts/cron/refresh-technical.ts [--dry-run] [--limit=N]
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { batchUpsert, batchProcess } from '../../src/lib/utils/batch';
import {
  computeTechnicalSnapshot,
  type Bar,
  type TechnicalSnapshot,
} from '../../src/lib/analytics/technical';

/** 指標計算に使う直近バー数。Portfolio(CHART_WINDOW=400)と揃え、シード依存の差異を避ける。 */
const LOOKBACK_BARS = 400;
/** 銘柄ごとのDB取得の同時実行数 */
const FETCH_CONCURRENCY = 12;
/** 公開を許可する最低カバレッジ（対象銘柄に対する計算成功割合）。大量欠損時は公開しない。 */
const MIN_COVERAGE = 0.8;

interface BarRow {
  trade_date: string;
  adj_open: number | null;
  adj_high: number | null;
  adj_low: number | null;
  adj_close: number | null;
  adj_volume: number | null;
}

type TechnicalRow = TechnicalSnapshot & {
  as_of_date: string;
  local_code: string;
  updated_at: string;
};

function validateEnv(): void {
  const required = ['NEXT_PUBLIC_SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

async function main(): Promise<void> {
  validateEnv();
  const dryRun = process.argv.includes('--dry-run');
  const logger = createLogger({ module: 'refresh-technical' });
  if (dryRun) logger.info('DRY RUN: 読み取り＋計算のみ。書き込みは行わない');

  const core = createAdminClient('jquants_core');
  const analytics = createAdminClient('analytics');

  // 1) 最新立会日を特定
  const { data: latestRow, error: latestErr } = await core
    .from('equity_bar_daily')
    .select('trade_date')
    .order('trade_date', { ascending: false })
    .limit(1)
    .single();
  if (latestErr || !latestRow) {
    throw new Error(`Failed to get latest trade_date: ${latestErr?.message ?? 'no rows'}`);
  }
  const asOfDate = (latestRow as { trade_date: string }).trade_date;
  logger.info('Latest trade date', { asOfDate });

  // 2) 最新立会日にバーがある銘柄を decisive にページング取得
  //    （order(local_code) で安定・重複/欠落回避、session='DAY' で対象集合を固定）
  // --limit N: 検証用に対象銘柄数を制限。部分公開を防ぐため --dry-run 必須。
  const limitArg = process.argv.find((a) => a.startsWith('--limit='));
  if (limitArg && !dryRun) {
    throw new Error('--limit は --dry-run と併用する検証専用オプションです（本番公開を防止）');
  }

  let codes = await listCodesForDate(core, asOfDate);

  if (limitArg) {
    const n = Number(limitArg.split('=')[1]);
    if (Number.isFinite(n) && n > 0) codes = codes.slice(0, n);
  } else {
    // 本番のみ: 当日の母集団が現行マスタに対して十分か（equity_bars 部分ロード検出）。
    // listCodesForDate の母数は当日株価そのものなので、独立基準(現行マスタ件数)と比較する。
    const { count: masterCount, error: mcErr } = await core
      .from('equity_master')
      .select('*', { count: 'exact', head: true })
      .eq('is_current', true);
    if (mcErr) throw new Error(`Failed to count master: ${mcErr.message}`);
    const expected = masterCount ?? 0;
    if (expected > 0 && codes.length < Math.floor(expected * MIN_COVERAGE)) {
      throw new Error(
        `Universe too small: ${codes.length} codes vs master ${expected} (< ${MIN_COVERAGE}). ` +
          `当日株価が部分ロードの可能性。公開を中止。`
      );
    }
  }
  logger.info('Target codes', { count: codes.length });
  if (codes.length === 0) {
    console.log(JSON.stringify({ success: true, asOfDate, targetCodes: 0, upserted: 0 }));
    return;
  }

  // 3) 銘柄ごとに直近 LOOKBACK_BARS 本を取得 → スナップショット計算（fetch エラーは throw）
  const computed = await batchProcess(
    codes,
    (code) => computeForCode(core, code, asOfDate),
    FETCH_CONCURRENCY
  );
  const rows = computed.filter((r): r is TechnicalRow => r !== null);
  const skipped = codes.length - rows.length;
  logger.info('Computed snapshots', { rows: rows.length, skipped });

  // 完全性チェック: 大量欠損なら公開しない（部分スナップショットを見せない）
  const minRows = Math.max(1, Math.floor(codes.length * MIN_COVERAGE));
  if (rows.length < minRows) {
    throw new Error(
      `Coverage too low: ${rows.length}/${codes.length} (< ${minRows}). 公開を中止。`
    );
  }

  if (dryRun) {
    console.log(
      JSON.stringify(
        { dryRun: true, asOfDate, targetCodes: codes.length, wouldUpsert: rows.length, skipped, sample: rows.slice(0, 3) },
        null,
        2
      )
    );
    return;
  }

  // 再実行判定: この asOfDate の完全スナップショットが既に存在するか（週末再実行/手動再dispatch）。
  // 既存ありなら upsert は冪等更新（同入力→同値）なので、途中失敗でも各行は完全なまま。
  // → 既存完全スナップショットを壊さないよう、ロールバック削除は「新規日付」の時だけ行う。
  const { count: existingCount, error: existErr } = await analytics
    .from('technical_metrics')
    .select('*', { count: 'exact', head: true })
    .eq('as_of_date', asOfDate);
  if (existErr) throw new Error(`Failed to probe existing snapshot: ${existErr.message}`);
  const isRepublish = (existingCount ?? 0) > 0;

  // 4) 一括 upsert（fail-fast）→ 成功時のみ旧日付を削除。
  let upserted = 0;
  try {
    const upsertResult = await batchUpsert(analytics, 'technical_metrics', rows, 'as_of_date,local_code');
    upserted = upsertResult.inserted;
    logger.info('Upsert done', { inserted: upserted, batches: upsertResult.batchCount, isRepublish });
  } catch (e) {
    logger.error('Upsert failed', {
      error: e instanceof Error ? e.message : String(e),
      isRepublish,
    });
    // 新規日付の部分書き込みのみロールバック（前日の完全スナップショットを残す）。
    // 再実行(isRepublish)時は既存の完全スナップショットを壊さないため削除しない。
    if (!isRepublish) {
      const { error: rbErr } = await analytics
        .from('technical_metrics')
        .delete()
        .eq('as_of_date', asOfDate);
      if (rbErr) logger.error('Rollback delete failed', { error: rbErr.message });
    }
    throw e;
  }

  // 5) 公開マーカーを asOfDate に原子的に切り替え（単一行 UPDATE）。
  //    これ以前は view が前日（完全）を見続け、部分公開ウィンドウを作らない。
  const { error: pubErr } = await analytics
    .from('technical_publication')
    .upsert(
      { id: true, published_as_of_date: asOfDate, updated_at: new Date().toISOString() },
      { onConflict: 'id' }
    );
  if (pubErr) {
    // 公開切替に失敗: view は旧公開日（完全）を指したまま。prune せず次回再実行で自己修復。
    logger.error('Failed to flip publication marker; keeping previous published date', {
      error: pubErr.message,
    });
    console.log(JSON.stringify({ success: false, asOfDate, upserted, skipped, published: false }));
    throw new Error(`Publication flip failed: ${pubErr.message}`);
  }

  // 6) 公開切替成功後にのみ旧 as_of_date を削除（最新スナップショットのみ保持）。失敗は非クリティカル。
  const { error: delErr, count: delCount } = await analytics
    .from('technical_metrics')
    .delete({ count: 'exact' })
    .lt('as_of_date', asOfDate);
  if (delErr) {
    logger.warn('Failed to prune old snapshots', { error: delErr.message });
  } else {
    logger.info('Pruned old snapshots', { deleted: delCount ?? 0 });
  }

  console.log(
    JSON.stringify(
      {
        success: true,
        asOfDate,
        targetCodes: codes.length,
        upserted,
        skipped,
        published: true,
        prunedOld: delCount ?? 0,
      },
      null,
      2
    )
  );
}

/** 指定日にバーがある銘柄を order(local_code) で安定ページング取得（session='DAY' 限定） */
async function listCodesForDate(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  core: any,
  asOfDate: string
): Promise<string[]> {
  const codes: string[] = [];
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await core
      .from('equity_bar_daily')
      .select('local_code')
      .eq('trade_date', asOfDate)
      .eq('session', 'DAY')
      .order('local_code', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) throw new Error(`Failed to list codes: ${error.message}`);
    const rows = (data as { local_code: string }[] | null) ?? [];
    for (const r of rows) codes.push(r.local_code);
    if (rows.length < PAGE) break;
  }
  return codes;
}

/** 1銘柄の直近バーを取得しスナップショット計算。fetch エラーは1回リトライ後 throw。 */
async function computeForCode(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  core: any,
  code: string,
  asOfDate: string
): Promise<TechnicalRow | null> {
  let lastErr: { message: string } | null = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    const { data, error } = await core
      .from('equity_bar_daily')
      .select('trade_date, adj_open, adj_high, adj_low, adj_close, adj_volume')
      .eq('local_code', code)
      .eq('session', 'DAY')
      .order('trade_date', { ascending: false })
      .limit(LOOKBACK_BARS);
    if (!error && data) {
      const rows = (data as BarRow[]).reverse(); // 昇順（古い→新しい）
      // 有効な adj_close 行のみ（null は除外）
      const valid = rows.filter((r) => r.adj_close != null);
      if (valid.length === 0) return null; // データなし → skip
      // 最新有効バーが asOfDate でなければ（最新日の close が null 等）日付ズレ回避で skip
      if (valid[valid.length - 1].trade_date !== asOfDate) return null;
      const bars: Bar[] = valid.map((r) => ({
        open: Number(r.adj_open ?? r.adj_close),
        high: Number(r.adj_high ?? r.adj_close),
        low: Number(r.adj_low ?? r.adj_close),
        close: Number(r.adj_close),
        volume: Number(r.adj_volume ?? 0),
      }));
      const snap = computeTechnicalSnapshot(bars);
      if (!snap) return null;
      return { as_of_date: asOfDate, local_code: code, ...snap, updated_at: new Date().toISOString() };
    }
    lastErr = error;
  }
  throw new Error(`Fetch failed for ${code}: ${lastErr?.message ?? 'unknown'}`);
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    const logger = createLogger({ module: 'refresh-technical' });
    logger.error('Script failed', { error: error instanceof Error ? error.message : String(error) });
    process.exit(1);
  });
