/**
 * 市場全体指標 日次更新スクリプト（GH Actions用）
 *
 * @description
 * analytics.market_indicators をソース単位の forward-fill で埋める（自己修復型）。
 * カーソルは「行の最新日」ではなく、担当カラムの NULL 検出で持つ
 * （ワイド型テーブルに複数ソースが書くため。部分失敗した日を後続実行が埋め直す）。
 *
 * ソースグループ（担当カラムのみを同一shapeでupsertし、他グループの列は触らない）:
 *   1. breadth: equity_bar_daily(session='DAY') から自前計算
 *      → advancers/decliners/unchanged/new_highs/new_lows/prime_turnover_value
 *      → adv_dec_ratio_25d は保存済みの騰落数25日窓から導出
 *   2. yahoo:   nikkei_close / nikkei_open / nikkei_high / nikkei_low（^N225 日足）
 *   3. daily2:  nikkei_per / short_selling_ratio（nikkei225jp.com。終値をYahoo保存値と突合）
 *   4. weekly:  margin_pl_ratio（nikkei225jp.com 週次。ソース側に存在する日付のみ対象）
 *   5. derived: nikkei_eps(=close/per) / topix_close / nt_ratio
 *
 * - job_runs は使わない（upsert 冪等・NULL検出で自己修復。refresh-technical と同方針）
 * - 外部ソースの失敗は該当グループのみ欠損させ、他グループは続行する
 * - 通常運転は直近 WINDOW_DAYS 日のみ走査。--full で系列開始日から全走査
 *   （ただし breadth/外部の対象は equity_bar_daily が存在する日付に限る。
 *    それ以前の履歴は scripts/seed/market-indicators.ts が担当）
 *
 * 実行: npx tsx scripts/cron/refresh-market-indicators.ts [--dry-run] [--full]
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { getJSTDate, getJSTDateTime } from '../../src/lib/utils/date';
import {
  BreadthAccumulator,
  includesPreviousYear,
  PRIME_MARKET_CODES,
  type BreadthBar,
} from '../../src/lib/analytics/market-breadth';
import {
  SERIES_START,
  type Client,
  type IndicatorRow,
  toNum,
  getOrCreate,
  loadExistingRows,
  upsertRows,
  fillYahoo,
  fillDaily2,
  fillWeekly,
  fillDerived,
  fillRatio,
} from '../../src/lib/market/indicators-sync';

/** 通常運転の走査窓（暦日）。祝日連休を挟んでも25営業日窓の再計算に足りる幅 */
const WINDOW_DAYS = 45;
/** breadth の公開最低カバレッジ（当日バーのあるプライム銘柄割合） */
const MIN_COVERAGE = 0.8;
/** equity_bar_daily の DB 収録開始日（それ以前の履歴は seed スクリプト担当） */
const DB_BARS_START = '2025-01-27';

const logger = createLogger({ module: 'refresh-market-indicators' });

function validateEnv(): void {
  const required = ['NEXT_PUBLIC_SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

function addDays(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

async function main(): Promise<void> {
  validateEnv();
  const dryRun = process.argv.includes('--dry-run');
  const full = process.argv.includes('--full');
  // cron-a.yml では breadth（株価/マスタ同期成功が前提）と external（独立）を
  // 別ステップで実行するためのグループ選択フラグ
  const onlyBreadth = process.argv.includes('--only-breadth');
  const skipBreadth = process.argv.includes('--skip-breadth');
  if (onlyBreadth && skipBreadth) throw new Error('--only-breadth と --skip-breadth は併用不可');
  if (dryRun) logger.info('DRY RUN: 読み取り＋計算のみ。書き込みは行わない');

  const core = createAdminClient('jquants_core');
  const analytics = createAdminClient('analytics');

  // 1) 終端日を用途別に分離:
  //    - breadth: equity_bar_daily の最新 trade_date（当日株価が無いと計算不能）
  //    - external/derived: JST今日以前の最新営業日（株価同期の失敗に巻き込まれない）
  const { data: latestRow, error: latestErr } = await core
    .from('equity_bar_daily')
    .select('trade_date')
    .eq('session', 'DAY')
    .order('trade_date', { ascending: false })
    .limit(1)
    .single();
  if (latestErr || !latestRow) {
    throw new Error(`Failed to get latest trade_date: ${latestErr?.message ?? 'no rows'}`);
  }
  const breadthEnd = (latestRow as { trade_date: string }).trade_date;
  // 場中（大引け前）に実行された場合、外部ソースの当日行は「場中スナップショット」で
  // あり確定値ではない。NULL検出方式では一度書くと夜の正規実行で上書きされないため、
  // JST16時前は external の対象を前日までに制限する（定時実行はJST18:40なので影響なし）。
  // externalCap は breadthEnd とは独立に適用する（当日バーが存在してもガードを維持）。
  const today = getJSTDate();
  const jstHour = Number(getJSTDateTime().slice(11, 13));
  const externalCap = jstHour >= 16 ? today : addDays(today, -1);
  const scanEnd = breadthEnd > externalCap ? breadthEnd : externalCap;
  const windowStart = full
    ? SERIES_START
    : addDays(breadthEnd < externalCap ? breadthEnd : externalCap, -WINDOW_DAYS);
  logger.info('Scan window', { windowStart, breadthEnd, externalCap, full });

  // 2) 窓内の営業日と保存済み行。騰落レシオの25日窓用に、正準営業日軸は
  //    窓より70暦日（>24営業日）前から持つ。
  const calendarDays = await listBusinessDays(core, windowStart, scanEnd);
  const externalDays = calendarDays.filter((d) => d <= externalCap);
  const breadthDays = calendarDays.filter((d) => d <= breadthEnd);
  const ratioDays = full
    ? breadthDays
    : (await listBusinessDays(core, addDays(windowStart, -70), breadthEnd));
  const rowMap = await loadExistingRows(analytics, windowStart);
  logger.info('Loaded state', {
    breadthDays: breadthDays.length,
    externalDays: externalDays.length,
    existingRows: rowMap.size,
  });

  const summary: Record<string, unknown> = { windowStart, breadthEnd, externalCap, dryRun };
  const failures: string[] = [];

  // 3) 各ソースグループを独立に forward-fill（外部起因の失敗は他グループを止めない）
  if (!skipBreadth) {
    try {
      summary.breadth = await fillBreadth(core, analytics, breadthDays, ratioDays, rowMap, dryRun);
    } catch (e) {
      failures.push(`breadth: ${message(e)}`);
    }
  }
  if (!onlyBreadth) {
    try {
      summary.yahoo = await fillYahoo(analytics, externalDays, rowMap, dryRun);
    } catch (e) {
      failures.push(`yahoo: ${message(e)}`);
    }
    try {
      summary.daily2 = await fillDaily2(analytics, externalDays, rowMap, dryRun);
    } catch (e) {
      failures.push(`daily2: ${message(e)}`);
    }
    try {
      summary.weekly = await fillWeekly(analytics, windowStart, externalCap, rowMap, dryRun);
    } catch (e) {
      failures.push(`weekly: ${message(e)}`);
    }
    try {
      summary.derived = await fillDerived(core, analytics, externalDays, rowMap, dryRun);
    } catch (e) {
      failures.push(`derived: ${message(e)}`);
    }
  }

  summary.failures = failures;
  console.log(JSON.stringify({ success: failures.length === 0, ...summary }, null, 2));
  if (failures.length > 0) {
    throw new Error(`Some source groups failed: ${failures.join(' / ')}`);
  }
}

/**
 * 窓内の営業日リスト（trading_calendar、昇順）
 *
 * NOTE: trading_calendar は 2025-01-22 以降のみ収録。それ以前の履歴は
 * seed スクリプトが担当するため、本スクリプトの走査対象としては十分。
 * 営業日でも equity_bar_daily 未同期の日は breadth が pending のまま残り、
 * バー到着後の実行で自己修復される（意図した挙動）。
 */
async function listBusinessDays(core: Client, from: string, to: string): Promise<string[]> {
  const days: string[] = [];
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await core
      .from('trading_calendar')
      .select('calendar_date')
      .eq('is_business_day', true)
      .gte('calendar_date', from)
      .lte('calendar_date', to)
      .order('calendar_date', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) throw new Error(`Failed to load trading_calendar: ${error.message}`);
    const rows = (data as { calendar_date: string }[] | null) ?? [];
    for (const r of rows) days.push(r.calendar_date);
    if (rows.length < PAGE) break;
  }
  return days;
}

// ============================================================
// breadth（equity_bar_daily から自前計算）
// ============================================================
async function fillBreadth(
  core: Client,
  analytics: Client,
  businessDays: string[],
  ratioDays: string[],
  rowMap: Map<string, IndicatorRow>,
  dryRun: boolean
): Promise<Record<string, unknown>> {
  const pending = businessDays.filter((d) => {
    const r = rowMap.get(d);
    return (
      !r ||
      r.advancers == null ||
      r.decliners == null ||
      r.unchanged == null ||
      r.new_highs == null ||
      r.new_lows == null ||
      r.prime_turnover_value == null
    );
  });
  if (pending.length === 0) {
    const ratioUpdated = await fillRatio(analytics, ratioDays, rowMap, dryRun);
    return { pending: 0, upserted: 0, ratioUpdated };
  }
  logger.info('Breadth pending dates', { count: pending.length, first: pending[0] });

  // 現行マスタからプライムユニバース（過去数十日の再計算にも現行マスタを使う近似。
  // 異動は月数銘柄オーダーでカウントへの影響は軽微）
  const primeSet = await loadCurrentPrimeSet(core);
  if (primeSet.size === 0) throw new Error('Prime universe is empty');
  const minCoverage = Math.floor(primeSet.size * MIN_COVERAGE);

  // 基準期間の先頭（最古 pending 日の年初/前年初）からバーを流し込み、状態を構築。
  // equity_bar_daily の DB 収録は 2025-01-27 以降のため、それ以前が必要な場合は
  // 不完全な基準期間となる（そのケースの履歴は seed スクリプトが担当）。
  const oldest = pending[0];
  const baselineYear = Number(oldest.slice(0, 4)) - (includesPreviousYear(oldest) ? 1 : 0);
  const feedStart = `${baselineYear}-01-01`;
  const feedEnd = pending[pending.length - 1];
  const feedDays = await listBusinessDays(core, feedStart, feedEnd);
  logger.info('Feeding bars', { feedStart, feedEnd, feedDays: feedDays.length });

  const acc = new BreadthAccumulator();
  const pendingSet = new Set(pending);
  const upserts: Array<Record<string, unknown> & { as_of_date: string }> = [];
  let haltedAt: string | null = null;

  for (const date of feedDays) {
    const bars = await fetchBarsForDate(core, date);
    if (bars.length === 0 && date < DB_BARS_START) continue; // DB収録前の暦日（seed担当領域）
    const day = bars.length > 0 ? acc.addDay(date, bars, primeSet) : null;
    // 未同期(0件)・カバレッジ不足の営業日を検出したら、その日以降の出力を停止する。
    // 不完全な前日状態で計算した後続日を非NULLで固定化しない（当該日修復後の次回実行で
    // フィード全体を再構築して自己修復する）。
    if (day == null || day.primeBarCount < minCoverage) {
      haltedAt = date;
      break;
    }
    if (!pendingSet.has(date)) continue;
    upserts.push({
      as_of_date: date,
      advancers: day.advancers,
      decliners: day.decliners,
      unchanged: day.unchanged,
      new_highs: day.newHighs,
      new_lows: day.newLows,
      prime_turnover_value: Math.round(day.turnoverValue),
      updated_at: new Date().toISOString(),
    });
  }

  if (haltedAt) {
    logger.warn('Breadth halted at incomplete day (missing bars or low coverage). 以降は次回実行で自己修復', {
      haltedAt,
    });
  }
  const upserted = await upsertRows(analytics, upserts, dryRun);
  // rowMap への反映は upsert 成功後（書き込み失敗時に ratio へ未永続化の騰落数を使わせない）
  for (const u of upserts) {
    const row = getOrCreate(rowMap, u.as_of_date);
    row.advancers = u.advancers as number;
    row.decliners = u.decliners as number;
    row.unchanged = u.unchanged as number;
    row.new_highs = u.new_highs as number;
    row.new_lows = u.new_lows as number;
    row.prime_turnover_value = u.prime_turnover_value as number;
  }
  // 今回埋め直した日付を窓に含む後続日は既存 ratio があっても再計算する（欠損修復の伝播）
  const newlyFilled = new Set(upserts.map((u) => u.as_of_date));
  const ratioUpdated = await fillRatio(analytics, ratioDays, rowMap, dryRun, newlyFilled);
  return { pending: pending.length, upserted, haltedAt, ratioUpdated };
}

/** 現行 equity_master からプライム＝旧一部の local_code 集合を取得 */
async function loadCurrentPrimeSet(core: Client): Promise<Set<string>> {
  const set = new Set<string>();
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await core
      .from('equity_master')
      .select('local_code, market_code')
      .eq('is_current', true)
      .order('local_code', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) throw new Error(`Failed to load equity_master: ${error.message}`);
    const rows = (data as { local_code: string; market_code: string | null }[] | null) ?? [];
    for (const r of rows) {
      if (r.market_code && PRIME_MARKET_CODES.has(r.market_code)) set.add(r.local_code);
    }
    if (rows.length < PAGE) break;
  }
  return set;
}

/**
 * 指定日の全銘柄バーを取得（trade_date 等値＋local_code 順のページング）
 *
 * NOTE: trade_date の範囲指定＋(trade_date, local_code) ソートは既存インデックスに
 * 乗らず statement timeout するため（実測）、日付等値クエリを営業日ごとに発行する。
 * 負荷パターンは refresh-technical.ts の per-code fetch と同等オーダー。
 */
async function fetchBarsForDate(core: Client, date: string): Promise<BreadthBar[]> {
  const bars: BreadthBar[] = [];
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await core
      .from('equity_bar_daily')
      .select('local_code, adj_close, adj_high, adj_low, turnover_value')
      .eq('trade_date', date)
      .eq('session', 'DAY')
      .order('local_code', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) throw new Error(`Failed to fetch bars for ${date}: ${error.message}`);
    const rows =
      (data as Array<{
        local_code: string;
        adj_close: unknown;
        adj_high: unknown;
        adj_low: unknown;
        turnover_value: unknown;
      }> | null) ?? [];
    for (const r of rows) {
      bars.push({
        code: r.local_code,
        adjClose: toNum(r.adj_close),
        adjHigh: toNum(r.adj_high),
        adjLow: toNum(r.adj_low),
        turnoverValue: toNum(r.turnover_value),
      });
    }
    if (rows.length < PAGE) break;
  }
  return bars;
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    logger.error('Script failed', { error: message(error) });
    process.exit(1);
  });
