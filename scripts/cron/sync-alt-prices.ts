/**
 * J-Quants 代替価格の日次同期（Yahoo → alt_equity_bar_daily / TOPIX 推計）
 *
 * @description
 * 保有 ∪ ウォッチ銘柄と固定ETF（1306・マクロ対立軸8本）の日足を Yahoo から取り、
 * alt_equity_bar_daily と alt_split_events へ書く。J-Quants の equity_bar_daily には書かない。
 * 設計正本: ../../../docs/PLANS-jquants-off-switch-2026-10.md §4.2・§4.3
 *
 * モード別の挙動:
 * - OFF: 代替価格を書き、TOPIX 推計行（source='proxy_etf_13060'）を topix_bar_daily へ書く。
 * - ON（シャドー運転）: 代替価格は書く（統合ビューは公式行を優先するので画面は変わらない）。
 *   TOPIX は書かず、直近の公式値と推計値の差をログに出して突き合わせる。
 *
 * 分割: 新しい分割イベントを見つけた銘柄は、代替期間の全体を取り直して調整値の基準を揃える。
 * 異常値: 前日比±30%超で分割イベントの無い日は採用せず、注意メールを送る。
 * いずれかの銘柄の取得に失敗したら exit 1（ワークフローの失敗通知で気づけるように）。
 *
 * 実行: npx tsx scripts/cron/sync-alt-prices.ts [--dry-run]
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { addDays, getJSTDate } from '../../src/lib/utils/date';
import { getJQuantsMode } from '../../src/lib/data-source/jquants-mode';
import { getTrackedLocalCodes } from '../../src/lib/analytics/tracked-codes';
import { fetchYahooEquityChart, toYahooSymbol, type YahooSplitEvent } from '../../src/lib/market/yahoo-equity-client';
import {
  FIXED_ALT_CODES,
  TOPIX_PROXY_ETF_CODE,
  computeTopixProxy,
  filterAnomalies,
  toAltBarRecords,
  type RejectedBar,
} from '../../src/lib/market/alt-prices';
import { sendOpsNoticeEmail } from '../../src/lib/notification/email';

const logger = createLogger({ module: 'sync-alt-prices' });

/** 通常の取得窓（暦日）。祝日連休を挟んでも直近の数営業日を取り直せる幅 */
const WINDOW_DAYS = 35;
/** ON 中の TOPIX 突き合わせで使う日数 */
const COMPARE_DAYS = 20;

type Core = ReturnType<typeof createAdminClient>;

async function loadKnownSplits(core: Core, codes: string[]): Promise<Map<string, YahooSplitEvent[]>> {
  const { data, error } = await core
    .from('alt_split_events')
    .select('local_code, ex_date, ratio')
    .in('local_code', codes);
  if (error) throw new Error(`alt_split_events の取得に失敗しました: ${error.message}`);
  const map = new Map<string, YahooSplitEvent[]>();
  for (const r of (data ?? []) as Array<{ local_code: string; ex_date: string; ratio: string | number }>) {
    const list = map.get(r.local_code) ?? [];
    list.push({ exDate: r.ex_date, ratio: Number(r.ratio) });
    map.set(r.local_code, list);
  }
  return map;
}

async function earliestAltDate(core: Core, code: string): Promise<string | null> {
  const { data, error } = await core
    .from('alt_equity_bar_daily')
    .select('trade_date')
    .eq('local_code', code)
    .order('trade_date', { ascending: true })
    .limit(1);
  if (error) throw new Error(`alt_equity_bar_daily の取得に失敗しました: ${error.message}`);
  return (data?.[0] as { trade_date: string } | undefined)?.trade_date ?? null;
}

/** 期間より前の最後の調整後終値（統合ビュー。公式・代替のどちらでもよい） */
async function prevAdjClose(core: Core, code: string, before: string): Promise<number | null> {
  const { data, error } = await core
    .from('v_equity_price_daily')
    .select('adj_close')
    .eq('local_code', code)
    .lt('trade_date', before)
    .order('trade_date', { ascending: false })
    .limit(1);
  if (error) throw new Error(`v_equity_price_daily の取得に失敗しました: ${error.message}`);
  const v = (data?.[0] as { adj_close: string | number | null } | undefined)?.adj_close;
  return v == null ? null : Number(v);
}

interface CodeResult {
  code: string;
  rows: number;
  rejected: RejectedBar[];
  newSplits: YahooSplitEvent[];
  error?: string;
}

async function syncCode(
  core: Core,
  code: string,
  today: string,
  knownSplits: YahooSplitEvent[],
  dryRun: boolean
): Promise<CodeResult> {
  const symbol = toYahooSymbol(code);
  if (!symbol) {
    return { code, rows: 0, rejected: [], newSplits: [], error: 'Yahoo シンボルに変換できないコード' };
  }

  let from = addDays(today, -WINDOW_DAYS);
  let chart = await fetchYahooEquityChart(symbol, from, today);
  const known = new Set(knownSplits.map((s) => s.exDate));
  const newSplits = chart.splits.filter((s) => !known.has(s.exDate));

  if (newSplits.length > 0) {
    // 保存済みの代替期間の全体を取り直し、分割後の基準で調整値を揃える
    // （取り直さないと、窓より前の代替行だけが分割前の基準のまま残る）
    const earliest = await earliestAltDate(core, code);
    if (earliest && earliest < from) {
      from = earliest;
      chart = await fetchYahooEquityChart(symbol, from, today);
    }
    if (!dryRun) {
      // 先に分割を登録する（統合ビューの公式行の調整が新しい基準になり、異常値ガードの比較が揃う）
      const { error } = await core.from('alt_split_events').upsert(
        newSplits.map((s) => ({ local_code: code, ex_date: s.exDate, ratio: s.ratio, source: 'yahoo' })),
        { onConflict: 'local_code,ex_date', ignoreDuplicates: true }
      );
      if (error) throw new Error(`alt_split_events への書き込みに失敗しました: ${error.message}`);
    }
  }

  if (chart.bars.length === 0) {
    return { code, rows: 0, rejected: [], newSplits, error: 'Yahoo から日足が返らない' };
  }

  const prev = await prevAdjClose(core, code, chart.bars[0].date);
  const { accepted, rejected } = filterAnomalies(chart.bars, prev);
  const records = toAltBarRecords(code, accepted, chart.splits).map((r) => ({
    ...r,
    fetched_at: new Date().toISOString(),
  }));

  if (!dryRun && records.length > 0) {
    const { error } = await core
      .from('alt_equity_bar_daily')
      .upsert(records, { onConflict: 'local_code,trade_date' });
    if (error) throw new Error(`alt_equity_bar_daily への書き込みに失敗しました: ${error.message}`);
  }

  return { code, rows: records.length, rejected, newSplits };
}

/** TOPIX 推計（OFF: 書く / ON: 公式値と突き合わせてログに出すだけ） */
async function syncTopixProxy(core: Core, enabled: boolean, dryRun: boolean): Promise<Record<string, unknown>> {
  const { data: anchorRows, error: anchorError } = await core
    .from('topix_bar_daily')
    .select('trade_date, close')
    .eq('source', 'jquants')
    .order('trade_date', { ascending: false })
    .limit(enabled ? COMPARE_DAYS + 1 : 1);
  if (anchorError) throw new Error(`topix_bar_daily の取得に失敗しました: ${anchorError.message}`);
  const official = ((anchorRows ?? []) as Array<{ trade_date: string; close: string | number }>).map((r) => ({
    date: r.trade_date,
    close: Number(r.close),
  }));
  if (official.length === 0) throw new Error('公式 TOPIX が1行もありません（推計の基準が作れない）');

  // ON: 20営業日前を基準に推計し、その後の公式値との差を見る。OFF: 最後の公式日を基準にする
  const anchor = official[official.length - 1];
  const { data: etfRows, error: etfError } = await core
    .from('v_equity_price_daily')
    .select('trade_date, adj_open, adj_high, adj_low, adj_close')
    .eq('local_code', TOPIX_PROXY_ETF_CODE)
    .gte('trade_date', anchor.date)
    .order('trade_date', { ascending: true });
  if (etfError) throw new Error(`1306 の価格取得に失敗しました: ${etfError.message}`);
  const etf = ((etfRows ?? []) as Array<Record<string, string | number | null>>).map((r) => ({
    date: String(r.trade_date),
    adjOpen: r.adj_open == null ? null : Number(r.adj_open),
    adjHigh: r.adj_high == null ? null : Number(r.adj_high),
    adjLow: r.adj_low == null ? null : Number(r.adj_low),
    adjClose: Number(r.adj_close),
  }));
  const proxies = computeTopixProxy(anchor, etf);

  if (enabled) {
    const byDate = new Map(official.map((o) => [o.date, o.close]));
    const diffs = proxies
      .filter((p) => byDate.has(p.trade_date) && p.close != null)
      .map((p) => (p.close! / byDate.get(p.trade_date)! - 1) * 100);
    const maxAbsPct = diffs.length ? Math.max(...diffs.map(Math.abs)) : null;
    return { mode: 'compare', anchor: anchor.date, compared: diffs.length, maxAbsDiffPct: maxAbsPct };
  }

  if (!dryRun && proxies.length > 0) {
    const { error } = await core.from('topix_bar_daily').upsert(proxies, { onConflict: 'trade_date' });
    if (error) throw new Error(`TOPIX 推計の書き込みに失敗しました: ${error.message}`);
  }
  return { mode: 'proxy', anchor: anchor.date, written: proxies.length, last: proxies.at(-1)?.trade_date ?? null };
}

async function main(): Promise<void> {
  const dryRun = process.argv.includes('--dry-run');
  const today = getJSTDate();
  const mode = await getJQuantsMode({ fresh: true });
  const core = createAdminClient('jquants_core');
  const portfolio = createAdminClient('portfolio');

  const tracked = await getTrackedLocalCodes(portfolio);
  const codes = Array.from(new Set([...tracked, ...FIXED_ALT_CODES])).sort();
  const knownSplits = await loadKnownSplits(core, codes);

  const results: CodeResult[] = [];
  for (const code of codes) {
    try {
      results.push(
        await syncCode(core, code, today, knownSplits.get(code) ?? [], dryRun)
      );
    } catch (error) {
      results.push({
        code,
        rows: 0,
        rejected: [],
        newSplits: [],
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  const failed = results.filter((r) => r.error);
  const rejected = results.flatMap((r) => r.rejected.map((x) => ({ code: r.code, ...x })));
  const splits = results.flatMap((r) => r.newSplits.map((s) => ({ code: r.code, ...s })));

  let topix: Record<string, unknown> = { skipped: 'ETF 取得失敗' };
  if (!failed.some((r) => r.code === TOPIX_PROXY_ETF_CODE)) {
    try {
      topix = await syncTopixProxy(core, mode.enabled, dryRun);
    } catch (error) {
      topix = { error: error instanceof Error ? error.message : String(error) };
    }
  }

  logger.info('代替価格の同期完了', {
    jquants: mode.enabled ? 'ON（シャドー）' : 'OFF',
    dryRun,
    codes: codes.length,
    rows: results.reduce((a, r) => a + r.rows, 0),
    failed: failed.map((r) => `${r.code}: ${r.error}`),
    rejected,
    splits,
    topix,
  });

  const notices: string[] = [];
  for (const s of splits) {
    notices.push(
      `分割・併合を検知: ${s.code.slice(0, 4)} ${s.exDate} 1株→${s.ratio}株。保有している場合は Portfolio の「株式分割」で数量を登録してください。`
    );
  }
  for (const r of rejected) {
    notices.push(
      `異常値として採用せず: ${r.code.slice(0, 4)} ${r.date} 終値 ${r.adjClose}（前回 ${r.prevAdjClose}）。続く場合は Yahoo の値を目視確認してください。`
    );
  }
  if (notices.length > 0 && !dryRun) {
    for (const n of notices) console.log(`::warning::${n}`);
    await sendOpsNoticeEmail('代替株価（Yahoo）の確認事項', notices);
  }

  if (failed.length > 0 || 'error' in topix) {
    throw new Error(
      `代替価格の同期に失敗した項目があります: ${[
        ...failed.map((r) => `${r.code}(${r.error})`),
        ...('error' in topix ? [`TOPIX推計(${String(topix.error)})`] : []),
      ].join(', ')}`
    );
  }
}

main().catch((error) => {
  logger.error('代替価格の同期に失敗しました', {
    error: error instanceof Error ? error.message : String(error),
  });
  process.exit(1);
});
