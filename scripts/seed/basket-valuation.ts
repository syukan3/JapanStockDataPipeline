#!/usr/bin/env tsx
/**
 * テーマバスケット割安判定の初回バックフィル+構成銘柄投入スクリプト
 *
 * @description 日経半導体株指数 (200A) の構成30銘柄+ETF(200A0)の日足を J-Quants API から
 * 直接取得（DBには書かない）し、jquants_core.financial_disclosure の PIT（point-in-time）
 * 参照と組み合わせて、アンカー日ウエート係数（basket_constituents）と日次集計
 * （basket_metrics 2019-01-04〜）を analytics へ投入する。
 *
 * 書き込み先は analytics.basket_definitions / basket_constituents / basket_metrics の
 * 3テーブルのみ（冪等 upsert。再実行可）。
 *
 * 計画書: docs/PLANS-basket-valuation-2026-07.md（ルートリポ）、DDL: 00105
 *
 * @example
 * ```
 * npm run seed:basket -- --dry-run              # DB書き込みなしで全計算+検証レポート
 * npm run seed:basket -- --dry-run --limit 30   # 直近30営業日分のみ投入対象にして反復
 * npm run seed:basket                            # 本実行（価格はローカルキャッシュ利用）
 * npm run seed:basket -- --refresh-cache         # 価格キャッシュを破棄して再取得
 * ```
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { loadEnv, createProgress, logResult, startTimer, type SeedResult } from './_shared';
// 純関数のみのモジュール（環境変数を読まないため静的importで安全）
import {
  extractSplitEvents,
  cumulativeAdjustmentFactor,
  pitFy,
  waterFillCap,
  buildConstituentDay,
  aggregateBasketDay,
  chainIndexSeries,
  pearsonCorrelation,
  annualizedTrackingError,
  type SlimBar,
  type SplitEvent,
  type ConstituentDay,
  type BasketDayAggregate,
} from '../../src/lib/analytics/basket-valuation';
import {
  fetchDisclosuresGrouped,
  buildPitByCode,
} from '../../src/lib/analytics/basket-valuation-data';
import { getJSTDate, addDays } from '../../src/lib/utils/date';

// ============================================================
// バスケット定義（2025年11月末入替後の現行30銘柄）
// ============================================================

const BASKET_ID = 'nkscd-200a';
const DISPLAY_NAME = '日経半導体株指数 (200A)';
const BENCHMARK_CODE = '200A0';
/** 直近定期見直し（2025年11月末）の最終営業日 */
const ANCHOR_DATE = '2025-11-28';
/** basket_metrics の投入開始日（financial_disclosure が 2019-01〜のため） */
const METRICS_FROM = '2019-01-04';
/**
 * 価格取得開始日の希望値。実際は J-Quants Standard のローリング10年ウィンドウ
 * （実測: 今日-10年より前の from は HTTP 400）にクランプされる。
 * バックフィルに必要なのは 2019-01 以降（分割イベントも開示 2019-01〜より後のみ影響）
 * のため、クランプされても計算には影響しない。
 */
const PRICE_FROM_TARGET = '2015-07-01';

/** ローリング10年ウィンドウの安全な開始日（境界ちょうどを避けて+2日マージン） */
function clampToApiWindow(today: string, target: string): string {
  const [y, m, d] = today.split('-').map(Number);
  const boundary = new Date(Date.UTC(y - 10, m - 1, d));
  boundary.setUTCDate(boundary.getUTCDate() + 2);
  const windowStart = boundary.toISOString().slice(0, 10);
  return target > windowStart ? target : windowStart;
}
/** このカバレッジ%未満の日は basket_metrics を書かない */
const COVERAGE_MIN_PCT = 50;
const CAP_DEFAULT = 0.15;
/** 半導体を主業としない銘柄の定期見直し時キャップ（仮決め: 信越化学/ソニーG/HOYA） */
const CAP_NON_SEMICON = 0.05;

interface ConstituentDef {
  code: string;
  name: string;
  isSemiconMain: boolean;
}

const CONSTITUENTS: ConstituentDef[] = [
  { code: '285A0', name: 'キオクシアHD', isSemiconMain: true },
  { code: '31320', name: 'マクニカHD', isSemiconMain: true },
  { code: '34360', name: 'SUMCO', isSemiconMain: true },
  { code: '40210', name: '日産化学', isSemiconMain: true },
  { code: '40430', name: 'トクヤマ', isSemiconMain: true },
  { code: '40630', name: '信越化学工業', isSemiconMain: false },
  { code: '41860', name: '東京応化工業', isSemiconMain: true },
  { code: '42030', name: '住友ベークライト', isSemiconMain: true },
  { code: '42720', name: '日本化薬', isSemiconMain: true },
  { code: '44010', name: 'ADEKA', isSemiconMain: true },
  { code: '46260', name: '太陽HD', isSemiconMain: true },
  { code: '49800', name: 'デクセリアルズ', isSemiconMain: true },
  { code: '50160', name: 'JX金属', isSemiconMain: true },
  { code: '61460', name: 'ディスコ', isSemiconMain: true },
  { code: '63150', name: 'TOWA', isSemiconMain: true },
  { code: '63230', name: 'ローツェ', isSemiconMain: true },
  { code: '65250', name: 'KOKUSAI ELECTRIC', isSemiconMain: true },
  { code: '65260', name: 'ソシオネクスト', isSemiconMain: true },
  { code: '67230', name: 'ルネサスエレクトロニクス', isSemiconMain: true },
  { code: '67280', name: 'アルバック', isSemiconMain: true },
  { code: '67580', name: 'ソニーグループ', isSemiconMain: false },
  { code: '68570', name: 'アドバンテスト', isSemiconMain: true },
  { code: '68900', name: 'フェローテック', isSemiconMain: true },
  { code: '69200', name: 'レーザーテック', isSemiconMain: true },
  { code: '69630', name: 'ローム', isSemiconMain: true },
  { code: '77290', name: '東京精密', isSemiconMain: true },
  { code: '77350', name: 'SCREEN HD', isSemiconMain: true },
  { code: '77410', name: 'HOYA', isSemiconMain: false },
  { code: '80350', name: '東京エレクトロン', isSemiconMain: true },
  { code: '81540', name: '加賀電子', isSemiconMain: true },
];

// ============================================================
// CLI
// ============================================================

interface CliOptions {
  dryRun: boolean;
  /** 直近N営業日分だけを投入対象にする（開発反復用。計算自体は全期間） */
  limit?: number;
  cacheDir: string;
  refreshCache: boolean;
  /** 集計終了日（デフォルト: JST昨日） */
  to?: string;
}

function parseCliArgs(): CliOptions {
  const args = process.argv.slice(2);
  const options: CliOptions = {
    dryRun: false,
    cacheDir: join(tmpdir(), 'basket-valuation-cache'),
    refreshCache: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const nextArg = args[i + 1];
    if (arg === '--dry-run') {
      options.dryRun = true;
    } else if (arg === '--refresh-cache') {
      options.refreshCache = true;
    } else if (arg === '--limit' && nextArg) {
      const limit = parseInt(nextArg, 10);
      if (isNaN(limit) || limit <= 0) {
        console.error(`Invalid --limit value: ${nextArg}. Expected positive integer`);
        process.exit(1);
      }
      options.limit = limit;
      i++;
    } else if (arg === '--cache-dir' && nextArg) {
      options.cacheDir = nextArg;
      i++;
    } else if (arg === '--to' && nextArg) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(nextArg)) {
        console.error(`Invalid --to date format: ${nextArg}. Expected YYYY-MM-DD`);
        process.exit(1);
      }
      options.to = nextArg;
      i++;
    }
  }

  return options;
}

// ============================================================
// 価格取得（ローカルキャッシュ付き・DBには書かない）
// ============================================================

interface BarCacheFile {
  code: string;
  from: string;
  to: string;
  fetchedAt: string;
  bars: SlimBar[];
}

async function loadPricesWithCache(
  fetchBars: (code: string, from: string, to: string) => Promise<SlimBar[]>,
  code: string,
  from: string,
  to: string,
  cacheDir: string,
  refresh: boolean
): Promise<SlimBar[]> {
  const cacheFile = join(cacheDir, `bars-${code}.json`);
  if (!refresh && existsSync(cacheFile)) {
    try {
      const cached = JSON.parse(readFileSync(cacheFile, 'utf-8')) as BarCacheFile;
      if (cached.from <= from && cached.to >= to) {
        return cached.bars.filter((b) => b.date >= from && b.date <= to);
      }
    } catch {
      // 壊れたキャッシュは再取得
    }
  }

  const bars = await fetchBars(code, from, to);
  const payload: BarCacheFile = {
    code,
    from,
    to,
    fetchedAt: new Date().toISOString(),
    bars,
  };
  writeFileSync(cacheFile, JSON.stringify(payload));
  return bars;
}

// ============================================================
// main
// ============================================================

async function main(): Promise<SeedResult> {
  loadEnv();
  const options = parseCliArgs();
  const timer = startTimer();

  // 動的インポート（環境変数ロード後）
  const { createJQuantsClient } = await import('../../src/lib/jquants/client');
  const { createAdminClient } = await import('../../src/lib/supabase/admin');
  const { batchUpsert } = await import('../../src/lib/utils/batch');

  const to = options.to ?? addDays(getJSTDate(), -1);
  if (to < ANCHOR_DATE) {
    throw new Error(`--to (${to}) must be on/after anchor date ${ANCHOR_DATE}`);
  }
  const priceFrom = clampToApiWindow(getJSTDate(), PRICE_FROM_TARGET);

  console.log('Starting Basket Valuation Seed');
  console.log(`  Basket:    ${BASKET_ID} (${CONSTITUENTS.length} constituents + ${BENCHMARK_CODE})`);
  console.log(`  Anchor:    ${ANCHOR_DATE}`);
  console.log(`  Metrics:   ${METRICS_FROM} .. ${to}`);
  console.log(`  Cache:     ${options.cacheDir}${options.refreshCache ? ' (refresh)' : ''}`);
  if (options.limit) console.log(`  Limit:     last ${options.limit} days`);
  if (options.dryRun) console.log('  Dry run:   no DB writes');

  mkdirSync(options.cacheDir, { recursive: true });

  // ---------- Step 1: 価格取得（30銘柄 + 200A0、API直接・キャッシュ付き） ----------
  const jq = createJQuantsClient();
  const fetchBars = async (code: string, from: string, toDate: string): Promise<SlimBar[]> => {
    const items = await jq.getEquityBarsDaily({ code, from, to: toDate });
    return items.map((item) => ({
      date: item.Date,
      close: item.C ?? null,
      adjClose: item.AdjC ?? null,
      adjFactor: item.AdjFactor ?? null,
    }));
  };

  const allCodes = [...CONSTITUENTS.map((c) => c.code), BENCHMARK_CODE];
  const barsByCode = new Map<string, SlimBar[]>();
  const priceProgress = createProgress(allCodes.length, 'basket_prices');
  let fetched = 0;
  for (const code of allCodes) {
    const bars = await loadPricesWithCache(
      fetchBars, code, priceFrom, to, options.cacheDir, options.refreshCache
    );
    barsByCode.set(code, bars);
    fetched += bars.length;
    priceProgress.increment(code);
  }
  priceProgress.done();

  const closeByCode = new Map<string, Map<string, number>>();
  const adjCloseByCode = new Map<string, Map<string, number>>();
  const splitEventsByCode = new Map<string, SplitEvent[]>();
  for (const [code, bars] of barsByCode) {
    const closes = new Map<string, number>();
    const adjCloses = new Map<string, number>();
    for (const bar of bars) {
      if (bar.close != null) closes.set(bar.date, bar.close);
      if (bar.adjClose != null) adjCloses.set(bar.date, bar.adjClose);
    }
    closeByCode.set(code, closes);
    adjCloseByCode.set(code, adjCloses);
    splitEventsByCode.set(code, extractSplitEvents(bars));
  }

  // ---------- Step 2: PIT 財務系列（financial_disclosure を読み取りのみ） ----------
  const core = createAdminClient('jquants_core');
  const constituentCodes = CONSTITUENTS.map((c) => c.code);
  const disclosuresByCode = await fetchDisclosuresGrouped(core, constituentCodes);
  const pitByCode = buildPitByCode(disclosuresByCode);
  console.log(
    `Disclosures loaded: ${[...disclosuresByCode.values()].reduce((s, r) => s + r.length, 0)} rows`
  );

  // ---------- Step 3: アンカー日の weight_factor（機械キャップ・水充填） ----------
  const anchorMcaps: { code: string; mcap: number }[] = [];
  const missingAtAnchor: string[] = [];
  for (const def of CONSTITUENTS) {
    const close = closeByCode.get(def.code)?.get(ANCHOR_DATE);
    const fy = pitFy(pitByCode.get(def.code)!.fy, ANCHOR_DATE);
    if (close == null || !fy?.sharesOutstanding) {
      missingAtAnchor.push(`${def.code} (close=${close}, fy=${fy?.disclosedDate})`);
      continue;
    }
    const cum = cumulativeAdjustmentFactor(
      splitEventsByCode.get(def.code) ?? [], fy.disclosedDate, ANCHOR_DATE
    );
    anchorMcaps.push({ code: def.code, mcap: close * (fy.sharesOutstanding / cum) });
  }
  if (missingAtAnchor.length > 0) {
    throw new Error(`Anchor data missing for: ${missingAtAnchor.join(', ')}`);
  }

  const totalAnchorMcap = anchorMcaps.reduce((s, m) => s + m.mcap, 0);
  const capInputs = anchorMcaps.map((m) => ({
    code: m.code,
    rawShare: m.mcap / totalAnchorMcap,
    capLimit: CONSTITUENTS.find((c) => c.code === m.code)!.isSemiconMain
      ? CAP_DEFAULT
      : CAP_NON_SEMICON,
  }));
  const cappedWeights = waterFillCap(capInputs);

  const weightFactorByCode = new Map<string, number>();
  const officialWeightByCode = new Map<string, number>();
  for (const input of capInputs) {
    const capped = cappedWeights.get(input.code)!;
    weightFactorByCode.set(input.code, capped / input.rawShare);
    officialWeightByCode.set(input.code, capped * 100);
  }

  const anchorIndexLevel = adjCloseByCode.get(BENCHMARK_CODE)?.get(ANCHOR_DATE);
  if (anchorIndexLevel == null) {
    throw new Error(`No ${BENCHMARK_CODE} adj_close on anchor date ${ANCHOR_DATE}`);
  }

  console.log(`Anchor index level (${BENCHMARK_CODE} adj_close @ ${ANCHOR_DATE}): ${anchorIndexLevel}`);
  console.log('Anchor weights (capped):');
  for (const def of CONSTITUENTS) {
    const weight = officialWeightByCode.get(def.code)!;
    const factor = weightFactorByCode.get(def.code)!;
    console.log(
      `  ${def.code} ${def.name}: weight=${weight.toFixed(2)}% factor=${factor.toFixed(4)}` +
        `${def.isSemiconMain ? '' : ' [5% cap]'}`
    );
  }

  // ---------- Step 4: 日次 basket_metrics（メモリ上で全計算） ----------
  const dateSet = new Set<string>();
  for (const code of constituentCodes) {
    for (const [date] of closeByCode.get(code) ?? []) {
      if (date >= METRICS_FROM && date <= to) dateSet.add(date);
    }
  }
  const dates = [...dateSet].sort();

  const aggByDate = new Map<string, BasketDayAggregate>();
  const weightsByDate = new Map<string, Map<string, number>>();
  const perStockPer = new Map<string, number>(); // 検証用: 最終日の個別PER（t基準）
  const lastDate = dates[dates.length - 1];

  for (const date of dates) {
    const items: ConstituentDay[] = [];
    for (const code of constituentCodes) {
      const item = buildConstituentDay(
        {
          code,
          factor: weightFactorByCode.get(code)!,
          officialWeight: officialWeightByCode.get(code)!,
          close: closeByCode.get(code)?.get(date),
          pit: pitByCode.get(code)!,
          events: splitEventsByCode.get(code) ?? [],
        },
        date
      );
      if (item) items.push(item);
    }
    const agg = aggregateBasketDay(items);
    aggByDate.set(date, agg);
    weightsByDate.set(date, agg.weights);
  }

  // 検証用: 最終日の個別PER（close / (eps × 分割累積係数)）
  for (const code of constituentCodes) {
    const close = closeByCode.get(code)?.get(lastDate);
    const fy = pitFy(pitByCode.get(code)!.fy, lastDate);
    if (close == null || fy?.eps == null || fy.eps === 0) continue;
    const cum = cumulativeAdjustmentFactor(
      splitEventsByCode.get(code) ?? [], fy.disclosedDate, lastDate
    );
    perStockPer.set(code, close / (fy.eps * cum));
  }

  const constituentAdjCloses = new Map<string, Map<string, number>>();
  for (const code of constituentCodes) {
    constituentAdjCloses.set(code, adjCloseByCode.get(code) ?? new Map());
  }
  const indexLevels = chainIndexSeries(
    dates, weightsByDate, constituentAdjCloses, ANCHOR_DATE, anchorIndexLevel
  );

  const round = (v: number | null | undefined, dp: number): number | null =>
    v == null ? null : Number(v.toFixed(dp));

  interface MetricRow {
    basket_id: string;
    as_of_date: string;
    index_level: number | null;
    etf_close: number | null;
    weighted_per: number | null;
    weighted_per_forward: number | null;
    weighted_pbr: number | null;
    weighted_psr: number | null;
    weighted_div_yield: number | null;
    weighted_eps_level: number | null;
    coverage_pct: number | null;
    updated_at: string;
  }

  const nowIso = new Date().toISOString();
  const allMetricRows: MetricRow[] = [];
  let skippedLowCoverage = 0;
  for (const date of dates) {
    const agg = aggByDate.get(date)!;
    if (agg.coveragePct < COVERAGE_MIN_PCT) {
      skippedLowCoverage++;
      continue;
    }
    const level = indexLevels.get(date) ?? null;
    const epsLevel =
      level != null && agg.weightedPer != null && agg.weightedPer > 0
        ? level / agg.weightedPer
        : null;
    allMetricRows.push({
      basket_id: BASKET_ID,
      as_of_date: date,
      index_level: round(level, 4),
      etf_close: round(adjCloseByCode.get(BENCHMARK_CODE)?.get(date) ?? null, 4),
      weighted_per: round(agg.weightedPer, 2),
      weighted_per_forward: round(agg.weightedPerForward, 2),
      weighted_pbr: round(agg.weightedPbr, 2),
      weighted_psr: round(agg.weightedPsr, 2),
      weighted_div_yield: round(agg.weightedDivYield, 3),
      weighted_eps_level: round(epsLevel, 4),
      coverage_pct: round(agg.coveragePct, 1),
      updated_at: nowIso,
    });
  }
  const metricRows = options.limit ? allMetricRows.slice(-options.limit) : allMetricRows;
  console.log(
    `Metric days: ${allMetricRows.length} (skipped ${skippedLowCoverage} low-coverage days` +
      `${options.limit ? `, writing last ${metricRows.length}` : ''})`
  );

  // ---------- DB 書き込み（basket_* 3テーブルのみ） ----------
  let inserted = 0;
  const errors: Error[] = [];
  const analytics = createAdminClient('analytics');
  if (!options.dryRun) {
    const { error: defError } = await analytics.from('basket_definitions').upsert(
      {
        basket_id: BASKET_ID,
        display_name: DISPLAY_NAME,
        benchmark_code: BENCHMARK_CODE,
        description:
          '日経半導体株指数の現行30銘柄を時価総額加重+機械キャップ(通常15%/非半導体主業5%)で模擬。' +
          '公式ウエートではなくアンカー日の機械キャップ適用値を使用。',
        anchor_date: ANCHOR_DATE,
        anchor_index_level: round(anchorIndexLevel, 4),
        updated_at: nowIso,
      },
      { onConflict: 'basket_id' }
    );
    if (defError) throw new Error(`basket_definitions upsert failed: ${defError.message}`);
    inserted += 1;

    const constituentRows = CONSTITUENTS.map((def) => ({
      basket_id: BASKET_ID,
      local_code: def.code,
      weight_factor: Number(weightFactorByCode.get(def.code)!.toFixed(8)),
      official_weight: Number(officialWeightByCode.get(def.code)!.toFixed(3)),
      is_semicon_main: def.isSemiconMain,
      valid_from: ANCHOR_DATE,
      valid_to: null,
    }));
    const { error: conError } = await analytics
      .from('basket_constituents')
      .upsert(constituentRows, { onConflict: 'basket_id,local_code,valid_from' });
    if (conError) throw new Error(`basket_constituents upsert failed: ${conError.message}`);
    inserted += constituentRows.length;

    const metricProgress = createProgress(metricRows.length, 'basket_metrics');
    const result = await batchUpsert(analytics, 'basket_metrics', metricRows, 'basket_id,as_of_date', {
      batchSize: 500,
      onBatchComplete: (_batch, done) => metricProgress.set(Math.min(done, metricRows.length)),
    });
    metricProgress.done();
    inserted += result.inserted;
    errors.push(...result.errors);
  }

  // ---------- Step 5: 検証レポート ----------
  const { data: smRows, error: smError } = await analytics
    .from('stock_metrics')
    .select('local_code, per, market_cap')
    .eq('as_of_date', lastDate)
    .in('local_code', constituentCodes);
  if (smError) {
    console.log(`stock_metrics fetch for verification failed: ${smError.message}`);
  }

  printVerificationReport({
    dates,
    aggByDate,
    indexLevels,
    etfAdjCloses: adjCloseByCode.get(BENCHMARK_CODE) ?? new Map(),
    allMetricRows,
    perStockPer,
    lastDate,
    weightFactorByCode,
    smRows: (smRows ?? []) as StockMetricsRow[],
  });

  const seedResult: SeedResult = {
    name: 'Basket Valuation',
    fetched,
    inserted,
    errors,
    durationMs: timer(),
  };
  logResult(seedResult);
  return seedResult;
}

// ============================================================
// 検証レポート（仕様書 Step 5）
// ============================================================

interface StockMetricsRow {
  local_code: string;
  per: number | string | null;
  market_cap: number | string | null;
}

interface VerificationInput {
  dates: string[];
  aggByDate: Map<string, BasketDayAggregate>;
  indexLevels: Map<string, number>;
  etfAdjCloses: Map<string, number>;
  allMetricRows: { as_of_date: string; weighted_per: number | null; coverage_pct: number | null }[];
  perStockPer: Map<string, number>;
  lastDate: string;
  weightFactorByCode: Map<string, number>;
  smRows: StockMetricsRow[];
}

function printVerificationReport(input: VerificationInput): void {
  const {
    dates, aggByDate, indexLevels, etfAdjCloses, allMetricRows,
    perStockPer, lastDate, weightFactorByCode, smRows,
  } = input;
  const toNum = (v: number | string | null): number | null => (v == null ? null : Number(v));

  console.log('\n========================================');
  console.log('VERIFICATION REPORT');
  console.log('========================================');

  // 1) 模擬指数 vs 200A0: 日次リターン相関・年率TE
  //    アンカー日以降は構成が公式と一致するため、全期間との差が「過去の構成固定近似」の影響を示す
  const trackingStats = (from: string): string => {
    const pairDates = dates.filter(
      (d) => d >= from && indexLevels.has(d) && etfAdjCloses.has(d)
    );
    const simReturns: number[] = [];
    const etfReturns: number[] = [];
    for (let i = 1; i < pairDates.length; i++) {
      const prev = pairDates[i - 1];
      const cur = pairDates[i];
      simReturns.push(indexLevels.get(cur)! / indexLevels.get(prev)! - 1);
      etfReturns.push(etfAdjCloses.get(cur)! / etfAdjCloses.get(prev)! - 1);
    }
    const corr = pearsonCorrelation(simReturns, etfReturns);
    const te = annualizedTrackingError(simReturns, etfReturns);
    return (
      `${pairDates[0] ?? 'N/A'}..${pairDates[pairDates.length - 1] ?? 'N/A'} (n=${simReturns.length}): ` +
      `corr=${corr?.toFixed(4) ?? 'N/A'} TE=${te?.toFixed(2) ?? 'N/A'}%`
    );
  };
  console.log(`\n[1] Sim index vs ${BENCHMARK_CODE} daily returns (corr target >= 0.95):`);
  console.log(`    since 2024-06: ${trackingStats('2024-06-01')}`);
  console.log(`    since anchor:  ${trackingStats(ANCHOR_DATE)}`);

  // 2) 直近 weighted_per vs stock_metrics 個別値からの手計算
  const lastAgg = aggByDate.get(lastDate);
  console.log(`\n[2] weighted_per cross-check @ ${lastDate}:`);
  if (smRows.length === 0) {
    console.log('    stock_metrics rows unavailable');
  } else {
    let num = 0;
    let den = 0;
    let used = 0;
    for (const row of smRows) {
      const per = toNum(row.per);
      const mcap = toNum(row.market_cap);
      const factor = weightFactorByCode.get(row.local_code);
      if (per == null || per <= 0 || mcap == null || factor == null) continue;
      num += factor * mcap;
      den += (factor * mcap) / per;
      used++;
    }
    const handPer = den > 0 ? num / den : null;
    console.log(`    computed weighted_per   = ${lastAgg?.weightedPer?.toFixed(2) ?? 'N/A'}`);
    console.log(
      `    stock_metrics hand-calc = ${handPer?.toFixed(2) ?? 'N/A'} ` +
        `(from ${used}/${smRows.length} names with per>0)`
    );
    console.log('    note: stock_metrics は赤字銘柄の per を NULL にするため手計算は対象銘柄が減る');
  }

  // 3) 東京エレクトロン(80350) の個別PER突合（stock_metrics 51.89 @2026-07-17 目安）
  const telPer = perStockPer.get('80350');
  const telSmPer = toNum(smRows.find((r) => r.local_code === '80350')?.per ?? null);
  const diffPct =
    telPer != null && telSmPer != null ? ((telPer - telSmPer) / telSmPer) * 100 : null;
  console.log(`\n[3] TEL(80350) PER @ ${lastDate}:`);
  console.log(
    `    computed = ${telPer?.toFixed(2) ?? 'N/A'} / stock_metrics = ${telSmPer?.toFixed(2) ?? 'N/A'}` +
      ` / diff = ${diffPct?.toFixed(2) ?? 'N/A'}% (target within ±10%)`
  );

  // 4) カバレッジ推移（年別平均）
  console.log('\n[4] Coverage by year (avg %):');
  const byYear = new Map<string, { sum: number; n: number }>();
  for (const row of allMetricRows) {
    const year = row.as_of_date.slice(0, 4);
    const entry = byYear.get(year) ?? { sum: 0, n: 0 };
    entry.sum += row.coverage_pct ?? 0;
    entry.n++;
    byYear.set(year, entry);
  }
  for (const [year, { sum, n }] of [...byYear].sort()) {
    console.log(`    ${year}: ${(sum / n).toFixed(1)}% (${n} days)`);
  }

  // 5) 高値圏(2024-07) vs 急落後(2025-04) の weighted_per サニティ
  const avgPer = (prefix: string): { avg: number; n: number } | null => {
    const values = allMetricRows
      .filter((r) => r.as_of_date.startsWith(prefix) && r.weighted_per != null)
      .map((r) => r.weighted_per!);
    if (values.length === 0) return null;
    return { avg: values.reduce((s, v) => s + v, 0) / values.length, n: values.length };
  };
  const peak = avgPer('2024-07');
  const trough = avgPer('2025-04');
  const latest = allMetricRows[allMetricRows.length - 1];
  console.log('\n[5] weighted_per sanity:');
  console.log(`    2024-07 (高値圏) avg = ${peak?.avg.toFixed(2) ?? 'N/A'} (${peak?.n ?? 0} days)`);
  console.log(`    2025-04 (急落後) avg = ${trough?.avg.toFixed(2) ?? 'N/A'} (${trough?.n ?? 0} days)`);
  console.log(`    latest (${latest?.as_of_date ?? 'N/A'}) = ${latest?.weighted_per ?? 'N/A'}`);
  console.log('========================================');
}

// 直接実行時のみmain()を呼ぶ（import時は実行しない）
const isDirectRun =
  process.argv[1]?.endsWith('basket-valuation.ts') || process.argv[1]?.endsWith('basket-valuation');
if (isDirectRun) {
  main()
    .then((result) => {
      if (result.errors.length > 0) {
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

export { main as seedBasketValuation, CONSTITUENTS, BASKET_ID, ANCHOR_DATE };
