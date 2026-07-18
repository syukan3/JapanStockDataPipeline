/**
 * テーマバスケット日次集計 更新スクリプト（GH Actions用）
 *
 * @description
 * analytics.basket_definitions の全バスケットについて、現行構成銘柄（basket_constituents,
 * valid_to is null）の当日 stock_metrics から当日ウエート・加重バリュエーション・
 * 模擬指数を計算し、analytics.basket_metrics へ upsert する（冪等）。
 * 計画書: docs/PLANS-basket-valuation-2026-07.md §5/§6/§8
 *
 * - 計算パイプラインは src/lib/analytics/basket-metrics.ts の純関数群（テスト対象）。
 * - バスケット定義が0件、または対象バスケットの現行構成銘柄が0件の場合はそのバスケットを
 *   スキップして正常終了する（Issue B の初回投入前でも失敗させない。他バスケットは継続）。
 * - job_runs / job_locks は使わない（upsert は冪等。refresh-technical.ts と同方針）。
 * - Cron A の stock_metrics 更新（refresh_stock_metrics RPC）後段の
 *   continue-on-error ステップとして実行する想定。equity_bars/equity_master/rebase
 *   成功をゲート条件にする（cron-a.yml 側。詳細は同ファイルのコメント参照）。
 *
 * 実行:
 *   npx tsx scripts/cron/refresh-basket-metrics.ts [--dry-run] [--date=YYYY-MM-DD]
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import {
  toNumberOrNull,
  parseBasketConstituentRow,
  parseStockMetricRow,
  computeWeights,
  weightedHarmonicMean,
  weightedAverage,
  selectEffectiveForecastEps,
  computeIndexLevel,
  computeWeightedEpsLevel,
  type BasketConstituent,
  type StockMetricSnapshot,
  type WeightedMetricEntry,
  type IndexReturnEntry,
} from '../../src/lib/analytics/basket-metrics';

// Supabaseクライアントはスキーマ束縛の動的型のため any で受ける（repo慣習。indicators-sync.ts 等と同方針）
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = any;

const logger = createLogger({ module: 'refresh-basket-metrics' });

interface BasketDefinition {
  basket_id: string;
  benchmark_code: string | null;
  anchor_date: string | null;
  anchor_index_level: number | null;
}

interface ForecastEpsRow {
  forecast_eps: number | null;
  next_forecast_eps: number | null;
}

function validateEnv(): void {
  const required = ['NEXT_PUBLIC_SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

async function main(): Promise<void> {
  validateEnv();
  const dryRun = process.argv.includes('--dry-run');
  const dateArg = process.argv.find((a) => a.startsWith('--date='));
  const explicitDate = dateArg ? dateArg.split('=')[1] : undefined;
  if (dryRun) logger.info('DRY RUN: 読み取り＋計算のみ。書き込みは行わない');

  const core = createAdminClient('jquants_core');
  const analytics = createAdminClient('analytics');

  const baskets = await listBasketDefinitions(analytics);
  if (baskets.length === 0) {
    logger.info('No basket definitions found; nothing to do');
    console.log(JSON.stringify({ success: true, baskets: 0, results: [] }));
    return;
  }
  logger.info('Loaded basket definitions', { count: baskets.length });

  const results: Record<string, unknown>[] = [];
  const failures: string[] = [];

  for (const basket of baskets) {
    try {
      const result = await refreshBasket(core, analytics, basket, explicitDate, dryRun);
      results.push({ basketId: basket.basket_id, ...result });
    } catch (e) {
      logger.error('Failed to refresh basket', { basketId: basket.basket_id, error: message(e) });
      failures.push(`${basket.basket_id}: ${message(e)}`);
      results.push({ basketId: basket.basket_id, error: message(e) });
    }
  }

  console.log(
    JSON.stringify({ success: failures.length === 0, baskets: baskets.length, results }, null, 2)
  );
  if (failures.length > 0) {
    throw new Error(`Some baskets failed: ${failures.join(' / ')}`);
  }
}

/** 1バスケット分の当日集計を計算し upsert する */
async function refreshBasket(
  core: Client,
  analytics: Client,
  basket: BasketDefinition,
  explicitDate: string | undefined,
  dryRun: boolean
): Promise<Record<string, unknown>> {
  const constituents = await listCurrentConstituents(analytics, basket.basket_id);
  if (constituents.length === 0) {
    logger.warn('No current constituents; skipping basket', { basketId: basket.basket_id });
    return { skipped: true, reason: 'no_constituents' };
  }
  const codes = constituents.map((c) => c.local_code);

  const targetDate = explicitDate ?? (await getLatestStockMetricsDate(analytics));
  if (!targetDate) {
    logger.warn('No stock_metrics data available; skipping basket', { basketId: basket.basket_id });
    return { skipped: true, reason: 'no_stock_metrics' };
  }

  const metricsMap = await getStockMetricsForDate(analytics, targetDate, codes);
  const { weights, coveragePct } = computeWeights(constituents, metricsMap);

  // 加重バリュエーション（実績PER/PBR/PSR = 調和集計、配当利回り = 単純加重平均）
  const weightedPer = weightedHarmonicMean(
    buildMetricEntries(constituents, metricsMap, (m) => m.per)
  );
  const weightedPbr = weightedHarmonicMean(
    buildMetricEntries(constituents, metricsMap, (m) => m.pbr)
  );
  const weightedPsr = weightedHarmonicMean(
    buildMetricEntries(constituents, metricsMap, (m) => m.psr)
  );
  const weightedDivYield = weightedAverage(
    buildMetricEntries(constituents, metricsMap, (m) => m.dividend_yield)
  );

  // フォワードPER（会社予想EPSベース。FY開示のforecast_eps欠損はnext_forecast_epsへフォールバック）
  const disclosuresByCode = await getLatestForecastEps(core, codes, targetDate);
  const forwardPerEntries: WeightedMetricEntry[] = constituents.map((c) => {
    const metric = metricsMap.get(c.local_code);
    const forecastEps = selectEffectiveForecastEps(disclosuresByCode.get(c.local_code));
    const forwardPer =
      metric?.close != null && forecastEps != null && forecastEps > 0
        ? metric.close / forecastEps
        : null;
    return {
      localCode: c.local_code,
      weightFactor: c.weight_factor,
      marketCap: metric?.market_cap ?? null,
      value: forwardPer,
    };
  });
  const weightedPerForward = weightedHarmonicMean(forwardPerEntries);

  // 模擬指数の連結（前営業日の basket_metrics 行がある場合のみ）
  const indexLevel = await computeIndexLevelForDate(
    core,
    analytics,
    basket,
    constituents,
    targetDate
  );
  const weightedEpsLevel = computeWeightedEpsLevel(indexLevel, weightedPer);

  const etfClose = basket.benchmark_code
    ? await getEquityCloseForDate(core, basket.benchmark_code, targetDate)
    : null;

  const row = {
    basket_id: basket.basket_id,
    as_of_date: targetDate,
    index_level: indexLevel,
    etf_close: etfClose,
    weighted_per: weightedPer,
    weighted_per_forward: weightedPerForward,
    weighted_pbr: weightedPbr,
    weighted_psr: weightedPsr,
    weighted_div_yield: weightedDivYield,
    weighted_eps_level: weightedEpsLevel,
    coverage_pct: Math.round(coveragePct * 10) / 10,
    updated_at: new Date().toISOString(),
  };

  if (dryRun) {
    logger.info('Dry run: would upsert basket_metrics', { basketId: basket.basket_id, row });
    return {
      dryRun: true,
      targetDate,
      constituents: constituents.length,
      coveredWeights: weights.size,
      row,
    };
  }

  const { error } = await analytics
    .from('basket_metrics')
    .upsert(row, { onConflict: 'basket_id,as_of_date' });
  if (error) {
    throw new Error(`Failed to upsert basket_metrics for ${basket.basket_id}: ${error.message}`);
  }

  return {
    targetDate,
    constituents: constituents.length,
    coveredWeights: weights.size,
    coveragePct: row.coverage_pct,
    upserted: true,
  };
}

/** 構成銘柄 + 当日メトリクスから、指定フィールドの WeightedMetricEntry[] を組み立てる */
function buildMetricEntries(
  constituents: BasketConstituent[],
  metricsMap: Map<string, StockMetricSnapshot>,
  selector: (m: StockMetricSnapshot) => number | null
): WeightedMetricEntry[] {
  return constituents.map((c) => {
    const metric = metricsMap.get(c.local_code);
    return {
      localCode: c.local_code,
      weightFactor: c.weight_factor,
      marketCap: metric?.market_cap ?? null,
      value: metric ? selector(metric) : null,
    };
  });
}

/**
 * 模擬指数（index_level）を対象日について計算する。
 *
 * - 前営業日の basket_metrics 行が無ければ null（バックフィルが過去分を埋める想定）。
 *   ただし前行が一度も存在せず、対象日がアンカー日と一致する場合のみ
 *   anchor_index_level を初期値として採用する。
 * - 前行はあるが index_level が未確定（null）の場合もチェーンできないため null のまま。
 */
async function computeIndexLevelForDate(
  core: Client,
  analytics: Client,
  basket: BasketDefinition,
  constituents: BasketConstituent[],
  targetDate: string
): Promise<number | null> {
  const codes = constituents.map((c) => c.local_code);
  const prevRow = await getPreviousBasketMetricsRow(analytics, basket.basket_id, targetDate);

  if (!prevRow) {
    if (basket.anchor_date === targetDate && basket.anchor_index_level != null) {
      return basket.anchor_index_level;
    }
    return null;
  }
  if (prevRow.index_level == null) {
    return null;
  }

  const prevMetricsMap = await getStockMetricsForDate(analytics, prevRow.as_of_date, codes);
  const closesByCode = await getEquityBarClosesForDates(core, codes, [
    prevRow.as_of_date,
    targetDate,
  ]);

  const entries: IndexReturnEntry[] = constituents.map((c) => {
    const dates = closesByCode.get(c.local_code);
    return {
      localCode: c.local_code,
      weightFactor: c.weight_factor,
      prevMarketCap: prevMetricsMap.get(c.local_code)?.market_cap ?? null,
      prevClose: dates?.get(prevRow.as_of_date) ?? null,
      currClose: dates?.get(targetDate) ?? null,
    };
  });

  return computeIndexLevel(prevRow.index_level, entries).indexLevel;
}

// ============================================================
// DB読み書き
// ============================================================

async function listBasketDefinitions(analytics: Client): Promise<BasketDefinition[]> {
  const { data, error } = await analytics
    .from('basket_definitions')
    .select('basket_id, benchmark_code, anchor_date, anchor_index_level')
    .order('basket_id', { ascending: true });
  if (error) throw new Error(`Failed to load basket_definitions: ${error.message}`);
  const rows =
    (data as Array<{
      basket_id: string;
      benchmark_code: string | null;
      anchor_date: string | null;
      anchor_index_level: unknown;
    }> | null) ?? [];
  return rows.map((r) => ({
    basket_id: r.basket_id,
    benchmark_code: r.benchmark_code,
    anchor_date: r.anchor_date,
    anchor_index_level: toNumberOrNull(r.anchor_index_level),
  }));
}

async function listCurrentConstituents(
  analytics: Client,
  basketId: string
): Promise<BasketConstituent[]> {
  const rows: BasketConstituent[] = [];
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await analytics
      .from('basket_constituents')
      .select('local_code, weight_factor, official_weight')
      .eq('basket_id', basketId)
      .is('valid_to', null)
      .order('local_code', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) {
      throw new Error(`Failed to load basket_constituents for ${basketId}: ${error.message}`);
    }
    const page =
      (data as Array<{
        local_code: string;
        weight_factor: unknown;
        official_weight: unknown;
      }> | null) ?? [];
    for (const r of page) rows.push(parseBasketConstituentRow(r));
    if (page.length < PAGE) break;
  }
  return rows;
}

/** stock_metrics の最新 as_of_date（--date 未指定時のフォールバック） */
async function getLatestStockMetricsDate(analytics: Client): Promise<string | null> {
  const { data, error } = await analytics
    .from('stock_metrics')
    .select('as_of_date')
    .order('as_of_date', { ascending: false })
    .limit(1);
  if (error) throw new Error(`Failed to get latest stock_metrics date: ${error.message}`);
  const rows = (data as { as_of_date: string }[] | null) ?? [];
  return rows[0]?.as_of_date ?? null;
}

async function getStockMetricsForDate(
  analytics: Client,
  date: string,
  codes: string[]
): Promise<Map<string, StockMetricSnapshot>> {
  const map = new Map<string, StockMetricSnapshot>();
  if (codes.length === 0) return map;
  const { data, error } = await analytics
    .from('stock_metrics')
    .select('local_code, market_cap, per, pbr, psr, dividend_yield, close')
    .eq('as_of_date', date)
    .in('local_code', codes);
  if (error) throw new Error(`Failed to load stock_metrics for ${date}: ${error.message}`);
  const rows =
    (data as Array<{
      local_code: string;
      market_cap: unknown;
      per: unknown;
      pbr: unknown;
      psr: unknown;
      dividend_yield: unknown;
      close: unknown;
    }> | null) ?? [];
  for (const r of rows) map.set(r.local_code, parseStockMetricRow(r));
  return map;
}

/** 対象日より前の直近 basket_metrics 行（模擬指数チェーンの起点） */
async function getPreviousBasketMetricsRow(
  analytics: Client,
  basketId: string,
  beforeDate: string
): Promise<{ as_of_date: string; index_level: number | null } | null> {
  const { data, error } = await analytics
    .from('basket_metrics')
    .select('as_of_date, index_level')
    .eq('basket_id', basketId)
    .lt('as_of_date', beforeDate)
    .order('as_of_date', { ascending: false })
    .limit(1);
  if (error) {
    throw new Error(`Failed to load previous basket_metrics for ${basketId}: ${error.message}`);
  }
  const rows = (data as Array<{ as_of_date: string; index_level: unknown }> | null) ?? [];
  if (rows.length === 0) return null;
  return { as_of_date: rows[0].as_of_date, index_level: toNumberOrNull(rows[0].index_level) };
}

/** codes × dates（通常2件: 前営業日/対象日）の adj_close を local_code -> date -> value で返す */
async function getEquityBarClosesForDates(
  core: Client,
  codes: string[],
  dates: string[]
): Promise<Map<string, Map<string, number | null>>> {
  const result = new Map<string, Map<string, number | null>>();
  if (codes.length === 0 || dates.length === 0) return result;
  const { data, error } = await core
    .from('equity_bar_daily')
    .select('local_code, trade_date, adj_close')
    .in('local_code', codes)
    .in('trade_date', dates)
    .eq('session', 'DAY');
  if (error) throw new Error(`Failed to load equity_bar_daily: ${error.message}`);
  const rows =
    (data as Array<{ local_code: string; trade_date: string; adj_close: unknown }> | null) ?? [];
  for (const r of rows) {
    if (!result.has(r.local_code)) result.set(r.local_code, new Map());
    result.get(r.local_code)!.set(r.trade_date, toNumberOrNull(r.adj_close));
  }
  return result;
}

async function getEquityCloseForDate(
  core: Client,
  code: string,
  date: string
): Promise<number | null> {
  const { data, error } = await core
    .from('equity_bar_daily')
    .select('adj_close')
    .eq('local_code', code)
    .eq('trade_date', date)
    .eq('session', 'DAY')
    .limit(1);
  if (error) throw new Error(`Failed to load equity_bar_daily for ${code}: ${error.message}`);
  const rows = (data as Array<{ adj_close: unknown }> | null) ?? [];
  return rows.length > 0 ? toNumberOrNull(rows[0].adj_close) : null;
}

/**
 * 対象日以前の最新開示（disclosed_date<=対象日）の予想EPSを銘柄ごとに取得する。
 *
 * NOTE: バスケット構成銘柄数（数十件）前提で全件フェッチしJS側で銘柄ごとの最新行を選ぶ
 * （PostgRESTはDISTINCT ONを直接サポートしないため）。local_code昇順→disclosed_date降順で
 * ソートされるため、各local_codeで最初に出現する行がその銘柄の最新開示になる。
 */
async function getLatestForecastEps(
  core: Client,
  codes: string[],
  asOfDate: string
): Promise<Map<string, ForecastEpsRow>> {
  const map = new Map<string, ForecastEpsRow>();
  if (codes.length === 0) return map;
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await core
      .from('financial_disclosure')
      .select('local_code, disclosed_date, forecast_eps, next_forecast_eps')
      .in('local_code', codes)
      .lte('disclosed_date', asOfDate)
      .order('local_code', { ascending: true })
      .order('disclosed_date', { ascending: false })
      .range(offset, offset + PAGE - 1);
    if (error) throw new Error(`Failed to load financial_disclosure: ${error.message}`);
    const rows =
      (data as Array<{
        local_code: string;
        disclosed_date: string;
        forecast_eps: unknown;
        next_forecast_eps: unknown;
      }> | null) ?? [];
    for (const r of rows) {
      if (!map.has(r.local_code)) {
        map.set(r.local_code, {
          forecast_eps: toNumberOrNull(r.forecast_eps),
          next_forecast_eps: toNumberOrNull(r.next_forecast_eps),
        });
      }
    }
    if (rows.length < PAGE) break;
  }
  return map;
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    logger.error('Script failed', { error: message(error) });
    process.exit(1);
  });
