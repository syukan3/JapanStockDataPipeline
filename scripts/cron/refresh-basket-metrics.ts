/**
 * テーマバスケット日次集計 更新スクリプト（GH Actions用）
 *
 * @description
 * analytics.basket_definitions の全バスケットについて、現行構成銘柄（basket_constituents,
 * valid_to is null）の当日集計を計算し、analytics.basket_metrics へ upsert する（冪等）。
 * 計画書: docs/PLANS-basket-valuation-2026-07.md §5/§6/§8
 *
 * 計算はバックフィル（scripts/seed/basket-valuation.ts）と**同一の PIT 計算パス**
 * （src/lib/analytics/basket-valuation.ts の buildConstituentDay → aggregateBasketDay →
 * chainIndexSeries）を通す。加重バリュエーションを stock_metrics の per/pbr/psr 列から
 * 集計すると赤字銘柄 PER=NULL 等の定義差でバックフィル系列と段差が生じるため、
 * 価格は jquants_core.equity_bar_daily（生close/adj_close/adjustment_factor）、
 * 財務は financial_disclosure の PIT 参照から直接計算する（stock_metrics 非依存）。
 *
 * - バスケット定義が0件、または対象バスケットの現行構成銘柄が0件の場合はそのバスケットを
 *   スキップして正常終了する（他バスケットは継続）。
 * - 模擬指数は前営業日の basket_metrics 行がある場合のみ日次リターンで連結する。
 *   前行が一度も存在せず対象日がアンカー日と一致する場合のみ anchor_index_level で初期化。
 * - job_runs / job_locks は使わない（upsert は冪等。refresh-technical.ts と同方針）。
 * - Cron A の equity_bars 同期後段の continue-on-error ステップとして実行する想定
 *   （cron-a.yml 側。詳細は同ファイルのコメント参照）。
 *
 * 実行:
 *   npx tsx scripts/cron/refresh-basket-metrics.ts [--dry-run] [--date=YYYY-MM-DD]
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import {
  aggregateBasketDay,
  buildConstituentDay,
  chainIndexSeries,
  effectiveCoverageWeight,
  type ConstituentDay,
  type SplitEvent,
  type PitFinancials,
} from '../../src/lib/analytics/basket-valuation';
import {
  toNumberOrNull,
  parseBasketConstituentRow,
  fetchDisclosuresGrouped,
  buildPitByCode,
  type BasketConstituent,
} from '../../src/lib/analytics/basket-valuation-data';

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

/** local_code -> trade_date -> {close, adjClose} */
type BarsByCode = Map<string, Map<string, { close: number | null; adjClose: number | null }>>;

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

  const targetDate = explicitDate ?? (await getLatestTradeDate(core));
  if (!targetDate) {
    logger.warn('No equity_bar_daily data available; skipping basket', {
      basketId: basket.basket_id,
    });
    return { skipped: true, reason: 'no_equity_bars' };
  }

  // PIT 財務（disclosed_date <= 対象日）と分割イベント（バックフィルと同一の計算素材）
  const pitByCode = buildPitByCode(await fetchDisclosuresGrouped(core, codes, targetDate));
  const splitEventsByCode = await fetchSplitEvents(core, codes, targetDate);

  // 前行（模擬指数チェーンの起点）を先に引き、必要な2日分の日足をまとめて取得
  const prevRow = await getPreviousBasketMetricsRow(analytics, basket.basket_id, targetDate);
  const barDates = prevRow ? [prevRow.as_of_date, targetDate] : [targetDate];
  const barsByCode = await getBarsForDates(core, codes, barDates);

  // sector33_auto は official_weight=null で保存されるため、カバレッジは均等按分(100/N)で算出する
  // （curated は保存済みの公式ウエート%をそのまま使う）。effectiveCoverageWeight が両者を吸収する。
  const buildItems = (date: string): ConstituentDay[] => {
    const items: ConstituentDay[] = [];
    for (const c of constituents) {
      const item = buildConstituentDay(
        {
          code: c.local_code,
          factor: c.weight_factor,
          officialWeight: effectiveCoverageWeight(c.official_weight, constituents.length),
          close: barsByCode.get(c.local_code)?.get(date)?.close,
          pit: pitByCode.get(c.local_code) ?? emptyPit(),
          events: splitEventsByCode.get(c.local_code) ?? [],
        },
        date
      );
      if (item) items.push(item);
    }
    return items;
  };

  const currItems = buildItems(targetDate);
  if (currItems.length === 0) {
    logger.warn('No constituent has price+PIT data for target date; skipping basket', {
      basketId: basket.basket_id,
      targetDate,
    });
    return { skipped: true, reason: 'no_constituent_data', targetDate };
  }
  const agg = aggregateBasketDay(currItems);

  // 模擬指数の連結（バックフィルと同じ chainIndexSeries を前行→対象日の2日系列で適用）
  let indexLevel: number | null = null;
  if (!prevRow) {
    if (basket.anchor_date === targetDate && basket.anchor_index_level != null) {
      indexLevel = basket.anchor_index_level;
    }
  } else if (prevRow.index_level != null) {
    const prevAgg = aggregateBasketDay(buildItems(prevRow.as_of_date));
    const adjCloseByCode = new Map<string, Map<string, number>>();
    for (const [code, dates] of barsByCode) {
      const adjMap = new Map<string, number>();
      for (const [date, bar] of dates) {
        if (bar.adjClose != null) adjMap.set(date, bar.adjClose);
      }
      adjCloseByCode.set(code, adjMap);
    }
    const levels = chainIndexSeries(
      [prevRow.as_of_date, targetDate],
      new Map([[prevRow.as_of_date, prevAgg.weights]]),
      adjCloseByCode,
      prevRow.as_of_date,
      prevRow.index_level
    );
    indexLevel = levels.get(targetDate) ?? null;
  }

  const etfClose = basket.benchmark_code
    ? (barsByCode.get(basket.benchmark_code) ??
        (await getBarsForDates(core, [basket.benchmark_code], [targetDate])).get(
          basket.benchmark_code
        ))?.get(targetDate)?.adjClose ?? null
    : null;

  // 丸め桁はバックフィル（seed/basket-valuation.ts）と揃える
  const round = (v: number | null | undefined, dp: number): number | null =>
    v == null ? null : Number(v.toFixed(dp));
  const epsLevel =
    indexLevel != null && agg.weightedPer != null && agg.weightedPer > 0
      ? indexLevel / agg.weightedPer
      : null;

  const row = {
    basket_id: basket.basket_id,
    as_of_date: targetDate,
    index_level: round(indexLevel, 4),
    etf_close: round(etfClose, 4),
    weighted_per: round(agg.weightedPer, 2),
    weighted_per_forward: round(agg.weightedPerForward, 2),
    weighted_pbr: round(agg.weightedPbr, 2),
    weighted_psr: round(agg.weightedPsr, 2),
    weighted_div_yield: round(agg.weightedDivYield, 3),
    weighted_eps_level: round(epsLevel, 4),
    coverage_pct: round(agg.coveragePct, 1),
    updated_at: new Date().toISOString(),
  };

  if (dryRun) {
    logger.info('Dry run: would upsert basket_metrics', { basketId: basket.basket_id, row });
    return {
      dryRun: true,
      targetDate,
      constituents: constituents.length,
      coveredConstituents: currItems.length,
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
    coveredConstituents: currItems.length,
    coveragePct: row.coverage_pct,
    upserted: true,
  };
}

function emptyPit(): PitFinancials {
  return { fy: [], forward: [] };
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

/** equity_bar_daily の最新 trade_date（--date 未指定時のフォールバック） */
async function getLatestTradeDate(core: Client): Promise<string | null> {
  const { data, error } = await core
    .from('equity_bar_daily')
    .select('trade_date')
    .eq('session', 'DAY')
    .order('trade_date', { ascending: false })
    .limit(1);
  if (error) throw new Error(`Failed to get latest trade date: ${error.message}`);
  const rows = (data as { trade_date: string }[] | null) ?? [];
  return rows[0]?.trade_date ?? null;
}

/** 対象日より前の直近 basket_metrics 行（模擬指数チェーンの起点） */
async function getPreviousBasketMetricsRow(
  analytics: Client,
  basketId: string,
  beforeDate: string
): Promise<{ as_of_date: string; index_level: number | null } | null> {
  // 起点は index_level が非null の直近行にする: 全銘柄欠損等で一度 null 行を書いても
  // 翌日以降はその手前の確定値から連結し自己回復する（直前行固定だと null が伝播し続ける）
  const { data, error } = await analytics
    .from('basket_metrics')
    .select('as_of_date, index_level')
    .eq('basket_id', basketId)
    .lt('as_of_date', beforeDate)
    .not('index_level', 'is', null)
    .order('as_of_date', { ascending: false })
    .limit(1);
  if (error) {
    throw new Error(`Failed to load previous basket_metrics for ${basketId}: ${error.message}`);
  }
  const rows = (data as Array<{ as_of_date: string; index_level: unknown }> | null) ?? [];
  if (rows.length === 0) return null;
  return { as_of_date: rows[0].as_of_date, index_level: toNumberOrNull(rows[0].index_level) };
}

/** codes × dates（通常2件: 前営業日/対象日）の 生close/adj_close を取得する */
async function getBarsForDates(
  core: Client,
  codes: string[],
  dates: string[]
): Promise<BarsByCode> {
  const result: BarsByCode = new Map();
  if (codes.length === 0 || dates.length === 0) return result;
  const { data, error } = await core
    .from('equity_bar_daily')
    .select('local_code, trade_date, close, adj_close')
    .in('local_code', codes)
    .in('trade_date', dates)
    .eq('session', 'DAY');
  if (error) throw new Error(`Failed to load equity_bar_daily: ${error.message}`);
  const rows =
    (data as Array<{
      local_code: string;
      trade_date: string;
      close: unknown;
      adj_close: unknown;
    }> | null) ?? [];
  for (const r of rows) {
    if (!result.has(r.local_code)) result.set(r.local_code, new Map());
    result.get(r.local_code)!.set(r.trade_date, {
      close: toNumberOrNull(r.close),
      adjClose: toNumberOrNull(r.adj_close),
    });
  }
  return result;
}

/**
 * 構成銘柄の分割・併合イベント（adjustment_factor <> 1）を取得する。
 *
 * NOTE: equity_bar_daily はアーカイブ安全弁でローリング保持（現状 2025-04-21〜）のため、
 * それ以前のイベントは拾えない。PIT の FY 開示は年次で更新され、必要な補正窓は
 * 「最新開示日〜対象日」（高々1年強）なので実用上はアーカイブ窓に収まる。
 */
async function fetchSplitEvents(
  core: Client,
  codes: string[],
  toDate: string
): Promise<Map<string, SplitEvent[]>> {
  const result = new Map<string, SplitEvent[]>();
  if (codes.length === 0) return result;
  const { data, error } = await core
    .from('equity_bar_daily')
    .select('local_code, trade_date, adjustment_factor')
    .in('local_code', codes)
    .lte('trade_date', toDate)
    .eq('session', 'DAY')
    .not('adjustment_factor', 'is', null)
    .neq('adjustment_factor', 1)
    .order('trade_date', { ascending: true });
  if (error) throw new Error(`Failed to load split events: ${error.message}`);
  const rows =
    (data as Array<{
      local_code: string;
      trade_date: string;
      adjustment_factor: unknown;
    }> | null) ?? [];
  for (const r of rows) {
    const factor = toNumberOrNull(r.adjustment_factor);
    if (factor == null || factor <= 0 || factor === 1) continue;
    if (!result.has(r.local_code)) result.set(r.local_code, []);
    const events = result.get(r.local_code)!;
    // 同一 trade_date の重複 session 行があっても二重掛けしない
    if (events.some((e) => e.date === r.trade_date)) continue;
    events.push({ date: r.trade_date, factor });
  }
  return result;
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    logger.error('Script failed', { error: message(error) });
    process.exit(1);
  });
