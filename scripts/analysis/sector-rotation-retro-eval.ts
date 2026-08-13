#!/usr/bin/env tsx
/**
 * セクターローテーション遡及評価（Issue EV-1 / Phase 1・研究スクリプト）
 *
 * @description 「4象限ローテーション × 業種マクロゲート × ETF4本選定」ルールを
 * 2019〜2026 に look-ahead なしで遡及適用し、対TOPIX超過リターンと象限別フォワード
 * リターンを評価する。**本番DBはSELECTのみ**。スキーマ変更・書き込みは一切しない。
 *
 * 計画書: ../docs/PLANS-sector-rotation-2026-08.md（ルートリポ）§1〜§5, §7 Phase 1
 * 出力先: ../docs/EVAL-sector-rotation-2026-08.md（人手で執筆。本スクリプトは素材を出す）
 *
 * ## データの出所と、そこから来る制約
 * - 資金軸 / ETF価格: analytics.basket_metrics（index_level・etf_close、2019-04〜日次）
 * - TOPIX: jquants_core.topix_bar_daily（2016-07〜）
 * - 業績軸: jquants_core.financial_disclosure（2019-01〜）の PIT 参照
 * - 空売り: analytics.short_selling_sector（2016-07〜・sector33別）
 * - マクロ: jquants_core.macro_indicator_daily（32系列）
 * - 構成銘柄の時価総額ウエート: **J-Quants API の月末スナップショット**。
 *   jquants_core.equity_bar_daily はアーカイブ安全弁で直近約15か月しか残っていないため
 *   （実測 2025-05-07〜）、過去の株価はDBから取れない。seed/basket-valuation.ts と同じく
 *   API から直接取得し、ローカルにキャッシュする（DBへは書かない）。
 *
 * ## 実行方法
 * ```
 * npx tsx scripts/analysis/sector-rotation-retro-eval.ts --build-cache   # 初回（API取得あり・約10分）
 * npx tsx scripts/analysis/sector-rotation-retro-eval.ts                 # キャッシュから再計算（数秒）
 * npx tsx scripts/analysis/sector-rotation-retro-eval.ts --json out.json # 結果をJSONでも出す
 * オプション: --cache-dir <path> / --lag-mode hybrid|assumed|released_at / --from YYYY-MM-DD
 * ```
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import { loadEnv } from '../seed/_shared';
import {
  hacRegression,
  hacTStat,
  overlapLag,
  meanOf,
  stdevOf,
  normalCdf,
  twoSidedP,
  maxDrawdown,
  annualize,
  informationRatio,
  percentileRank,
  spearman,
} from './sector-rotation-stats';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = any;

// ============================================================
// 設定
// ============================================================

/** 週次評価の開始日。業績軸は前年同期比が要るため 2020 年からしか成立しない（§データ可用性） */
const DEFAULT_FROM = '2019-07-01';

/** 資金軸の相対リターン窓（営業日） */
const FLOW_WINDOW = 60;

/** 資金軸の中立バンド（±%） */
const NEUTRAL_BAND = 0.01;

/** マクロドライバーの変化窓（営業日） */
const MACRO_WINDOW = 63;

/** マクロ z スコアのトレーリング窓（営業日・約5年） */
const MACRO_Z_WINDOW = 1250;

/** マクロ z スコアに必要な最小観測数 */
const MACRO_Z_MIN_OBS = 250;

/** マクロゲートの通過閾値（0-100） */
const MACRO_GATE_PASS = 50;

/** ETF流動性ゲート: 直近20営業日の平均売買代金の下限（円） */
const LIQUIDITY_MIN_TURNOVER = 50_000_000;
const LIQUIDITY_WINDOW = 20;

/** 選定するETFの最大本数 */
const MAX_PICKS = 4;

/** フォワードリターンの評価ホライズン（営業日） */
const HORIZONS = [20, 60, 120];

/** 週次1期 = 5営業日 */
const PERIOD_BUSINESS_DAYS = 5;
const WEEKS_PER_YEAR = 52;

/** 片道コスト感応度（bp） */
const COST_BPS = [0, 10, 30];

/** 業績集約のカバレッジ下限（これ未満の basket×月は業績軸を欠測扱い） */
const FUNDAMENTAL_MIN_COVERAGE = 0.5;

/**
 * 業種マクロゲートのドライバー対応表（計画書§4を、実在する系列だけで再構成したもの）。
 *
 * 計画書からの逸脱（EVALに明記する）:
 * - ISM/PMI は macro_series_metadata に存在しない（NAPM は 00023 で FRED 廃止に伴い削除済）。
 *   → 米鉱工業生産 INDPRO と景気動向指数CI先行 estat_ci_leading で代替。
 * - HYスプレッド BAMLH0A0HYM2 は 2023-07-25 以降しか無く、2019〜の評価に使うと
 *   期間途中でスコア定義が変わる。→ リスク環境系は VIXCLS と SP500 で構成。
 * - 日銀政策金利 IRSTCI01JPM156N は月次。不動産の金利ドライバーは JGB20年（日次）を主とする。
 *
 * kind: 'rate' は差分（%ポイント）、'level' は対数変化率で「63営業日変化」を測る。
 */
interface MacroDriver {
  seriesId: string;
  /** +1: 上昇がその業種に追い風 / -1: 逆風 */
  direction: 1 | -1;
  kind: 'rate' | 'level';
}

const MACRO_DRIVERS: Record<string, MacroDriver[]> = {
  'topix33-banks-1615': [
    { seriesId: 'mof_jgb_20y', direction: 1, kind: 'rate' },
    { seriesId: 'mof_jgb_30y', direction: 1, kind: 'rate' },
    { seriesId: 'T10Y2Y', direction: 1, kind: 'rate' },
  ],
  'topix33-realestate-1633': [
    { seriesId: 'mof_jgb_20y', direction: -1, kind: 'rate' },
    { seriesId: 'mof_jgb_30y', direction: -1, kind: 'rate' },
  ],
  'topix33-transportequip-1622': [
    { seriesId: 'DEXJPUS', direction: 1, kind: 'level' },
    { seriesId: 'RSXFS', direction: 1, kind: 'level' },
    { seriesId: 'UMCSENT', direction: 1, kind: 'level' },
  ],
  'topix33-machinery-1624': [
    { seriesId: 'INDPRO', direction: 1, kind: 'level' },
    { seriesId: 'estat_ci_leading', direction: 1, kind: 'level' },
  ],
  'topix33-wholesale-1629': [
    { seriesId: 'DCOILWTICO', direction: 1, kind: 'level' },
    { seriesId: 'PCOPPUSDM', direction: 1, kind: 'level' },
    { seriesId: 'INDPRO', direction: 1, kind: 'level' },
  ],
  'topix33-chemical-1620': [
    { seriesId: 'DCOILWTICO', direction: -1, kind: 'level' },
    { seriesId: 'INDPRO', direction: 1, kind: 'level' },
  ],
  'topix33-steel-1623': [
    { seriesId: 'PCOPPUSDM', direction: 1, kind: 'level' },
    { seriesId: 'INDPRO', direction: 1, kind: 'level' },
  ],
  'topix33-utilities-1627': [
    { seriesId: 'DCOILWTICO', direction: -1, kind: 'level' },
    { seriesId: 'mof_jgb_20y', direction: -1, kind: 'rate' },
  ],
  'topix33-retail-1630': [
    { seriesId: 'estat_core_cpi', direction: -1, kind: 'level' },
    { seriesId: 'CSCICP02JPM460S', direction: 1, kind: 'level' },
  ],
  'topix33-foods-1617': [
    { seriesId: 'estat_core_cpi', direction: -1, kind: 'level' },
    { seriesId: 'CSCICP02JPM460S', direction: 1, kind: 'level' },
  ],
  'topix33-pharma-1621': [
    { seriesId: 'VIXCLS', direction: 1, kind: 'level' },
    { seriesId: 'DEXJPUS', direction: 1, kind: 'level' },
  ],
  'topix33-elec-1625': [
    { seriesId: 'VIXCLS', direction: -1, kind: 'level' },
    { seriesId: 'SP500', direction: 1, kind: 'level' },
  ],
  'nkscd-200a': [
    { seriesId: 'VIXCLS', direction: -1, kind: 'level' },
    { seriesId: 'SP500', direction: 1, kind: 'level' },
  ],
  'physical-ai-2638': [
    { seriesId: 'VIXCLS', direction: -1, kind: 'level' },
    { seriesId: 'SP500', direction: 1, kind: 'level' },
  ],
};

/** released_at が「実際の公表日」として妥当と見なせる上限（日）。Scouter macro-regime-forward-eval と同一 */
const RELEASED_AT_PLAUSIBLE_MAX_DAYS = 400;

// ============================================================
// CLI
// ============================================================

interface CliOptions {
  buildCache: boolean;
  refreshPrices: boolean;
  cacheDir: string;
  lagMode: 'hybrid' | 'assumed' | 'released_at';
  from: string;
  json: string | null;
}

function parseArgs(): CliOptions {
  const argv = process.argv.slice(2);
  const o: CliOptions = {
    buildCache: false,
    refreshPrices: false,
    cacheDir: join(tmpdir(), 'sector-rotation-eval-cache'),
    lagMode: 'hybrid',
    from: DEFAULT_FROM,
    json: null,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === '--build-cache') o.buildCache = true;
    else if (a === '--refresh-prices') { o.buildCache = true; o.refreshPrices = true; }
    else if (a === '--cache-dir' && next) { o.cacheDir = next; i++; }
    else if (a === '--lag-mode' && next) { o.lagMode = next as CliOptions['lagMode']; i++; }
    else if (a === '--from' && next) { o.from = next; i++; }
    else if (a === '--json' && next) { o.json = next; i++; }
  }
  return o;
}

// ============================================================
// 汎用ユーティリティ
// ============================================================

function num(v: unknown): number | null {
  if (v == null) return null;
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

function addDays(date: string, n: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

function shiftYear(date: string, n: number): string {
  const [y, m, d] = date.split('-').map(Number);
  const last = new Date(Date.UTC(y + n, m, 0)).getUTCDate();
  return `${y + n}-${String(m).padStart(2, '0')}-${String(Math.min(d, last)).padStart(2, '0')}`;
}

/** PostgREST の1000行上限を越えてページングする */
async function pageAll<T>(build: (from: number, to: number) => Client): Promise<T[]> {
  const out: T[] = [];
  const PAGE = 1000;
  for (let off = 0; ; off += PAGE) {
    const { data, error } = await build(off, off + PAGE - 1);
    if (error) throw new Error(`query failed: ${error.message}`);
    const rows = (data ?? []) as T[];
    out.push(...rows);
    if (rows.length < PAGE) break;
  }
  return out;
}

function readJson<T>(path: string): T {
  return JSON.parse(readFileSync(path, 'utf-8')) as T;
}

function fmt(v: number | null | undefined, digits = 2): string {
  return v == null || !Number.isFinite(v) ? '  n/a' : v.toFixed(digits);
}

function pct(v: number | null | undefined, digits = 1): string {
  return v == null || !Number.isFinite(v) ? 'n/a' : `${(v * 100).toFixed(digits)}%`;
}

// ============================================================
// キャッシュ層（DB + J-Quants → ローカルJSON）
// ============================================================

interface BasketDef {
  basket_id: string;
  display_name: string;
  benchmark_code: string;
  constituent_source: string;
  sector33_filter: string | null;
  display_order: number;
}

interface ConstituentRow {
  basket_id: string;
  local_code: string;
  weight_factor: number;
}

interface MetricRow {
  basket_id: string;
  as_of_date: string;
  index_level: number | null;
  etf_close: number | null;
}

interface DisclosureRow {
  local_code: string;
  disclosed_date: string;
  disclosed_time: string | null;
  disclosure_id: string;
  period_type: string | null;
  fiscal_year_end: string | null;
  sales: number | null;
  operating_profit: number | null;
  ordinary_profit: number | null;
  net_income: number | null;
  equity: number | null;
  forecast_op: number | null;
  forecast_odp: number | null;
  shares_outstanding_fy: number | null;
}

interface ShortSectorRow {
  as_of_date: string;
  sector33_code: string;
  selling_ex_short_value: number | null;
  short_with_restrictions_value: number | null;
  short_without_restrictions_value: number | null;
}

interface MacroRow {
  series_id: string;
  indicator_date: string;
  value: number | null;
  released_at: string | null;
}

/** 月末スナップショット: date -> code -> {close, adjClose} */
type PricePanel = Record<string, Record<string, [number, number]>>;

/** ETF日次: code -> [date, adjClose, turnover][] */
type EtfSeries = Record<string, [string, number, number][]>;

interface CacheBundle {
  defs: BasketDef[];
  constituents: ConstituentRow[];
  metrics: MetricRow[];
  topix: [string, number][];
  disclosures: DisclosureRow[];
  shortSector: ShortSectorRow[];
  macro: MacroRow[];
  sectorByCode: Record<string, string>;
  sectorNameByCode: Record<string, string>;
  panel: PricePanel;
  etf: EtfSeries;
  builtAt: string;
}

const CACHE_FILES = {
  db: 'db.json',
  panel: 'price-panel.json',
  etf: 'etf-series.json',
} as const;

async function buildDbCache(cacheDir: string): Promise<Omit<CacheBundle, 'panel' | 'etf'>> {
  const { createAdminClient } = await import('../../src/lib/supabase/admin');
  const core: Client = createAdminClient('jquants_core');
  const an: Client = createAdminClient('analytics');

  console.log('[cache] basket_definitions / constituents ...');
  const defs = (await pageAll<BasketDef>((f, t) =>
    an
      .from('basket_definitions')
      .select('basket_id, display_name, benchmark_code, constituent_source, sector33_filter, display_order')
      .order('display_order')
      .range(f, t)
  )).map((d) => ({ ...d, display_order: Number(d.display_order) }));

  const consRaw = await pageAll<Client>((f, t) =>
    an
      .from('basket_constituents')
      .select('basket_id, local_code, weight_factor')
      .is('valid_to', null)
      .order('basket_id')
      .order('local_code')
      .range(f, t)
  );
  const constituents: ConstituentRow[] = consRaw.map((r: Client) => ({
    basket_id: r.basket_id,
    local_code: r.local_code,
    weight_factor: num(r.weight_factor) ?? 0,
  }));
  console.log(`         ${defs.length} baskets, ${constituents.length} constituents`);

  console.log('[cache] basket_metrics ...');
  const metricsRaw = await pageAll<Client>((f, t) =>
    an
      .from('basket_metrics')
      .select('basket_id, as_of_date, index_level, etf_close')
      .order('basket_id')
      .order('as_of_date')
      .range(f, t)
  );
  const metrics: MetricRow[] = metricsRaw.map((r: Client) => ({
    basket_id: r.basket_id,
    as_of_date: r.as_of_date,
    index_level: num(r.index_level),
    etf_close: num(r.etf_close),
  }));
  console.log(`         ${metrics.length} rows`);

  console.log('[cache] topix_bar_daily ...');
  const topixRaw = await pageAll<Client>((f, t) =>
    core.from('topix_bar_daily').select('trade_date, close').order('trade_date').range(f, t)
  );
  const topix: [string, number][] = topixRaw
    .map((r: Client) => [r.trade_date, num(r.close)] as [string, number | null])
    .filter((x): x is [string, number] => x[1] != null);
  console.log(`         ${topix.length} rows`);

  console.log('[cache] equity_master (sector33) ...');
  const emRaw = await pageAll<Client>((f, t) =>
    core
      .from('equity_master')
      .select('local_code, sector33_code, sector33_name')
      .eq('is_current', true)
      .order('local_code')
      .range(f, t)
  );
  const sectorByCode: Record<string, string> = {};
  const sectorNameByCode: Record<string, string> = {};
  for (const r of emRaw as Client[]) {
    if (r.sector33_code) {
      sectorByCode[r.local_code] = r.sector33_code;
      sectorNameByCode[r.sector33_code] = r.sector33_name;
    }
  }

  console.log('[cache] financial_disclosure (constituents) ...');
  const codes = [...new Set(constituents.map((c) => c.local_code))];
  const disclosures: DisclosureRow[] = [];
  const CHUNK = 120;
  for (let i = 0; i < codes.length; i += CHUNK) {
    const chunk = codes.slice(i, i + CHUNK);
    const rows = await pageAll<Client>((f, t) =>
      core
        .from('financial_disclosure')
        .select(
          'local_code, disclosed_date, disclosed_time, disclosure_id, period_type, fiscal_year_end, ' +
            'sales, operating_profit, ordinary_profit, net_income, equity, forecast_op, forecast_odp, shares_outstanding_fy'
        )
        .in('local_code', chunk)
        .order('local_code')
        .order('disclosed_date')
        .order('disclosure_id')
        .range(f, t)
    );
    for (const r of rows as Client[]) {
      disclosures.push({
        local_code: r.local_code,
        disclosed_date: r.disclosed_date,
        disclosed_time: r.disclosed_time,
        disclosure_id: r.disclosure_id,
        period_type: r.period_type,
        fiscal_year_end: r.fiscal_year_end,
        sales: num(r.sales),
        operating_profit: num(r.operating_profit),
        ordinary_profit: num(r.ordinary_profit),
        net_income: num(r.net_income),
        equity: num(r.equity),
        forecast_op: num(r.forecast_op),
        forecast_odp: num(r.forecast_odp),
        shares_outstanding_fy: num(r.shares_outstanding_fy),
      });
    }
    process.stdout.write(`\r         ${Math.min(i + CHUNK, codes.length)}/${codes.length} codes, ${disclosures.length} rows`);
  }
  console.log('');

  console.log('[cache] short_selling_sector ...');
  const ssRaw = await pageAll<Client>((f, t) =>
    an
      .from('short_selling_sector')
      .select(
        'as_of_date, sector33_code, selling_ex_short_value, short_with_restrictions_value, short_without_restrictions_value'
      )
      .order('as_of_date')
      .order('sector33_code')
      .range(f, t)
  );
  const shortSector: ShortSectorRow[] = ssRaw.map((r: Client) => ({
    as_of_date: r.as_of_date,
    sector33_code: r.sector33_code,
    selling_ex_short_value: num(r.selling_ex_short_value),
    short_with_restrictions_value: num(r.short_with_restrictions_value),
    short_without_restrictions_value: num(r.short_without_restrictions_value),
  }));
  console.log(`         ${shortSector.length} rows`);

  console.log('[cache] macro_indicator_daily ...');
  const wanted = [...new Set(Object.values(MACRO_DRIVERS).flat().map((d) => d.seriesId))];
  const macroRaw = await pageAll<Client>((f, t) =>
    core
      .from('macro_indicator_daily')
      .select('series_id, indicator_date, value, released_at')
      .in('series_id', wanted)
      .order('series_id')
      .order('indicator_date')
      .range(f, t)
  );
  const macro: MacroRow[] = macroRaw.map((r: Client) => ({
    series_id: r.series_id,
    indicator_date: r.indicator_date,
    value: num(r.value),
    released_at: r.released_at,
  }));
  console.log(`         ${macro.length} rows over ${wanted.length} series`);

  const bundle = {
    defs,
    constituents,
    metrics,
    topix,
    disclosures,
    shortSector,
    macro,
    sectorByCode,
    sectorNameByCode,
    builtAt: new Date().toISOString(),
  };
  writeFileSync(join(cacheDir, CACHE_FILES.db), JSON.stringify(bundle));
  return bundle;
}

/**
 * 月末スナップショットと ETF 日次系列を J-Quants API から取得する。
 *
 * equity_bar_daily はアーカイブで直近約15か月しか残っていないため、過去の株価はAPIからしか
 * 取れない（seed/basket-valuation.ts と同じ判断）。DBへの書き込みは一切しない。
 */
async function buildPriceCache(
  cacheDir: string,
  db: Omit<CacheBundle, 'panel' | 'etf'>,
  from: string,
  refresh: boolean
): Promise<{ panel: PricePanel; etf: EtfSeries }> {
  const panelPath = join(cacheDir, CACHE_FILES.panel);
  const etfPath = join(cacheDir, CACHE_FILES.etf);
  const { createJQuantsClient } = await import('../../src/lib/jquants/client');
  const jq = createJQuantsClient();

  // 月末営業日（TOPIX の営業日カレンダーから導出）。開示日は 2019-01 から効くので
  // 分割補正の階段を欠かさないよう 2019-01 から採る。
  const tradeDates = db.topix.map((t) => t[0]).filter((d) => d >= '2019-01-01');
  const monthEnds: string[] = [];
  for (let i = 0; i < tradeDates.length; i++) {
    const cur = tradeDates[i];
    const next = tradeDates[i + 1];
    if (!next || next.slice(0, 7) !== cur.slice(0, 7)) monthEnds.push(cur);
  }
  const codeSet = new Set(db.constituents.map((c) => c.local_code));

  let panel: PricePanel = {};
  if (!refresh && existsSync(panelPath)) {
    panel = readJson<PricePanel>(panelPath);
    console.log(`[cache] price panel: reusing ${Object.keys(panel).length} cached dates`);
  }
  const todo = monthEnds.filter((d) => !panel[d]);
  console.log(`[cache] price panel: ${todo.length} month-end snapshots to fetch (of ${monthEnds.length})`);
  let done = 0;
  for (const d of todo) {
    const rows = await jq.getEquityBarsDaily({ date: d });
    const day: Record<string, [number, number]> = {};
    for (const r of rows) {
      if (!codeSet.has(r.Code)) continue;
      const c = num(r.C);
      const ac = num((r as Client).AdjC);
      if (c == null || ac == null || c <= 0 || ac <= 0) continue;
      day[r.Code] = [c, ac];
    }
    panel[d] = day;
    done++;
    if (done % 5 === 0 || done === todo.length) {
      process.stdout.write(`\r         ${done}/${todo.length} snapshots (${d}, ${Object.keys(day).length} codes)`);
      writeFileSync(panelPath, JSON.stringify(panel));
    }
  }
  if (todo.length) console.log('');
  writeFileSync(panelPath, JSON.stringify(panel));

  // ETF 日次（終値・売買代金）。14本だけなので銘柄コード指定で全期間を取る。
  let etf: EtfSeries = {};
  if (!refresh && existsSync(etfPath)) etf = readJson<EtfSeries>(etfPath);
  const etfCodes = db.defs.map((d) => d.benchmark_code);
  for (const code of etfCodes) {
    if (etf[code]?.length) continue;
    console.log(`[cache] ETF ${code} daily bars ...`);
    const rows = await jq.getEquityBarsDaily({ code, from: addDays(from, -400), to: '2026-12-31' });
    const series: [string, number, number][] = [];
    for (const r of rows) {
      const ac = num((r as Client).AdjC);
      const va = num(r.Va);
      if (ac == null || ac <= 0) continue;
      series.push([r.Date, ac, va ?? 0]);
    }
    series.sort((a, b) => (a[0] < b[0] ? -1 : 1));
    etf[code] = series;
    console.log(`         ${series.length} bars ${series[0]?.[0] ?? '-'} .. ${series.at(-1)?.[0] ?? '-'}`);
    writeFileSync(etfPath, JSON.stringify(etf));
  }
  writeFileSync(etfPath, JSON.stringify(etf));
  return { panel, etf };
}

async function loadCache(o: CliOptions): Promise<CacheBundle> {
  mkdirSync(o.cacheDir, { recursive: true });
  const dbPath = join(o.cacheDir, CACHE_FILES.db);
  let db: Omit<CacheBundle, 'panel' | 'etf'>;
  if (o.buildCache || !existsSync(dbPath)) {
    db = await buildDbCache(o.cacheDir);
  } else {
    console.log('[cache] reusing db.json');
    db = readJson<Omit<CacheBundle, 'panel' | 'etf'>>(dbPath);
  }
  const panelPath = join(o.cacheDir, CACHE_FILES.panel);
  let panel: PricePanel;
  let etf: EtfSeries;
  if (o.buildCache || !existsSync(panelPath)) {
    const built = await buildPriceCache(o.cacheDir, db, o.from, o.refreshPrices);
    panel = built.panel;
    etf = built.etf;
  } else {
    console.log('[cache] reusing price-panel.json / etf-series.json');
    panel = readJson<PricePanel>(panelPath);
    etf = readJson<EtfSeries>(join(o.cacheDir, CACHE_FILES.etf));
  }
  return { ...db, panel, etf };
}

// ============================================================
// PIT 財務（業績軸）
// ============================================================

/** 利益は営業利益を主、無ければ経常利益（銀行・保険は営業利益を開示しない） */
function profitOf(r: DisclosureRow): number | null {
  return r.operating_profit ?? r.ordinary_profit;
}
function forecastProfitOf(r: DisclosureRow): number | null {
  return r.forecast_op ?? r.forecast_odp;
}

interface PeriodRecord {
  disclosedDate: string;
  periodType: string;
  fiscalYearEnd: string;
  profit: number | null;
  forecastProfit: number | null;
  netIncome: number | null;
  equity: number | null;
  shares: number | null;
  sales: number | null;
}

/** 銘柄ごとの開示履歴（disclosed_date 昇順） */
type CodeHistory = PeriodRecord[];

function buildHistories(rows: DisclosureRow[]): Map<string, CodeHistory> {
  const byCode = new Map<string, CodeHistory>();
  for (const r of rows) {
    if (!r.disclosed_date || !r.fiscal_year_end || !r.period_type) continue;
    let list = byCode.get(r.local_code);
    if (!list) { list = []; byCode.set(r.local_code, list); }
    list.push({
      disclosedDate: r.disclosed_date,
      periodType: r.period_type,
      fiscalYearEnd: r.fiscal_year_end,
      profit: profitOf(r),
      forecastProfit: forecastProfitOf(r),
      netIncome: r.net_income,
      equity: r.equity,
      shares: r.shares_outstanding_fy,
      sales: r.sales,
    });
  }
  for (const list of byCode.values()) {
    list.sort((a, b) => (a.disclosedDate === b.disclosedDate ? 0 : a.disclosedDate < b.disclosedDate ? -1 : 1));
  }
  return byCode;
}

interface PitFundamentals {
  /** 直近開示（実績利益あり）の同期間前年比 */
  opYoY: number | null;
  /** 進行期の会社予想利益 ÷ 直近FY実績利益 − 1 */
  forecastGrowth: number | null;
  /** 直近FY実績の 純利益 ÷ 自己資本 */
  roe: number | null;
  /** 時価総額算出に使う PIT 株式数（開示日基準）と、その開示日 */
  shares: number | null;
  sharesDisclosedDate: string | null;
}

/**
 * t 時点で参照可能な開示だけを使って業績3指標を組む。
 *
 * - 実績利益YoY: t 以前の最新開示（利益あり）を取り、同じ period_type かつ
 *   fiscal_year_end が1年前の開示（これも t 以前）と比較する。前年が赤字（<=0）なら
 *   増益率が意味を持たないので null（符号の反転で順位が壊れるのを避ける）。
 * - 予想増益率: 進行期の会社予想（forecast_op ?? forecast_odp）÷ 直近FY実績利益 − 1。
 *   next_forecast_* は開示率18.7%しかなく系列として使えないため、「来期」ではなく
 *   「進行期」の予想を用いる（計画書§3からの逸脱・EVALに明記）。
 * - ROE: 直近FY実績の net_income / equity（financial_disclosure.roe は非NULLが5行しかない）。
 */
function pitFundamentals(history: CodeHistory, asOf: string): PitFundamentals {
  const visible: PeriodRecord[] = [];
  for (const rec of history) {
    if (rec.disclosedDate > asOf) break;
    visible.push(rec);
  }
  if (visible.length === 0) {
    return { opYoY: null, forecastGrowth: null, roe: null, shares: null, sharesDisclosedDate: null };
  }

  // 期間キー -> 最新開示（訂正は新しい方が勝つ）
  const byPeriod = new Map<string, PeriodRecord>();
  for (const rec of visible) {
    const key = `${rec.periodType}|${rec.fiscalYearEnd}`;
    const prev = byPeriod.get(key);
    if (!prev || rec.disclosedDate >= prev.disclosedDate) byPeriod.set(key, rec);
  }

  // 直近の「実績利益あり」開示
  let latestProfit: PeriodRecord | null = null;
  for (const rec of visible) if (rec.profit != null) latestProfit = rec;

  let opYoY: number | null = null;
  if (latestProfit) {
    const priorKey = `${latestProfit.periodType}|${shiftYear(latestProfit.fiscalYearEnd, -1)}`;
    let prior = byPeriod.get(priorKey) ?? null;
    if (!prior || prior.profit == null) {
      // 決算期変更等で年度末が数日ずれる場合の近傍一致
      const target = new Date(shiftYear(latestProfit.fiscalYearEnd, -1)).getTime();
      for (const [k, rec] of byPeriod) {
        if (!k.startsWith(`${latestProfit.periodType}|`) || rec.profit == null) continue;
        if (Math.abs(new Date(rec.fiscalYearEnd).getTime() - target) <= 10 * 86400000) { prior = rec; break; }
      }
    }
    if (prior?.profit != null && prior.profit > 0 && latestProfit.profit != null) {
      opYoY = latestProfit.profit / prior.profit - 1;
    }
  }

  // 直近の FY 実績（sales 非NULL の FY 短信本体のみ。予想修正行を除く）
  let latestFy: PeriodRecord | null = null;
  for (const rec of visible) {
    if (rec.periodType !== 'FY' || rec.sales == null) continue;
    if (!latestFy || rec.fiscalYearEnd > latestFy.fiscalYearEnd ||
      (rec.fiscalYearEnd === latestFy.fiscalYearEnd && rec.disclosedDate >= latestFy.disclosedDate)) {
      latestFy = rec;
    }
  }

  // 進行期の会社予想（直近FY年度末より先の年度を対象とする予想のうち最新開示）
  let forecastGrowth: number | null = null;
  if (latestFy?.profit != null && latestFy.profit > 0) {
    let bestForecast: PeriodRecord | null = null;
    for (const rec of visible) {
      if (rec.forecastProfit == null) continue;
      if (rec.fiscalYearEnd <= latestFy.fiscalYearEnd) continue;
      bestForecast = rec; // disclosed 昇順なので最後に残るのが最新
    }
    if (bestForecast?.forecastProfit != null) {
      forecastGrowth = bestForecast.forecastProfit / latestFy.profit - 1;
    }
  }

  const roe =
    latestFy?.netIncome != null && latestFy.equity != null && latestFy.equity > 0
      ? latestFy.netIncome / latestFy.equity
      : null;

  // 株式数は FY 開示のものを使う（buildConstituentDay と同じ考え方）
  const shares = latestFy?.shares ?? null;
  return { opYoY, forecastGrowth, roe, shares, sharesDisclosedDate: latestFy?.disclosedDate ?? null };
}

// ============================================================
// メイン評価
// ============================================================

interface WeeklyBasketState {
  basketId: string;
  flow: number | null;
  fundRank: number | null;
  fundHigh: boolean | null;
  quadrant: 'Q1' | 'Q2' | 'Q3' | 'Q4' | 'neutral' | 'unknown';
  macroScore: number | null;
  macroPass: boolean | null;
  macroCoverage: number;
  shortPct: number | null;
  liquid: boolean | null;
  turnover20: number | null;
  fundCoverage: number;
  selected: boolean;
  rank: number | null;
  strength: number | null;
}

interface WeeklyRow {
  date: string;
  entryDate: string | null;
  states: Map<string, WeeklyBasketState>;
  picks: string[];
}

async function main(): Promise<void> {
  const o = parseArgs();
  loadEnv();
  console.log('=== Sector Rotation Retrospective Evaluation (Phase 1 / EV-1) ===');
  console.log(`  cacheDir=${o.cacheDir}  lagMode=${o.lagMode}  from=${o.from}`);

  const cache = await loadCache(o);
  const {
    defs, constituents, metrics, topix, disclosures, shortSector, macro, sectorByCode, panel, etf,
  } = cache;

  // ---------- 営業日カレンダー ----------
  const tradeDates = topix.map((t) => t[0]);
  const dateIdx = new Map<string, number>(tradeDates.map((d, i) => [d, i]));
  const topixClose = new Map<string, number>(topix);

  // ---------- 週次評価日（各週の最終営業日） ----------
  const weekKey = (d: string) => {
    const dt = new Date(`${d}T00:00:00Z`);
    const day = dt.getUTCDay();
    // ISO週の月曜に寄せる
    const monday = new Date(dt);
    monday.setUTCDate(dt.getUTCDate() - ((day + 6) % 7));
    return monday.toISOString().slice(0, 10);
  };
  const lastOfWeek = new Map<string, string>();
  for (const d of tradeDates) if (d >= o.from) lastOfWeek.set(weekKey(d), d);
  const evalDates = [...lastOfWeek.values()].sort();
  console.log(`\n[calendar] trading days ${tradeDates[0]} .. ${tradeDates.at(-1)} (${tradeDates.length})`);
  console.log(`[calendar] weekly eval dates: ${evalDates.length} (${evalDates[0]} .. ${evalDates.at(-1)})`);

  // ---------- basket 系列 ----------
  const basketIds = defs.map((d) => d.basket_id);
  const nameById = new Map(defs.map((d) => [d.basket_id, d.display_name]));
  const benchById = new Map(defs.map((d) => [d.basket_id, d.benchmark_code]));
  const indexByBasket = new Map<string, Map<string, number>>();
  const etfCloseByBasket = new Map<string, Map<string, number>>();
  for (const id of basketIds) {
    indexByBasket.set(id, new Map());
    etfCloseByBasket.set(id, new Map());
  }
  for (const m of metrics) {
    if (m.index_level != null) indexByBasket.get(m.basket_id)?.set(m.as_of_date, m.index_level);
    if (m.etf_close != null) etfCloseByBasket.get(m.basket_id)?.set(m.as_of_date, m.etf_close);
  }
  // ETF は J-Quants 実データを主系列にする（売買代金も同時に得られるため）
  const etfAdjByCode = new Map<string, Map<string, number>>();
  const etfTurnoverByCode = new Map<string, Map<string, number>>();
  for (const [code, series] of Object.entries(etf)) {
    const a = new Map<string, number>();
    const v = new Map<string, number>();
    for (const [d, ac, va] of series) { a.set(d, ac); v.set(d, va); }
    etfAdjByCode.set(code, a);
    etfTurnoverByCode.set(code, v);
  }

  // ---------- 構成銘柄・ウエート素材 ----------
  const consByBasket = new Map<string, ConstituentRow[]>();
  for (const c of constituents) {
    let l = consByBasket.get(c.basket_id);
    if (!l) { l = []; consByBasket.set(c.basket_id, l); }
    l.push(c);
  }
  const histories = buildHistories(disclosures);

  // 月末スナップショット日（昇順）と、そこから作る分割調整比 A(t)=adjClose/close
  const panelDates = Object.keys(panel).sort();
  const adjRatioAt = (code: string, d: string): number | null => {
    const px = panel[d]?.[code];
    return px ? px[1] / px[0] : null;
  };
  /**
   * 開示日 d0 の A(d0) を「d0 以降で最初の月末」の A で近似する。
   * 直前の月末と A が異なる＝その窓の中で分割が起きており、d0 が分割の前か後か判別できない。
   * その場合は近似が最大で分割比だけ時価総額を誤るため、当該 銘柄×月 を集計から落とす。
   */
  const panelIndexOnOrAfter = (d: string): number => {
    let lo = 0, hi = panelDates.length;
    while (lo < hi) { const mid = (lo + hi) >> 1; if (panelDates[mid] < d) lo = mid + 1; else hi = mid; }
    return lo;
  };

  // ---------- 業績軸（月次計算 → 週次へ前方フィル） ----------
  console.log('\n[fundamental] computing monthly weighted aggregates ...');
  interface FundMonth {
    opYoY: number | null; fg: number | null; roe: number | null;
    coverage: number; covOpYoY: number; covFg: number; covRoe: number;
    weights: Map<string, number>;
  }
  const fundByBasket = new Map<string, Map<string, FundMonth>>();
  let ambiguousSplitCells = 0;
  let totalCells = 0;
  for (const id of basketIds) fundByBasket.set(id, new Map());

  const evalPanelDates = panelDates.filter((d) => d >= addDays(o.from, -400));
  for (const T of evalPanelDates) {
    for (const id of basketIds) {
      const cons = consByBasket.get(id) ?? [];
      let wSum = 0;
      const contrib: { w: number; opYoY: number | null; fg: number | null; roe: number | null }[] = [];
      const weights = new Map<string, number>();
      for (const c of cons) {
        const px = panel[T]?.[c.local_code];
        if (!px) continue;
        const hist = histories.get(c.local_code);
        if (!hist) continue;
        const f = pitFundamentals(hist, T);
        if (f.shares == null || f.sharesDisclosedDate == null) continue;
        const aT = adjRatioAt(c.local_code, T);
        const pi = panelIndexOnOrAfter(f.sharesDisclosedDate);
        const d0Panel = pi < panelDates.length ? panelDates[pi] : null;
        const a0 = d0Panel ? adjRatioAt(c.local_code, d0Panel) : null;
        totalCells++;
        if (aT == null || a0 == null || a0 <= 0) continue;
        // 開示日をまたぐ窓で分割が起きていたら、A(d0) を確定できないので落とす
        const aPrev = pi > 0 ? adjRatioAt(c.local_code, panelDates[pi - 1]) : null;
        if (aPrev != null && Math.abs(aPrev / a0 - 1) > 0.001) { ambiguousSplitCells++; continue; }
        // mcap(T) = adjClose(T) × shares_fy / A(d0)   （分割調整の向きは 00094 / basket-valuation と同一）
        const mcap = px[1] * (f.shares / a0);
        if (!(mcap > 0)) continue;
        const w = c.weight_factor * mcap;
        if (!(w > 0)) continue;
        weights.set(c.local_code, w);
        wSum += w;
        contrib.push({ w, opYoY: f.opYoY, fg: f.forecastGrowth, roe: f.roe });
      }
      if (wSum <= 0) continue;
      for (const [k, v] of weights) weights.set(k, v / wSum);
      const wmean = (pick: (x: (typeof contrib)[number]) => number | null): { v: number | null; cov: number } => {
        let n = 0, dsum = 0;
        for (const c of contrib) { const v = pick(c); if (v == null || !Number.isFinite(v)) continue; n += c.w * v; dsum += c.w; }
        return dsum > 0 ? { v: n / dsum, cov: dsum / wSum } : { v: null, cov: 0 };
      };
      const a = wmean((x) => x.opYoY);
      const b = wmean((x) => x.fg);
      const c2 = wmean((x) => x.roe);
      fundByBasket.get(id)!.set(T, {
        opYoY: a.cov >= FUNDAMENTAL_MIN_COVERAGE ? a.v : null,
        fg: b.cov >= FUNDAMENTAL_MIN_COVERAGE ? b.v : null,
        roe: c2.cov >= FUNDAMENTAL_MIN_COVERAGE ? c2.v : null,
        coverage: Math.min(a.cov, b.cov, c2.cov),
        covOpYoY: a.cov,
        covFg: b.cov,
        covRoe: c2.cov,
        weights,
      });
    }
  }
  console.log(`[fundamental] months=${evalPanelDates.length}, stock-month cells=${totalCells}, split-ambiguous dropped=${ambiguousSplitCells}`);

  // 月次の横断ランク（14バスケット内）→ 業績スコア
  const fundScoreByBasket = new Map<string, Map<string, number>>();
  const fundCovByBasket = new Map<string, Map<string, number>>();
  for (const id of basketIds) { fundScoreByBasket.set(id, new Map()); fundCovByBasket.set(id, new Map()); }
  for (const T of evalPanelDates) {
    const rankOf = (pick: (f: FundMonth) => number | null): Map<string, number> => {
      const vals: { id: string; v: number }[] = [];
      for (const id of basketIds) {
        const f = fundByBasket.get(id)!.get(T);
        const v = f ? pick(f) : null;
        if (v != null && Number.isFinite(v)) vals.push({ id, v });
      }
      vals.sort((x, y) => x.v - y.v);
      const out = new Map<string, number>();
      vals.forEach((x, i) => out.set(x.id, vals.length > 1 ? i / (vals.length - 1) : 0.5));
      return out;
    };
    const r1 = rankOf((f) => f.opYoY);
    const r2 = rankOf((f) => f.fg);
    const r3 = rankOf((f) => f.roe);
    for (const id of basketIds) {
      const parts = [r1.get(id), r2.get(id), r3.get(id)].filter((x): x is number => x != null);
      if (parts.length === 0) continue;
      fundScoreByBasket.get(id)!.set(T, meanOf(parts));
      fundCovByBasket.get(id)!.set(T, fundByBasket.get(id)!.get(T)?.coverage ?? 0);
    }
  }
  /** 月次スコアを t 以前の直近月末から引く（前方フィル・look-aheadなし） */
  const fundAt = (id: string, t: string): { score: number | null; cov: number } => {
    const m = fundScoreByBasket.get(id)!;
    let best: string | null = null;
    for (const T of evalPanelDates) { if (T > t) break; if (m.has(T)) best = T; }
    return best ? { score: m.get(best)!, cov: fundCovByBasket.get(id)!.get(best) ?? 0 } : { score: null, cov: 0 };
  };

  // ---------- マクロゲート ----------
  console.log('[macro] building PIT availability series ...');
  const macroBySeries = new Map<string, MacroRow[]>();
  for (const r of macro) {
    if (r.value == null) continue;
    let l = macroBySeries.get(r.series_id);
    if (!l) { l = []; macroBySeries.set(r.series_id, l); }
    l.push(r);
  }
  // 系列ごとの推定公表ラグ（indicator_date 間隔の中央値から）
  const assumedLag = new Map<string, number>();
  for (const [sid, list] of macroBySeries) {
    list.sort((a, b) => (a.indicator_date < b.indicator_date ? -1 : 1));
    const gaps: number[] = [];
    for (let i = 1; i < list.length; i++) {
      const g = (new Date(list[i].indicator_date).getTime() - new Date(list[i - 1].indicator_date).getTime()) / 86400000;
      if (g > 0) gaps.push(g);
    }
    gaps.sort((a, b) => a - b);
    const med = gaps.length ? gaps[Math.floor(gaps.length / 2)] : 30;
    assumedLag.set(sid, med <= 4 ? 1 : med <= 10 ? 7 : 45);
  }
  const availDateOf = (r: MacroRow, sid: string): string => {
    const rel = r.released_at ? String(r.released_at).slice(0, 10) : null;
    const plausible =
      rel != null && rel >= r.indicator_date &&
      (new Date(rel).getTime() - new Date(r.indicator_date).getTime()) / 86400000 <= RELEASED_AT_PLAUSIBLE_MAX_DAYS;
    if (o.lagMode === 'released_at' && rel) return rel;
    if (o.lagMode === 'hybrid' && plausible && rel) return rel;
    return addDays(r.indicator_date, assumedLag.get(sid) ?? 45);
  };
  // 系列ごとに「営業日 d 時点で参照できる最新値」を作る
  const pitSeries = new Map<string, Map<string, number>>();
  const seriesFirstAvail = new Map<string, string>();
  for (const [sid, list] of macroBySeries) {
    const events = list
      .map((r) => ({ avail: availDateOf(r, sid), v: r.value as number }))
      .sort((a, b) => (a.avail < b.avail ? -1 : 1));
    const out = new Map<string, number>();
    let k = 0;
    let cur: number | null = null;
    for (const d of tradeDates) {
      while (k < events.length && events[k].avail <= d) { cur = events[k].v; k++; }
      if (cur != null) {
        if (!seriesFirstAvail.has(sid)) seriesFirstAvail.set(sid, d);
        out.set(d, cur);
      }
    }
    pitSeries.set(sid, out);
  }
  // 63営業日変化 → z スコア（トレーリング5年・PIT）
  const chgSeries = new Map<string, Map<string, number>>();
  for (const [sid, ser] of pitSeries) {
    const drv = Object.values(MACRO_DRIVERS).flat().find((d) => d.seriesId === sid)!;
    const out = new Map<string, number>();
    for (let i = MACRO_WINDOW; i < tradeDates.length; i++) {
      const d = tradeDates[i];
      const p = tradeDates[i - MACRO_WINDOW];
      const a = ser.get(d);
      const b = ser.get(p);
      if (a == null || b == null) continue;
      if (drv.kind === 'rate') out.set(d, a - b);
      else if (b > 0 && a > 0) out.set(d, Math.log(a / b));
    }
    chgSeries.set(sid, out);
  }
  const macroZAt = (sid: string, t: string): number | null => {
    const ser = chgSeries.get(sid);
    if (!ser) return null;
    const cur = ser.get(t);
    if (cur == null) return null;
    const idx = dateIdx.get(t)!;
    const lo = Math.max(0, idx - MACRO_Z_WINDOW);
    const hist: number[] = [];
    for (let i = lo; i <= idx; i++) { const v = ser.get(tradeDates[i]); if (v != null) hist.push(v); }
    if (hist.length < MACRO_Z_MIN_OBS) return null;
    const sd = stdevOf(hist);
    return sd > 0 ? (cur - meanOf(hist)) / sd : 0;
  };
  const macroScoreAt = (basketId: string, t: string): { score: number | null; coverage: number } => {
    const drivers = MACRO_DRIVERS[basketId] ?? [];
    const zs: number[] = [];
    for (const d of drivers) {
      const z = macroZAt(d.seriesId, t);
      if (z != null) zs.push(z * d.direction);
    }
    if (zs.length === 0) return { score: null, coverage: 0 };
    return { score: normalCdf(meanOf(zs)) * 100, coverage: zs.length / drivers.length };
  };

  // ---------- 空売り過熱（sector33 ウエート加重合成） ----------
  const ssByDate = new Map<string, Map<string, ShortSectorRow>>();
  for (const r of shortSector) {
    let m = ssByDate.get(r.as_of_date);
    if (!m) { m = new Map(); ssByDate.set(r.as_of_date, m); }
    m.set(r.sector33_code, r);
  }
  /** basket の sector33 ウエート（月末の時価総額ウエートから合成） */
  const sectorWeightsAt = (id: string, t: string): Map<string, number> => {
    let bestT: string | null = null;
    for (const T of evalPanelDates) { if (T > t) break; if (fundByBasket.get(id)!.has(T)) bestT = T; }
    const out = new Map<string, number>();
    if (!bestT) return out;
    const w = fundByBasket.get(id)!.get(bestT)!.weights;
    for (const [code, ww] of w) {
      const s = sectorByCode[code];
      if (!s) continue;
      out.set(s, (out.get(s) ?? 0) + ww);
    }
    return out;
  };
  const shortRatioSeries = new Map<string, Map<string, number>>();
  for (const id of basketIds) shortRatioSeries.set(id, new Map());
  const ssDates = [...ssByDate.keys()].sort().filter((d) => d >= '2018-01-01');
  for (const id of basketIds) {
    let cachedT: string | null = null;
    let sw = new Map<string, number>();
    for (const d of ssDates) {
      const monthKey = d.slice(0, 7);
      if (cachedT !== monthKey) { sw = sectorWeightsAt(id, d); cachedT = monthKey; }
      if (sw.size === 0) continue;
      const day = ssByDate.get(d)!;
      let numr = 0, den = 0, complete = true;
      for (const [sector, w] of sw) {
        if (!(w > 0)) continue;
        const row = day.get(sector);
        if (!row || row.selling_ex_short_value == null || row.short_with_restrictions_value == null || row.short_without_restrictions_value == null) { complete = false; break; }
        const shortValue = row.short_with_restrictions_value + row.short_without_restrictions_value;
        numr += w * shortValue;
        den += w * (row.selling_ex_short_value + shortValue);
      }
      if (!complete || !(den > 0)) continue;
      shortRatioSeries.get(id)!.set(d, (numr / den) * 100);
    }
  }
  const shortPctAt = (id: string, t: string): number | null => {
    const ser = shortRatioSeries.get(id)!;
    const hist: number[] = [];
    let cur: number | null = null;
    for (const d of ssDates) {
      if (d > t) break;
      const v = ser.get(d);
      if (v == null) continue;
      hist.push(v);
      cur = v;
    }
    if (cur == null || hist.length < 250) return null;
    return percentileRank(hist, cur);
  };

  // ---------- 週次の状態表を作る ----------
  console.log('[signal] building weekly states ...');
  const relReturn = (id: string, t: string): number | null => {
    const idx = dateIdx.get(t);
    if (idx == null || idx < FLOW_WINDOW) return null;
    const p = tradeDates[idx - FLOW_WINDOW];
    const lv = indexByBasket.get(id)!;
    const a = lv.get(t), b = lv.get(p);
    const ta = topixClose.get(t), tb = topixClose.get(p);
    if (a == null || b == null || ta == null || tb == null || b <= 0 || tb <= 0) return null;
    return a / b - (ta / tb);
  };
  const turnover20At = (id: string, t: string): number | null => {
    const code = benchById.get(id)!;
    const tv = etfTurnoverByCode.get(code);
    if (!tv) return null;
    const idx = dateIdx.get(t);
    if (idx == null) return null;
    const vals: number[] = [];
    for (let i = idx; i >= 0 && vals.length < LIQUIDITY_WINDOW; i--) {
      const v = tv.get(tradeDates[i]);
      if (v != null) vals.push(v);
    }
    return vals.length >= LIQUIDITY_WINDOW / 2 ? meanOf(vals) : null;
  };

  const weekly: WeeklyRow[] = [];
  for (const t of evalDates) {
    const idx = dateIdx.get(t)!;
    const entryDate = idx + 1 < tradeDates.length ? tradeDates[idx + 1] : null;
    const states = new Map<string, WeeklyBasketState>();
    // 業績軸の上位半分は「その週に業績スコアが計算できたバスケット」内での相対
    const scored: { id: string; s: number }[] = [];
    for (const id of basketIds) {
      const f = fundAt(id, t);
      if (f.score != null) scored.push({ id, s: f.score });
    }
    const median = scored.length ? [...scored].sort((a, b) => a.s - b.s)[Math.floor(scored.length / 2)].s : null;
    for (const id of basketIds) {
      const flow = relReturn(id, t);
      const f = fundAt(id, t);
      const fundHigh = f.score != null && median != null ? f.score >= median : null;
      let quadrant: WeeklyBasketState['quadrant'] = 'unknown';
      if (flow != null && fundHigh != null) {
        if (Math.abs(flow) <= NEUTRAL_BAND) quadrant = 'neutral';
        else if (fundHigh && flow > 0) quadrant = 'Q1';
        else if (fundHigh && flow < 0) quadrant = 'Q2';
        else if (!fundHigh && flow > 0) quadrant = 'Q3';
        else quadrant = 'Q4';
      }
      const ms = macroScoreAt(id, t);
      const turnover20 = turnover20At(id, t);
      const shortPct = shortPctAt(id, t);
      states.set(id, {
        basketId: id,
        flow,
        fundRank: f.score,
        fundHigh,
        quadrant,
        macroScore: ms.score,
        macroPass: ms.score != null ? ms.score >= MACRO_GATE_PASS : null,
        macroCoverage: ms.coverage,
        shortPct,
        liquid: turnover20 == null ? null : turnover20 >= LIQUIDITY_MIN_TURNOVER,
        turnover20,
        fundCoverage: f.cov,
        selected: false,
        rank: null,
        strength: null,
      });
    }
    // ---- 強さスコアは象限①②の全バスケットに付ける（ゲートは絞り込みにのみ使う。
    //      アブレーションでゲートを外したときに順位付けが変わらないようにするため）
    for (const [, s] of states) {
      if (s.quadrant !== 'Q1' && s.quadrant !== 'Q2') continue;
      if (s.fundRank == null || s.flow == null) continue;
      // 強さ: 業績ランク（0-1）を主軸に、①は資金流入の勢いを加点、②は空売り過熱を反転加点
      // （計画書§5の「象限の強さ→マクロスコア→割安ラベル」のうち、割安ラベルは
      //  analytics.basket_score_daily の履歴が本番に存在しないため順位付けから外している）
      let strength = s.fundRank;
      if (s.quadrant === 'Q1') strength += Math.min(0.3, Math.max(0, s.flow) * 2);
      else strength += (s.shortPct ?? 0.5) * 0.3;
      s.strength = strength;
    }
    // ---- 選定: 象限①② × マクロゲート × 流動性 → 強さ順に上位4本
    const eligible: { id: string; strength: number }[] = [];
    for (const [id, s] of states) {
      if (s.strength == null) continue;
      if (s.macroPass !== true) continue;
      if (s.liquid !== true) continue;
      eligible.push({ id, strength: s.strength });
    }
    eligible.sort((a, b) => (b.strength - a.strength) || ((states.get(b.id)!.macroScore ?? 0) - (states.get(a.id)!.macroScore ?? 0)));
    const picks = eligible.slice(0, MAX_PICKS).map((e) => e.id);
    picks.forEach((id, i) => { states.get(id)!.selected = true; states.get(id)!.rank = i + 1; });
    weekly.push({ date: t, entryDate, states, picks });
  }

  // ============================================================
  // レポート
  // ============================================================
  const report: Record<string, unknown> = { generatedAt: new Date().toISOString(), options: o };

  console.log('\n================ 1. データ可用性 ================');
  const availRows: Record<string, unknown>[] = [];
  for (const id of basketIds) {
    const nFlow = weekly.filter((w) => w.states.get(id)!.flow != null).length;
    const nFund = weekly.filter((w) => w.states.get(id)!.fundRank != null).length;
    const nMacro = weekly.filter((w) => w.states.get(id)!.macroScore != null).length;
    const nLiq = weekly.filter((w) => w.states.get(id)!.liquid != null).length;
    const nShort = weekly.filter((w) => w.states.get(id)!.shortPct != null).length;
    const firstFund = weekly.find((w) => w.states.get(id)!.fundRank != null)?.date ?? '-';
    const firstMacro = weekly.find((w) => w.states.get(id)!.macroScore != null)?.date ?? '-';
    const firstLiq = weekly.find((w) => w.states.get(id)!.liquid != null)?.date ?? '-';
    const avgCov = meanOf(weekly.map((w) => w.states.get(id)!.fundCoverage).filter((x) => x > 0));
    // 「利用可能か」と「ゲートを通ったか」は別物。両方出す（creditflow軸が黙って死んだ教訓）
    const nLiqPass = weekly.filter((w) => w.states.get(id)!.liquid === true).length;
    const nMacroPass = weekly.filter((w) => w.states.get(id)!.macroPass === true).length;
    const med = (xs: number[]): number | null => (xs.length ? [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)] : null);
    const turnovers = weekly.map((w) => w.states.get(id)!.turnover20).filter((x): x is number => x != null);
    const medTurnover = med(turnovers);
    // 流動性は期間中に大きく変わっている。直近1年と全期間を分けて出す
    // （全期間の中央値だけ見ると「今も買えない」と誤読しかねない）
    const recent = weekly.filter((w) => w.date >= '2025-08-01');
    const medRecent = med(recent.map((w) => w.states.get(id)!.turnover20).filter((x): x is number => x != null));
    const liqPassRecent = recent.length ? recent.filter((w) => w.states.get(id)!.liquid === true).length / recent.length : 0;
    const covsRaw = evalPanelDates.map((T) => fundByBasket.get(id)!.get(T)).filter((f): f is FundMonth => !!f);
    const row = {
      basket: id, name: nameById.get(id), weeks: weekly.length,
      flowPct: nFlow / weekly.length, fundPct: nFund / weekly.length, macroPct: nMacro / weekly.length,
      liqAvailPct: nLiq / weekly.length, liqPassPct: nLiqPass / weekly.length,
      macroPassPct: nMacroPass / weekly.length,
      shortPct: nShort / weekly.length,
      firstFund, firstMacro, firstLiq, avgFundCoverage: avgCov,
      medianTurnover20: medTurnover,
      medianTurnover20Recent: medRecent,
      liqPassPctRecent: liqPassRecent,
      covOpYoY: meanOf(covsRaw.map((f) => f.covOpYoY)),
      covFg: meanOf(covsRaw.map((f) => f.covFg)),
      covRoe: meanOf(covsRaw.map((f) => f.covRoe)),
      drivers: (MACRO_DRIVERS[id] ?? []).map((d) => `${d.seriesId}${d.direction > 0 ? '+' : '-'}`).join(' '),
    };
    availRows.push(row);
    console.log(
      `${id.padEnd(28)} flow=${pct(row.flowPct, 0).padStart(5)} fund=${pct(row.fundPct, 0).padStart(5)} ` +
      `macroPass=${pct(row.macroPassPct, 0).padStart(5)} 流動性pass 全期間=${pct(row.liqPassPct, 0).padStart(5)}/直近1年=${pct(liqPassRecent, 0).padStart(5)} ` +
      `売買代金中央値 全期間=${(medTurnover == null ? 'n/a' : (medTurnover / 1e6).toFixed(0) + '百万').padStart(8)}/直近=${(medRecent == null ? 'n/a' : (medRecent / 1e6).toFixed(0) + '百万').padStart(8)} ` +
      `| 業績カバレッジ opYoY=${pct(row.covOpYoY, 0)} 予想=${pct(row.covFg, 0)} ROE=${pct(row.covRoe, 0)}`
    );
  }
  report.availability = availRows;

  console.log('\n  マクロ系列ごとの初回利用可能日（PIT・lagMode=' + o.lagMode + '）:');
  const seriesAvail: Record<string, unknown>[] = [];
  for (const sid of [...new Set(Object.values(MACRO_DRIVERS).flat().map((d) => d.seriesId))].sort()) {
    const firstZ = tradeDates.find((d) => d >= o.from && macroZAt(sid, d) != null) ?? '-';
    const r = { seriesId: sid, firstValue: seriesFirstAvail.get(sid) ?? '-', firstZScore: firstZ, assumedLagDays: assumedLag.get(sid) ?? null, n: macroBySeries.get(sid)?.length ?? 0 };
    seriesAvail.push(r);
    console.log(`    ${sid.padEnd(20)} n=${String(r.n).padStart(5)} value从=${r.firstValue} zスコア从=${firstZ} 推定ラグ=${r.assumedLagDays}d`);
  }
  report.macroSeriesAvailability = seriesAvail;

  // ---------- 2. 象限別フォワードリターン ----------
  console.log('\n================ 2. 象限別フォワードリターン（模擬指数・対TOPIX超過） ================');
  interface Obs { date: string; id: string; quadrant: string; macroPass: boolean | null; fwd: Record<number, number | null>; }
  const obs: Obs[] = [];
  for (const w of weekly) {
    const idx = dateIdx.get(w.date)!;
    for (const id of basketIds) {
      const s = w.states.get(id)!;
      if (s.quadrant === 'unknown') continue;
      const lv = indexByBasket.get(id)!;
      const base = lv.get(w.date);
      const tb = topixClose.get(w.date);
      const fwd: Record<number, number | null> = {};
      for (const h of HORIZONS) {
        const fi = idx + h;
        const fd = fi < tradeDates.length ? tradeDates[fi] : null;
        const fv = fd ? lv.get(fd) : undefined;
        const tv = fd ? topixClose.get(fd) : undefined;
        fwd[h] = base != null && fv != null && tb != null && tv != null && base > 0 && tb > 0 ? fv / base - tv / tb : null;
      }
      obs.push({ date: w.date, id, quadrant: s.quadrant, macroPass: s.macroPass, fwd });
    }
  }
  const quadResults: Record<string, unknown>[] = [];
  for (const q of ['Q1', 'Q2', 'Q3', 'Q4', 'neutral']) {
    for (const gate of ['all', 'gated', 'ungated'] as const) {
      const sel = obs.filter((x) => x.quadrant === q && (gate === 'all' || (gate === 'gated' ? x.macroPass === true : x.macroPass === false)));
      const line: Record<string, unknown> = { quadrant: q, gate, n: sel.length };
      const parts: string[] = [];
      for (const h of HORIZONS) {
        const vals = sel.map((x) => x.fwd[h]).filter((v): v is number => v != null);
        if (vals.length < 10) { parts.push(`h${h}: n=${vals.length} (too few)`); line[`h${h}`] = null; continue; }
        const lag = overlapLag(h, PERIOD_BUSINESS_DAYS);
        const t = hacTStat(vals, lag);
        line[`h${h}`] = { n: vals.length, mean: meanOf(vals), tHac: t, p: twoSidedP(t) };
        parts.push(`h${h}: n=${String(vals.length).padStart(4)} 平均=${(meanOf(vals) * 100).toFixed(2)}% t=${fmt(t)}`);
      }
      quadResults.push(line);
      console.log(`  ${q.padEnd(8)} ${gate.padEnd(8)} n=${String(sel.length).padStart(4)} | ${parts.join('  ')}`);
    }
  }
  report.quadrantForward = quadResults;
  console.log('  ※ 重複窓のためHAC(Newey-West)補正済み。バスケット間の同時点相関は未補正なので t は楽観側。');

  // ---------- 3. ポートフォリオ・シミュレーション ----------
  console.log('\n================ 3. ETF4本選定ルールのシミュレーション ================');

  /** 週次リターン列を作る。priceOf は (basketId, date) -> 価格 */
  function simulate(
    pickOf: (w: WeeklyRow) => string[],
    priceOf: (id: string, d: string) => number | null,
    costBp: number,
    range?: { from?: string; to?: string }
  ): { rets: number[]; bench: number[]; dates: string[]; invested: number; turnover: number[]; skipped: number } {
    const rets: number[] = [];
    const bench: number[] = [];
    const dates: string[] = [];
    const turnover: number[] = [];
    let invested = 0;
    let skipped = 0;
    let prev: string[] = [];
    for (let i = 0; i < weekly.length - 1; i++) {
      const w = weekly[i];
      const wNext = weekly[i + 1];
      const entry = w.entryDate;
      const exit = wNext.entryDate;
      if (!entry || !exit) continue;
      if (range?.from && entry < range.from) continue;
      if (range?.to && entry > range.to) continue;
      const raw = pickOf(w);
      // 建てられない銘柄（価格欠損）は現金扱いにせずポートから外す
      const usable = raw.filter((id) => priceOf(id, entry) != null && priceOf(id, exit) != null);
      skipped += raw.length - usable.length;
      let r = 0;
      if (usable.length > 0) {
        for (const id of usable) {
          const p0 = priceOf(id, entry)!;
          const p1 = priceOf(id, exit)!;
          r += (p1 / p0 - 1) / usable.length;
        }
        invested++;
      }
      // コスト: Σ|Δw| × 片道bp
      const wPrev = new Map<string, number>(prev.map((id) => [id, 1 / prev.length]));
      const wCur = new Map<string, number>(usable.map((id) => [id, 1 / usable.length]));
      let l1 = 0;
      for (const id of new Set([...wPrev.keys(), ...wCur.keys()])) l1 += Math.abs((wCur.get(id) ?? 0) - (wPrev.get(id) ?? 0));
      turnover.push(l1 / 2);
      r -= l1 * (costBp / 10000);
      prev = usable;
      const tb = topixClose.get(entry);
      const te = topixClose.get(exit);
      if (tb == null || te == null || tb <= 0) continue;
      rets.push(r);
      bench.push(te / tb - 1);
      dates.push(entry);
    }
    return { rets, bench, dates, invested, turnover, skipped };
  }

  const etfPrice = (id: string, d: string): number | null => {
    const code = benchById.get(id)!;
    return etfAdjByCode.get(code)?.get(d) ?? null;
  };
  const idxPrice = (id: string, d: string): number | null => indexByBasket.get(id)!.get(d) ?? null;

  // ベンチマークその2: 14バスケットのETFを常時等ウエートで持つ「ユニバース」。
  // 業種ETF群そのものが TOPIX を上回るため、TOPIX比の超過には「銘柄選択の巧拙」と
  // 「ユニバースの傾き」が混ざる。選択の巧拙を見るにはこちらとの比較が要る。
  const simUniverse = simulate(() => basketIds, etfPrice, 0);
  const univByDate = new Map(simUniverse.dates.map((d, i) => [d, simUniverse.rets[i]]));

  function summarize(label: string, sim: ReturnType<typeof simulate>): Record<string, unknown> {
    const excess = sim.rets.map((r, i) => r - sim.bench[i]);
    const univ = sim.dates.map((d) => univByDate.get(d) ?? null);
    const excessUniv: number[] = [];
    const univRets: number[] = [];
    sim.rets.forEach((r, i) => { const u = univ[i]; if (u != null) { excessUniv.push(r - u); univRets.push(u); } });
    const t = hacTStat(excess, 1);
    const tU = hacTStat(excessUniv, 1);
    const annStrat = annualize(sim.rets, WEEKS_PER_YEAR);
    const annBench = annualize(sim.bench, WEEKS_PER_YEAR);
    const annUniv = annualize(univRets, WEEKS_PER_YEAR);
    const out = {
      label,
      weeks: sim.rets.length,
      annReturn: annStrat,
      annBench,
      annUniverse: annUniv,
      annExcess: annStrat - annBench,
      annExcessVsUniverse: annStrat - annUniv,
      meanWeeklyExcess: meanOf(excess),
      tHac: t,
      p: twoSidedP(t),
      tHacVsUniverse: tU,
      pVsUniverse: twoSidedP(tU),
      ir: informationRatio(excess, WEEKS_PER_YEAR),
      irVsUniverse: informationRatio(excessUniv, WEEKS_PER_YEAR),
      maxDD: maxDrawdown(sim.rets),
      maxDDBench: maxDrawdown(sim.bench),
      investedRatio: sim.invested / Math.max(1, sim.rets.length),
      avgTurnover: meanOf(sim.turnover),
      annTurnover: meanOf(sim.turnover) * WEEKS_PER_YEAR,
      skippedPicks: sim.skipped,
    };
    console.log(
      `  ${label.padEnd(32)} n=${String(out.weeks).padStart(3)}w 年率=${pct(out.annReturn).padStart(7)} ` +
      `対TOPIX=${pct(out.annExcess).padStart(7)}(t${fmt(out.tHac).padStart(6)}) ` +
      `対ユニバース=${pct(out.annExcessVsUniverse).padStart(7)}(t${fmt(out.tHacVsUniverse).padStart(6)}) ` +
      `IR=${fmt(out.ir).padStart(5)} maxDD=${pct(out.maxDD).padStart(6)} ` +
      `稼働=${pct(out.investedRatio, 0).padStart(5)} 回転=${fmt(out.annTurnover, 1)}回/年`
    );
    return out;
  }
  console.log(`  [ベンチ] TOPIX 年率=${pct(annualize(simUniverse.bench, WEEKS_PER_YEAR))} / ` +
    `14バスケットETF等ウエート 年率=${pct(annualize(simUniverse.rets, WEEKS_PER_YEAR))}`);

  const mainResults: Record<string, unknown>[] = [];
  console.log('\n  --- 実ETF（adj_close）ベース・コスト感応度 ---');
  for (const bp of COST_BPS) {
    mainResults.push(summarize(`ETF 4本選定 (片道${bp}bp)`, simulate((w) => w.picks, etfPrice, bp)));
  }
  console.log('\n  --- 模擬指数ベース（参考。ETF乖離・流動性の影響を除いた上限値） ---');
  for (const bp of COST_BPS) {
    mainResults.push(summarize(`模擬指数 4本選定 (片道${bp}bp)`, simulate((w) => w.picks, idxPrice, bp)));
  }
  report.mainSimulation = mainResults;

  // ---------- 4. アブレーション ----------
  console.log('\n================ 4. アブレーション（すべて片道10bp・実ETF） ================');
  const ABL_BP = 10;
  const ablations: Record<string, unknown>[] = [];
  const topRank = (w: WeeklyRow, filter: (s: WeeklyBasketState) => boolean, key: (s: WeeklyBasketState) => number | null): string[] => {
    const c: { id: string; v: number }[] = [];
    for (const [id, s] of w.states) {
      if (!filter(s)) continue;
      const v = key(s);
      if (v == null) continue;
      c.push({ id, v });
    }
    c.sort((a, b) => b.v - a.v);
    return c.slice(0, MAX_PICKS).map((x) => x.id);
  };

  ablations.push(summarize('(base) 本ルール', simulate((w) => w.picks, etfPrice, ABL_BP)));
  ablations.push(summarize('(a) マクロゲートなし', simulate(
    (w) => topRank(w, (s) => (s.quadrant === 'Q1' || s.quadrant === 'Q2') && s.liquid === true, (s) => s.fundRank),
    etfPrice, ABL_BP)));
  ablations.push(summarize('(b) 業績軸のみ（上位4）', simulate(
    (w) => topRank(w, (s) => s.liquid === true, (s) => s.fundRank), etfPrice, ABL_BP)));
  ablations.push(summarize('(c) フロー軸のみ（上位4=順張り）', simulate(
    (w) => topRank(w, (s) => s.liquid === true, (s) => s.flow), etfPrice, ABL_BP)));
  ablations.push(summarize('(c2) フロー軸のみ（下位4=逆張り）', simulate(
    (w) => topRank(w, (s) => s.liquid === true, (s) => (s.flow == null ? null : -s.flow)), etfPrice, ABL_BP)));
  ablations.push(summarize('(d) 全14バスケット等ウエート常時', simulate(() => basketIds, etfPrice, ABL_BP)));
  ablations.push(summarize('(d2) 流動性通過ETFのみ等ウエート常時', simulate(
    (w) => basketIds.filter((id) => w.states.get(id)!.liquid === true), etfPrice, ABL_BP)));
  console.log(`  (e) TOPIX買い持ち                  n=${simUniverse.bench.length}w 年率=${pct(annualize(simUniverse.bench, WEEKS_PER_YEAR))} maxDD=${pct(maxDrawdown(simUniverse.bench))}（定義上 対TOPIX超過=0）`);
  // 象限①のみ / ②のみ
  ablations.push(summarize('(f) 象限①のみ（ゲート有）', simulate(
    (w) => topRank(w, (s) => s.quadrant === 'Q1' && s.macroPass === true && s.liquid === true, (s) => s.fundRank), etfPrice, ABL_BP)));
  ablations.push(summarize('(g) 象限②のみ（ゲート有）', simulate(
    (w) => topRank(w, (s) => s.quadrant === 'Q2' && s.macroPass === true && s.liquid === true, (s) => s.fundRank), etfPrice, ABL_BP)));
  // 交絡の切り分け: 「現金待機」と「流動性ゲート」が結果をどれだけ動かしているか
  ablations.push(summarize('(h) 本ルール・流動性ゲート無し', simulate(
    (w) => topRank(w, (s) => (s.quadrant === 'Q1' || s.quadrant === 'Q2') && s.macroPass === true, (s) => s.strength ?? s.fundRank),
    etfPrice, ABL_BP)));
  ablations.push(summarize('(i) マクロゲート反転（<50で通過）', simulate(
    (w) => topRank(w, (s) => (s.quadrant === 'Q1' || s.quadrant === 'Q2') && s.macroPass === false && s.liquid === true, (s) => s.fundRank),
    etfPrice, ABL_BP)));
  ablations.push(summarize('(j) 象限④（最弱のはず）のみ', simulate(
    (w) => topRank(w, (s) => s.quadrant === 'Q4' && s.liquid === true, (s) => s.fundRank), etfPrice, ABL_BP)));
  report.ablations = ablations;
  console.log('  ※ (b)(c)(c2)(d2)(f)(g)(i)(j) は流動性ゲート通過ETFのみが対象。実質の候補が数本しか');
  console.log('     無いため、これらの対TOPIX超過は「銘柄選択の巧拙」ではなく「流動性で残った');
  console.log('     少数ETF（銀行1615が主）の成績」を主に反映している点に注意。');

  // ---------- 4b. サブ期間 ----------
  // ETFの流動性は期間中に大きく改善している。「今から運用するなら」の判断材料として
  // 前半・後半に割って見る（後半だけ効くなら、前半の非流動性が結論を汚しているだけかもしれない）
  console.log('\n================ 4b. サブ期間（片道10bp・実ETF） ================');
  const subPeriods: Record<string, unknown>[] = [];
  for (const [label, range] of [
    ['前半 2019-07〜2022-12', { to: '2022-12-31' }],
    ['後半 2023-01〜2026-08', { from: '2023-01-01' }],
    ['直近2年 2024-08〜', { from: '2024-08-01' }],
  ] as const) {
    subPeriods.push(summarize(`本ルール ${label}`, simulate((w) => w.picks, etfPrice, ABL_BP, range)));
    subPeriods.push(summarize(`  参考 全14EW ${label}`, simulate(() => basketIds, etfPrice, ABL_BP, range)));
  }
  report.subPeriods = subPeriods;

  // ---------- 4c. 「選ぶ銘柄が悪い」のか「選ぶ時期が悪い」のか ----------
  // 選ばれた週のリターンを、同じバスケットの無条件平均リターンと比べる。
  // 下回るなら銘柄選択ではなくタイミング（＝ゲート）が損をさせている。
  console.log('\n================ 4c. 選択 vs タイミングの切り分け ================');
  const heldRet: number[] = [];
  const unconditionalByBasket = new Map<string, number[]>();
  const heldByBasket = new Map<string, number[]>();
  for (const id of basketIds) { unconditionalByBasket.set(id, []); heldByBasket.set(id, []); }
  for (let i = 0; i < weekly.length - 1; i++) {
    const entry = weekly[i].entryDate;
    const exit = weekly[i + 1].entryDate;
    if (!entry || !exit) continue;
    const picks = new Set(weekly[i].picks);
    for (const id of basketIds) {
      const p0 = etfPrice(id, entry);
      const p1 = etfPrice(id, exit);
      if (p0 == null || p1 == null || p0 <= 0) continue;
      const r = p1 / p0 - 1;
      unconditionalByBasket.get(id)!.push(r);
      if (picks.has(id)) { heldByBasket.get(id)!.push(r); heldRet.push(r); }
    }
  }
  console.log('  バスケット別: 選定週の平均週次リターン vs 無条件平均（年率換算）');
  const timingRows: Record<string, unknown>[] = [];
  for (const id of basketIds) {
    const held = heldByBasket.get(id)!;
    if (held.length < 10) continue;
    const uncond = unconditionalByBasket.get(id)!;
    const diff = meanOf(held) - meanOf(uncond);
    const row = { basket: id, heldWeeks: held.length, heldAnn: meanOf(held) * WEEKS_PER_YEAR, uncondAnn: meanOf(uncond) * WEEKS_PER_YEAR, diffAnn: diff * WEEKS_PER_YEAR, tHac: hacTStat(held.map((r) => r - meanOf(uncond)), 1) };
    timingRows.push(row);
    console.log(
      `    ${id.padEnd(28)} 保有${String(held.length).padStart(4)}週 選定時=${pct(row.heldAnn).padStart(8)} 無条件=${pct(row.uncondAnn).padStart(8)} ` +
      `差=${pct(row.diffAnn).padStart(8)} t=${fmt(row.tHac)}`
    );
  }
  const allUncond = basketIds.flatMap((id) => unconditionalByBasket.get(id)!);
  console.log(`  合計: 選定週 n=${heldRet.length} 平均(年率)=${pct(meanOf(heldRet) * WEEKS_PER_YEAR)} / 全バスケット無条件 平均(年率)=${pct(meanOf(allUncond) * WEEKS_PER_YEAR)}`);
  console.log(`        差=${pct((meanOf(heldRet) - meanOf(allUncond)) * WEEKS_PER_YEAR)} → 負なら「選んだ時期が悪い」`);
  report.timingVsSelection = { rows: timingRows, heldMeanAnn: meanOf(heldRet) * WEEKS_PER_YEAR, uncondMeanAnn: meanOf(allUncond) * WEEKS_PER_YEAR, n: heldRet.length };

  // ---------- 5. 選定の実態 ----------
  console.log('\n================ 5. 選定の実態 ================');
  const pickCount = new Map<string, number>();
  let cashWeeks = 0;
  const nPicksHist = new Map<number, number>();
  for (const w of weekly) {
    nPicksHist.set(w.picks.length, (nPicksHist.get(w.picks.length) ?? 0) + 1);
    if (w.picks.length === 0) cashWeeks++;
    for (const id of w.picks) pickCount.set(id, (pickCount.get(id) ?? 0) + 1);
  }
  console.log(`  週数=${weekly.length}  現金週=${cashWeeks} (${pct(cashWeeks / weekly.length, 0)})`);
  console.log(`  選定本数の分布: ${[...nPicksHist.entries()].sort((a, b) => a[0] - b[0]).map(([k, v]) => `${k}本:${v}週`).join(' ')}`);
  console.log('  バスケット別の選定回数:');
  for (const [id, n] of [...pickCount.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`    ${id.padEnd(28)} ${String(n).padStart(4)}週 (${pct(n / weekly.length, 0)})`);
  }
  const quadCount = new Map<string, number>();
  for (const w of weekly) for (const [, s] of w.states) quadCount.set(s.quadrant, (quadCount.get(s.quadrant) ?? 0) + 1);
  console.log(`  象限の出現数（basket×週）: ${[...quadCount.entries()].sort().map(([k, v]) => `${k}:${v}`).join(' ')}`);
  const gatePass = weekly.flatMap((w) => [...w.states.values()]).filter((s) => s.macroPass === true).length;
  const gateEval = weekly.flatMap((w) => [...w.states.values()]).filter((s) => s.macroPass != null).length;
  console.log(`  マクロゲート通過率: ${gatePass}/${gateEval} (${pct(gatePass / Math.max(1, gateEval), 0)})`);
  report.selection = {
    weeks: weekly.length, cashWeeks, picksHistogram: [...nPicksHist.entries()],
    pickCount: [...pickCount.entries()], quadrantCount: [...quadCount.entries()],
    macroGatePassRate: gatePass / Math.max(1, gateEval),
  };

  // ---------- 6. 軸そのものの予測力（スコア序列） ----------
  console.log('\n================ 6. 軸の単体予測力（週次断面 Spearman・対TOPIX超過60日） ================');
  const axisRows: Record<string, unknown>[] = [];
  for (const [axisName, get] of [
    ['業績ランク', (s: WeeklyBasketState) => s.fundRank],
    ['資金フロー60日', (s: WeeklyBasketState) => s.flow],
    ['マクロスコア', (s: WeeklyBasketState) => s.macroScore],
    ['空売り過熱pct', (s: WeeklyBasketState) => s.shortPct],
  ] as const) {
    for (const h of HORIZONS) {
      const rhos: number[] = [];
      for (const w of weekly) {
        const idx = dateIdx.get(w.date)!;
        const xs: number[] = [], ys: number[] = [];
        for (const id of basketIds) {
          const s = w.states.get(id)!;
          const v = get(s);
          if (v == null) continue;
          const lv = indexByBasket.get(id)!;
          const base = lv.get(w.date);
          const tb = topixClose.get(w.date);
          const fi = idx + h;
          const fd = fi < tradeDates.length ? tradeDates[fi] : null;
          const fv = fd ? lv.get(fd) : undefined;
          const tv = fd ? topixClose.get(fd) : undefined;
          if (base == null || fv == null || tb == null || tv == null || base <= 0 || tb <= 0) continue;
          xs.push(v);
          ys.push(fv / base - tv / tb);
        }
        if (xs.length >= 6) rhos.push(spearman(xs, ys));
      }
      if (rhos.length < 20) continue;
      const lag = overlapLag(h, PERIOD_BUSINESS_DAYS);
      const t = hacTStat(rhos, lag);
      axisRows.push({ axis: axisName, horizon: h, weeks: rhos.length, meanRho: meanOf(rhos), tHac: t, p: twoSidedP(t) });
      console.log(`  ${axisName.padEnd(16)} h=${String(h).padStart(3)}d 週数=${String(rhos.length).padStart(4)} 平均ρ=${fmt(meanOf(rhos), 3).padStart(7)} t(HAC)=${fmt(t).padStart(6)} p=${fmt(twoSidedP(t), 3)}`);
    }
  }
  report.axisPredictivePower = axisRows;

  // マクロゲートの符号がバスケット横断で一様かを見る（一様に負なら系統的な機序、
  // まだらならノイズ。macro_regime が逆指標化したのと同じ轍かどうかの判別）
  console.log('\n  マクロスコアの時系列予測力（バスケット別・スコア高→60日後の対TOPIX超過）:');
  const macroPerBasket: Record<string, unknown>[] = [];
  for (const id of basketIds) {
    const xs: number[] = [];
    const ys: number[] = [];
    for (const w of weekly) {
      const s = w.states.get(id)!;
      if (s.macroScore == null) continue;
      const idx = dateIdx.get(w.date)!;
      const lv = indexByBasket.get(id)!;
      const base = lv.get(w.date);
      const tb = topixClose.get(w.date);
      const fi = idx + 60;
      const fd = fi < tradeDates.length ? tradeDates[fi] : null;
      const fv = fd ? lv.get(fd) : undefined;
      const tv = fd ? topixClose.get(fd) : undefined;
      if (base == null || fv == null || tb == null || tv == null || base <= 0 || tb <= 0) continue;
      xs.push(s.macroScore);
      ys.push(fv / base - tv / tb);
    }
    if (xs.length < 50) continue;
    const reg = hacRegression(xs, ys, overlapLag(60, PERIOD_BUSINESS_DAYS));
    const rho = spearman(xs, ys);
    macroPerBasket.push({ basket: id, n: xs.length, spearman: rho, tHac: reg.tStat });
    console.log(`    ${id.padEnd(28)} n=${String(xs.length).padStart(4)} ρ=${fmt(rho, 3).padStart(7)} t(HAC)=${fmt(reg.tStat).padStart(6)} ${(MACRO_DRIVERS[id] ?? []).map((d) => d.seriesId + (d.direction > 0 ? '+' : '-')).join(' ')}`);
  }
  const negCount = macroPerBasket.filter((m) => (m.spearman as number) < 0).length;
  console.log(`    → 負の符号: ${negCount}/${macroPerBasket.length} バスケット`);
  report.macroPerBasket = macroPerBasket;

  if (o.json) {
    writeFileSync(o.json, JSON.stringify(report, null, 2));
    console.log(`\n[out] wrote ${o.json}`);
  }
  console.log('\n完了。');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
