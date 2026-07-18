/**
 * テーマバスケット日次集計の計算（純関数）
 *
 * @description
 * `analytics.basket_constituents` のウエート係数 + 当日 `analytics.stock_metrics` から
 * バスケットの当日ウエート・加重バリュエーション・模擬指数の連結を計算する。
 * scripts/cron/refresh-basket-metrics.ts が analytics.basket_metrics へ保存する。
 * 計画書: docs/PLANS-basket-valuation-2026-07.md §5/§6/§8
 *
 * 設計要点:
 * - 当日ウエート w_i(t) = weight_factor_i × market_cap_i(t) / Σ_j(weight_factor_j × market_cap_j(t))
 *   （market_cap が欠損/0以下の銘柄は分子分母から除外して再正規化）。
 * - 加重バリュエーション（実績PER/PBR/PSR/フォワードPER）は調和集計:
 *   weighted_X = Σ adj_mcap_i / Σ(adj_mcap_i / X_i)（adj_mcap_i = weight_factor_i × market_cap_i）。
 *   X_i が null または 0 以下の銘柄は分子分母から除外する（赤字銘柄はPER集計から除外する方式。
 *   00105 の列コメントが示す「赤字銘柄を分母に含める」教科書的方式とは異なるが、
 *   本実装は ISSUE-A の指示（per_i<=0 除外）に従う。将来のレビューで方式差異を確認すること）。
 * - 配当利回りは調和集計ではなく単純加重平均（0は有効値として許容、nullのみ除外）。
 * - 模擬指数は前営業日の basket_metrics 行がある場合のみ日次リターンで連結する
 *   （前行が無ければ null のまま。バックフィルスクリプトが過去分を埋める）。
 *   ただし前行が一度も存在せず、対象日がアンカー日と一致する場合のみ
 *   anchor_index_level を直接の初期値として採用する（模擬指数の基準化）。
 */

/** PostgREST の numeric（文字列/数値/null混在）を number|null に統一する */
export function toNumberOrNull(v: unknown): number | null {
  if (v == null) return null;
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

// ============================================================
// 構成銘柄・当日メトリクス
// ============================================================

/** analytics.basket_constituents の生行（valid_to is null の現行行のみ想定） */
export interface RawBasketConstituentRow {
  local_code: string;
  weight_factor: unknown;
  official_weight: unknown;
}

export interface BasketConstituent {
  local_code: string;
  weight_factor: number;
  official_weight: number | null;
}

export function parseBasketConstituentRow(raw: RawBasketConstituentRow): BasketConstituent {
  return {
    local_code: raw.local_code,
    weight_factor: toNumberOrNull(raw.weight_factor) ?? 0,
    official_weight: toNumberOrNull(raw.official_weight),
  };
}

/** analytics.stock_metrics から必要列のみ取り出した生行 */
export interface RawStockMetricRow {
  local_code: string;
  market_cap: unknown;
  per: unknown;
  pbr: unknown;
  psr: unknown;
  dividend_yield: unknown;
  close: unknown;
}

export interface StockMetricSnapshot {
  local_code: string;
  market_cap: number | null;
  per: number | null;
  pbr: number | null;
  psr: number | null;
  dividend_yield: number | null;
  close: number | null;
}

export function parseStockMetricRow(raw: RawStockMetricRow): StockMetricSnapshot {
  return {
    local_code: raw.local_code,
    market_cap: toNumberOrNull(raw.market_cap),
    per: toNumberOrNull(raw.per),
    pbr: toNumberOrNull(raw.pbr),
    psr: toNumberOrNull(raw.psr),
    dividend_yield: toNumberOrNull(raw.dividend_yield),
    close: toNumberOrNull(raw.close),
  };
}

// ============================================================
// 当日ウエート w_i(t) + coverage_pct
// ============================================================

export interface WeightResult {
  /** local_code -> w_i(t)。採用できた銘柄の合計は 1（100%）に正規化済み */
  weights: Map<string, number>;
  /** 当日メトリクス算出に採用できた構成銘柄のウエート合計%（0-100） */
  coveragePct: number;
}

/**
 * 当日ウエート w_i(t) を計算する。
 *
 * market_cap が欠損/0以下の銘柄は分子分母から除外し、採用できた銘柄のみで正規化する。
 * coverage_pct は「除外前の全構成」に対する採用比率であり、市場データに依存しない
 * official_weight（アンカー日時点の公式ウエート%）を分母に使う
 * （w_i(t) 自身は採用集合のみで常に合計100%になり、除外率の指標にならないため）。
 * official_weight が全銘柄でnull（未設定）の場合のみ、採用銘柄数の単純割合にフォールバックする。
 */
export function computeWeights(
  constituents: BasketConstituent[],
  metricsByCode: Map<string, StockMetricSnapshot>
): WeightResult {
  const contributions = new Map<string, number>();
  let denominator = 0;

  for (const c of constituents) {
    const marketCap = metricsByCode.get(c.local_code)?.market_cap;
    if (marketCap == null || !(marketCap > 0)) continue;
    const contribution = c.weight_factor * marketCap;
    if (!(contribution > 0)) continue;
    contributions.set(c.local_code, contribution);
    denominator += contribution;
  }

  const weights = new Map<string, number>();
  if (denominator > 0) {
    for (const [code, contribution] of contributions) {
      weights.set(code, contribution / denominator);
    }
  }

  return { weights, coveragePct: computeCoveragePct(constituents, weights) };
}

function computeCoveragePct(
  constituents: BasketConstituent[],
  included: Map<string, number>
): number {
  if (constituents.length === 0) return 0;

  const totalOfficialWeight = constituents.reduce(
    (sum, c) => sum + (c.official_weight ?? 0),
    0
  );
  if (totalOfficialWeight > 0) {
    const includedOfficialWeight = constituents
      .filter((c) => included.has(c.local_code))
      .reduce((sum, c) => sum + (c.official_weight ?? 0), 0);
    return (includedOfficialWeight / totalOfficialWeight) * 100;
  }

  // official_weight が使えない場合のフォールバック: 採用銘柄数の単純割合
  return (included.size / constituents.length) * 100;
}

// ============================================================
// 加重バリュエーション（調和集計 / 単純加重平均）
// ============================================================

export interface WeightedMetricEntry {
  localCode: string;
  weightFactor: number;
  marketCap: number | null;
  /** per / pbr / psr / forward_per / dividend_yield いずれかの当日値 */
  value: number | null;
}

/**
 * 調和加重集計: Σ adj_mcap_i / Σ (adj_mcap_i / value_i)
 *
 * value が null または 0 以下の銘柄、market_cap が欠損/0以下の銘柄は分子分母から除外する。
 * 採用銘柄が0件（全滅）の場合は null を返す。
 */
export function weightedHarmonicMean(entries: WeightedMetricEntry[]): number | null {
  let numerator = 0;
  let denominator = 0;

  for (const e of entries) {
    if (e.marketCap == null || !(e.marketCap > 0)) continue;
    if (e.value == null || !(e.value > 0)) continue;
    const adjMcap = e.weightFactor * e.marketCap;
    numerator += adjMcap;
    denominator += adjMcap / e.value;
  }

  if (numerator === 0 || denominator === 0) return null;
  return numerator / denominator;
}

/**
 * 単純加重平均: Σ(adj_mcap_i × value_i) / Σ adj_mcap_i
 *
 * value が null の銘柄は除外して再正規化する（0以下は配当利回り等で有効値のため許容）。
 * 採用銘柄が0件の場合は null を返す。
 */
export function weightedAverage(entries: WeightedMetricEntry[]): number | null {
  let numerator = 0;
  let denominator = 0;

  for (const e of entries) {
    if (e.marketCap == null || !(e.marketCap > 0)) continue;
    if (e.value == null) continue;
    const adjMcap = e.weightFactor * e.marketCap;
    numerator += adjMcap * e.value;
    denominator += adjMcap;
  }

  if (denominator === 0) return null;
  return numerator / denominator;
}

// ============================================================
// フォワードPER用の予想EPS選定
// ============================================================

export interface ForecastEpsSource {
  forecast_eps: number | null;
  next_forecast_eps: number | null;
}

/**
 * 会社予想EPSを選定する。
 *
 * NOTE: J-Quantsの期末開示は forecast_eps（今期予想）が null になり、
 * 来期計画は next_forecast_eps に入る既知の癖がある（[[stock-detail-forward-analysis]]）。
 * forecast_eps が null の場合のみ next_forecast_eps にフォールバックする。
 */
export function selectEffectiveForecastEps(source: ForecastEpsSource | undefined): number | null {
  if (!source) return null;
  return source.forecast_eps ?? source.next_forecast_eps ?? null;
}

// ============================================================
// 模擬指数の連結（index_level）
// ============================================================

export interface IndexReturnEntry {
  localCode: string;
  weightFactor: number;
  /** 前営業日の market_cap（w_i(t-1) の算出に使用） */
  prevMarketCap: number | null;
  /** 前営業日の adj_close */
  prevClose: number | null;
  /** 対象日の adj_close */
  currClose: number | null;
}

export interface IndexLevelResult {
  indexLevel: number | null;
  /** 診断用: Σ w_i(t-1) × r_i(t)（採用集合で再正規化済み） */
  indexReturn: number | null;
  includedCount: number;
}

/**
 * 模擬指数を1日分連結する: index_level(t) = index_level(t-1) × Σ w_i(t-1) × r_i(t)
 *
 * - prevIndexLevel が null（前行なし、または前行の index_level が未確定）の場合は null を返す
 *   （呼び出し側でアンカー日の特例初期化を別途判定すること）。
 * - w_i(t-1) は前営業日の market_cap から算出する（当日ウエートと同じ方式）。
 * - r_i(t) = currClose / prevClose。前日/当日いずれかの adj_close が欠損する銘柄は
 *   採用集合から除外し、残った銘柄の w_i(t-1) で再正規化してから加重平均する。
 */
export function computeIndexLevel(
  prevIndexLevel: number | null,
  entries: IndexReturnEntry[]
): IndexLevelResult {
  if (prevIndexLevel == null) {
    return { indexLevel: null, indexReturn: null, includedCount: 0 };
  }

  // 1) w_i(t-1)（前日 market_cap ベース、当日ウエートと同じロジック）
  const prevWeights = new Map<string, number>();
  let weightDenom = 0;
  for (const e of entries) {
    if (e.prevMarketCap == null || !(e.prevMarketCap > 0)) continue;
    const contribution = e.weightFactor * e.prevMarketCap;
    if (!(contribution > 0)) continue;
    prevWeights.set(e.localCode, contribution);
    weightDenom += contribution;
  }
  if (weightDenom === 0) {
    return { indexLevel: null, indexReturn: null, includedCount: 0 };
  }

  // 2) r_i(t) が計算可能な銘柄のみ採用し、採用集合内で w_i(t-1) を再正規化する
  let renormDenom = 0;
  const included: { weight: number; ret: number }[] = [];
  for (const e of entries) {
    const contribution = prevWeights.get(e.localCode);
    if (contribution == null) continue;
    if (e.prevClose == null || !(e.prevClose > 0) || e.currClose == null) continue;
    included.push({ weight: contribution, ret: e.currClose / e.prevClose });
    renormDenom += contribution;
  }
  if (renormDenom === 0) {
    return { indexLevel: null, indexReturn: null, includedCount: 0 };
  }

  const indexReturn = included.reduce(
    (sum, x) => sum + (x.weight / renormDenom) * x.ret,
    0
  );

  return {
    indexLevel: prevIndexLevel * indexReturn,
    indexReturn,
    includedCount: included.length,
  };
}

/** weighted_eps_level = index_level / weighted_per（どちらか null/0 なら null） */
export function computeWeightedEpsLevel(
  indexLevel: number | null,
  weightedPer: number | null
): number | null {
  if (indexLevel == null || weightedPer == null || weightedPer === 0) return null;
  return indexLevel / weightedPer;
}
