/**
 * 類似銘柄検索（ファクタープロファイル近傍）のベクトル計算（純関数）
 *
 * @description
 * analytics.stock_screen の13指標を決定論的に13次元ベクトルへ変換する。
 * scripts/cron/refresh-factor-vectors.ts が analytics.stock_factor_vectors へ保存し、
 * DB側の RPC analytics.similar_stocks が pgvector のコサイン距離で近傍探索する。
 *
 * ⚠️ DIMENSIONS の次元順序は
 *    supabase/migrations/00101_stock_factor_vectors.sql のコメント（embedding列）と
 *    厳密に一致させること（vector(13) の列位置がそのまま次元の意味になるため）。
 *
 * 変換パイプライン（次元＝列ごとに独立、順序厳守）:
 *   (a) 派生値計算（deriveRawDimensions）
 *   (b) 非NULL集合で1%/99%パーセンタイルにwinsorize（winsorize）
 *   (c) z-score化（非NULL集合のmean/std。std=0なら全て0）（zScore）
 *   (d) ±3にクランプ（clamp）
 *   (e) NULLは0（=平均）で補完（fillNullWithZero）
 *   (f) 重み乗算（applyWeight）
 * transformColumn() が (b)〜(f) を1本のパイプラインとして実行する。
 */

// ===== 次元定義 =====

export interface FactorDimension {
  /** 次元名（embedding列の並び順に対応） */
  key: string;
  /** 重み（乗算） */
  weight: number;
}

/** 13次元の定義（この配列の順序 = vector(13) の列位置）。順序を変更してはいけない。 */
export const DIMENSIONS: readonly FactorDimension[] = [
  { key: 'earnings_yield', weight: 1.0 },
  { key: 'log_pbr', weight: 1.0 },
  { key: 'dividend_yield', weight: 1.0 },
  { key: 'roe', weight: 1.0 },
  { key: 'log_mcap', weight: 1.0 },
  { key: 'value_pct', weight: 1.0 },
  { key: 'quality_pct', weight: 1.0 },
  { key: 'momentum_pct', weight: 1.0 },
  { key: 'dev_25', weight: 0.5 },
  { key: 'dev_200', weight: 0.75 },
  { key: 'rsi_14', weight: 0.5 },
  { key: 'atr_pct', weight: 1.0 },
  { key: 'vol_ratio_20', weight: 0.5 },
] as const;

export const VECTOR_LENGTH = DIMENSIONS.length; // 13

// ===== 入力行の型 =====

/** analytics.stock_screen から必要列のみを取り出した生行（PostgRESTは numeric を文字列で返し得る） */
export interface RawScreenRow {
  as_of_date: string;
  local_code: string;
  sector17_code: string | null;
  sector17_name: string | null;
  market_cap: unknown;
  per: unknown;
  pbr: unknown;
  dividend_yield: unknown;
  roe: unknown;
  value_pct: unknown;
  quality_pct: unknown;
  momentum_pct: unknown;
  dev_25: unknown;
  dev_200: unknown;
  rsi_14: unknown;
  atr_pct: unknown;
  vol_ratio_20: unknown;
}

/** Number() 変換済みの行 */
export interface ParsedScreenRow {
  as_of_date: string;
  local_code: string;
  sector17_code: string | null;
  sector17_name: string | null;
  market_cap: number | null;
  per: number | null;
  pbr: number | null;
  dividend_yield: number | null;
  roe: number | null;
  value_pct: number | null;
  quality_pct: number | null;
  momentum_pct: number | null;
  dev_25: number | null;
  dev_200: number | null;
  rsi_14: number | null;
  atr_pct: number | null;
  vol_ratio_20: number | null;
}

/** PostgRESTの numeric（文字列/数値/null混在）を number|null に統一する */
export function toNumberOrNull(v: unknown): number | null {
  if (v == null) return null;
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

/** stock_screen の生行を Number() 変換済みの型に正規化する */
export function parseScreenRow(raw: RawScreenRow): ParsedScreenRow {
  return {
    as_of_date: raw.as_of_date,
    local_code: raw.local_code,
    sector17_code: raw.sector17_code,
    sector17_name: raw.sector17_name,
    market_cap: toNumberOrNull(raw.market_cap),
    per: toNumberOrNull(raw.per),
    pbr: toNumberOrNull(raw.pbr),
    dividend_yield: toNumberOrNull(raw.dividend_yield),
    roe: toNumberOrNull(raw.roe),
    value_pct: toNumberOrNull(raw.value_pct),
    quality_pct: toNumberOrNull(raw.quality_pct),
    momentum_pct: toNumberOrNull(raw.momentum_pct),
    dev_25: toNumberOrNull(raw.dev_25),
    dev_200: toNumberOrNull(raw.dev_200),
    rsi_14: toNumberOrNull(raw.rsi_14),
    atr_pct: toNumberOrNull(raw.atr_pct),
    vol_ratio_20: toNumberOrNull(raw.vol_ratio_20),
  };
}

// ===== 除外判定 =====

/**
 * ベクトル表への収録可否。
 * market_cap が NULL、または per と pbr が両方 NULL の銘柄は除外する
 * （時価総額なしは比較の土台がなく、バリュエーション指標が全滅では意味のあるベクトルにならない）。
 */
export function shouldExcludeStock(row: Pick<ParsedScreenRow, 'market_cap' | 'per' | 'pbr'>): boolean {
  if (row.market_cap == null) return true;
  if (row.per == null && row.pbr == null) return true;
  return false;
}

// ===== (a) 派生値計算 =====

/**
 * 1銘柄ぶんの13次元「生の派生値」を DIMENSIONS の順序で計算する。
 * per/pbr は正の値のみ有効（0以下・NULLは対象次元をNULLにする）。
 */
export function deriveRawDimensions(
  row: Pick<
    ParsedScreenRow,
    | 'per'
    | 'pbr'
    | 'dividend_yield'
    | 'roe'
    | 'market_cap'
    | 'value_pct'
    | 'quality_pct'
    | 'momentum_pct'
    | 'dev_25'
    | 'dev_200'
    | 'rsi_14'
    | 'atr_pct'
    | 'vol_ratio_20'
  >
): (number | null)[] {
  const earningsYield = row.per != null && row.per > 0 ? 1 / row.per : null;
  const logPbr = row.pbr != null && row.pbr > 0 ? Math.log(row.pbr) : null;
  const logMcap = row.market_cap != null && row.market_cap > 0 ? Math.log(row.market_cap) : null;

  return [
    earningsYield,
    logPbr,
    row.dividend_yield,
    row.roe,
    logMcap,
    row.value_pct,
    row.quality_pct,
    row.momentum_pct,
    row.dev_25,
    row.dev_200,
    row.rsi_14,
    row.atr_pct,
    row.vol_ratio_20,
  ];
}

/** 13次元中、非NULLだった次元数（coverage列の値） */
export function countCoverage(rawDimensions: (number | null)[]): number {
  return rawDimensions.filter((v) => v != null).length;
}

// ===== (b) winsorize =====

/** 線形補間パーセンタイル（ソート済み配列が前提、type 7相当） */
export function percentile(sortedValues: number[], p: number): number {
  if (sortedValues.length === 0) return NaN;
  if (sortedValues.length === 1) return sortedValues[0];
  const idx = p * (sortedValues.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedValues[lo];
  const frac = idx - lo;
  return sortedValues[lo] + (sortedValues[hi] - sortedValues[lo]) * frac;
}

/** 非NULL集合の1%/99%パーセンタイルで両側クリップ（NULLはそのまま） */
export function winsorize(
  values: (number | null)[],
  lowerP = 0.01,
  upperP = 0.99
): (number | null)[] {
  const nonNull = values.filter((v): v is number => v != null).sort((a, b) => a - b);
  if (nonNull.length === 0) return values.slice();
  const lo = percentile(nonNull, lowerP);
  const hi = percentile(nonNull, upperP);
  return values.map((v) => (v == null ? null : Math.min(hi, Math.max(lo, v))));
}

// ===== (c) z-score =====

/** 非NULL集合の平均・母集団標準偏差でz-score化。std=0（全値同一/1件のみ等）なら非NULL値は全て0。 */
export function zScore(values: (number | null)[]): (number | null)[] {
  const nonNull = values.filter((v): v is number => v != null);
  if (nonNull.length === 0) return values.slice();
  const mean = nonNull.reduce((s, v) => s + v, 0) / nonNull.length;
  const variance = nonNull.reduce((s, v) => s + (v - mean) ** 2, 0) / nonNull.length;
  const std = Math.sqrt(variance);
  if (std === 0) {
    return values.map((v) => (v == null ? null : 0));
  }
  return values.map((v) => (v == null ? null : (v - mean) / std));
}

// ===== (d) クランプ =====

/** ±limit にクランプ（NULLはそのまま） */
export function clamp(values: (number | null)[], limit = 3): (number | null)[] {
  return values.map((v) => (v == null ? null : Math.max(-limit, Math.min(limit, v))));
}

// ===== (e) NULL補完 =====

/** NULLを fillValue（既定0 = z-score平均）で補完 */
export function fillNullWithZero(values: (number | null)[], fillValue = 0): number[] {
  return values.map((v) => (v == null ? fillValue : v));
}

// ===== (f) 重み =====

/** 各値に重みを乗算 */
export function applyWeight(values: number[], weight: number): number[] {
  return values.map((v) => v * weight);
}

// ===== パイプライン合成 =====

/** 1次元(列)ぶんの (b)〜(f) を通しで実行する */
export function transformColumn(rawColumn: (number | null)[], weight: number): number[] {
  const winsorized = winsorize(rawColumn);
  const z = zScore(winsorized);
  const clamped = clamp(z);
  const filled = fillNullWithZero(clamped);
  return applyWeight(filled, weight);
}

export interface BuildVectorsResult {
  /** rows[i] に対応する13次元の生の派生値（変換前） */
  rawDimensions: (number | null)[][];
  /** rows[i] に対応する coverage（非NULL次元数） */
  coverage: number[];
  /** rows[i] に対応する変換済み13次元ベクトル */
  vectors: number[][];
}

/**
 * 除外判定済みの行集合から、列（次元）単位でパイプラインを適用し、
 * 各銘柄の13次元ベクトルを組み立てる。
 * winsorize/z-scoreは「渡された行集合内」の非NULL値のみを母集団とするため、
 * 呼び出し側で shouldExcludeStock による事前フィルタを済ませておくこと。
 */
export function buildVectors(
  rows: Pick<
    ParsedScreenRow,
    | 'per'
    | 'pbr'
    | 'dividend_yield'
    | 'roe'
    | 'market_cap'
    | 'value_pct'
    | 'quality_pct'
    | 'momentum_pct'
    | 'dev_25'
    | 'dev_200'
    | 'rsi_14'
    | 'atr_pct'
    | 'vol_ratio_20'
  >[]
): BuildVectorsResult {
  const rawDimensions = rows.map(deriveRawDimensions);
  const coverage = rawDimensions.map(countCoverage);
  const numRows = rows.length;
  const vectors: number[][] = Array.from({ length: numRows }, () => new Array(VECTOR_LENGTH).fill(0));

  for (let d = 0; d < VECTOR_LENGTH; d++) {
    const column = rawDimensions.map((r) => r[d]);
    const transformed = transformColumn(column, DIMENSIONS[d].weight);
    for (let j = 0; j < numRows; j++) {
      vectors[j][d] = transformed[j];
    }
  }

  return { rawDimensions, coverage, vectors };
}

// ===== 出力ヘルパー =====

/** pgvectorのリテラル文字列表現 "[0.1,0.2,...]" に変換（丸めでペイロードを抑制） */
export function formatEmbeddingLiteral(vector: number[], precision = 6): string {
  return `[${vector.map((v) => Number(v.toFixed(precision))).join(',')}]`;
}

/** コサイン類似度（--similar 検証モード用。DB側の 1 - (a <=> b) と等価な計算） */
export function cosineSimilarity(a: number[], b: number[]): number {
  if (a.length !== b.length) {
    throw new Error(`cosineSimilarity: dimension mismatch (${a.length} vs ${b.length})`);
  }
  let dot = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  if (normA === 0 || normB === 0) return 0;
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

/** 数値配列の分布（非NULL値の min/max/mean、非NULL件数）。--dry-run のログ出力用 */
export function distributionStats(values: (number | null)[]): {
  count: number;
  min: number | null;
  max: number | null;
  mean: number | null;
} {
  const nonNull = values.filter((v): v is number => v != null);
  if (nonNull.length === 0) {
    return { count: 0, min: null, max: null, mean: null };
  }
  return {
    count: nonNull.length,
    min: Math.min(...nonNull),
    max: Math.max(...nonNull),
    mean: nonNull.reduce((s, v) => s + v, 0) / nonNull.length,
  };
}
