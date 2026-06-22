/**
 * テクニカル指標の計算（純関数）
 *
 * @description
 * 価格系列（昇順=古い→新しい）から各種テクニカル指標を計算する依存ゼロの純関数群。
 * `computeTechnicalSnapshot()` は最新バー1点ぶんの「スナップショット」を返し、
 * scripts/cron/refresh-technical.ts が analytics.technical_metrics へ保存する。
 *
 * ⚠️ 数式は Portfolio 側 `lib/indicators.ts` と厳密に一致させること。
 *    両リポ共通のゴールデンテストベクタ（__fixtures__/golden-vectors.json）で照合する。
 *
 * 計算価格: adj_close（スナップショットは横断比較のため調整後で統一）。
 *   - RSI: 期間14, Wilder 平滑
 *   - MACD: EMA12 / EMA26, シグナル EMA9, ヒスト = MACD - シグナル
 *   - ボリンジャー: SMA20 ± 2σ（母集団σ）
 *   - ストキャス: slow %K(14,3) / %D = SMA3
 *   - ATR: 期間14, Wilder
 *   - 一目: 転換9 / 基準26 / 先行スパンB=52, 雲=26先行
 *   - クロス: SMA25 × SMA75
 */

export interface Bar {
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export type CrossType = 'golden' | 'dead';
export type Cross = { type: CrossType; age: number } | null;
export type IchimokuState = 'above' | 'inside' | 'below';

export type Series = (number | null)[];

export interface TechnicalSnapshot {
  close: number | null;
  sma_25: number | null;
  sma_75: number | null;
  sma_200: number | null;
  dev_25: number | null;
  dev_75: number | null;
  dev_200: number | null;
  above_sma200: boolean | null;
  cross_25_75: CrossType | null;
  cross_25_75_age: number | null;
  rsi_14: number | null;
  macd: number | null;
  macd_signal: number | null;
  macd_hist: number | null;
  stoch_k: number | null;
  stoch_d: number | null;
  bb_percent_b: number | null;
  bb_bandwidth: number | null;
  atr_14: number | null;
  atr_pct: number | null;
  vol_ratio_20: number | null;
  ichimoku_state: IchimokuState | null;
}

// ===== 数値ユーティリティ =====

/** 小数 d 桁に丸める（null はそのまま） */
export function round(n: number | null | undefined, d: number): number | null {
  if (n == null || !Number.isFinite(n)) return null;
  const f = 10 ** d;
  return Math.round(n * f) / f;
}

const last = <T>(a: T[]): T | undefined => a[a.length - 1];

// ===== 移動平均 =====

/** 単純移動平均（period 未満は null） */
export function sma(values: number[], period: number): Series {
  const out: Series = new Array(values.length).fill(null);
  if (period <= 0) return out;
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    sum += values[i];
    if (i >= period) sum -= values[i - period];
    if (i >= period - 1) out[i] = sum / period;
  }
  return out;
}

/** nullable 系列の SMA（窓内に null があればその位置は null） */
function smaNullable(values: Series, period: number): Series {
  const out: Series = new Array(values.length).fill(null);
  if (period <= 0) return out;
  for (let i = period - 1; i < values.length; i++) {
    let sum = 0;
    let ok = true;
    for (let j = i - period + 1; j <= i; j++) {
      const v = values[j];
      if (v == null) { ok = false; break; }
      sum += v;
    }
    if (ok) out[i] = sum / period;
  }
  return out;
}

/** 指数移動平均（先頭 period 本の SMA をシードに Wilder ではなく標準 EMA） */
export function ema(values: number[], period: number): Series {
  const out: Series = new Array(values.length).fill(null);
  if (period <= 0 || values.length < period) return out;
  const k = 2 / (period + 1);
  let seed = 0;
  for (let i = 0; i < period; i++) seed += values[i];
  seed /= period;
  out[period - 1] = seed;
  let prev = seed;
  for (let i = period; i < values.length; i++) {
    prev = values[i] * k + prev * (1 - k);
    out[i] = prev;
  }
  return out;
}

// ===== オシレーター =====

function rsiFrom(avgGain: number, avgLoss: number): number {
  if (avgLoss === 0) return avgGain === 0 ? 50 : 100;
  if (avgGain === 0) return 0;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

/** RSI（Wilder 平滑） */
export function rsi(closes: number[], period = 14): Series {
  const out: Series = new Array(closes.length).fill(null);
  if (closes.length <= period) return out;
  let gainSum = 0;
  let lossSum = 0;
  for (let i = 1; i <= period; i++) {
    const ch = closes[i] - closes[i - 1];
    if (ch >= 0) gainSum += ch; else lossSum -= ch;
  }
  let avgGain = gainSum / period;
  let avgLoss = lossSum / period;
  out[period] = rsiFrom(avgGain, avgLoss);
  for (let i = period + 1; i < closes.length; i++) {
    const ch = closes[i] - closes[i - 1];
    const gain = ch > 0 ? ch : 0;
    const loss = ch < 0 ? -ch : 0;
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
    out[i] = rsiFrom(avgGain, avgLoss);
  }
  return out;
}

export interface MacdResult {
  macd: Series;
  signal: Series;
  hist: Series;
}

/** MACD（EMA fast/slow, シグナル EMA） */
export function macd(closes: number[], fast = 12, slow = 26, signalPeriod = 9): MacdResult {
  const emaFast = ema(closes, fast);
  const emaSlow = ema(closes, slow);
  const macdLine: Series = closes.map((_, i) =>
    emaFast[i] != null && emaSlow[i] != null ? (emaFast[i] as number) - (emaSlow[i] as number) : null
  );
  const firstIdx = macdLine.findIndex((v) => v != null);
  const signal: Series = new Array(closes.length).fill(null);
  if (firstIdx >= 0) {
    const defined = macdLine.slice(firstIdx).map((v) => v as number);
    const sig = ema(defined, signalPeriod);
    for (let i = 0; i < sig.length; i++) signal[firstIdx + i] = sig[i];
  }
  const hist: Series = closes.map((_, i) =>
    macdLine[i] != null && signal[i] != null ? (macdLine[i] as number) - (signal[i] as number) : null
  );
  return { macd: macdLine, signal, hist };
}

export interface BollingerResult {
  mid: Series;
  upper: Series;
  lower: Series;
  percentB: Series;
  bandwidth: Series;
}

/** ボリンジャーバンド（母集団σ） */
export function bollinger(closes: number[], period = 20, mult = 2): BollingerResult {
  const mid = sma(closes, period);
  const upper: Series = new Array(closes.length).fill(null);
  const lower: Series = new Array(closes.length).fill(null);
  const percentB: Series = new Array(closes.length).fill(null);
  const bandwidth: Series = new Array(closes.length).fill(null);
  for (let i = period - 1; i < closes.length; i++) {
    const m = mid[i] as number;
    let sq = 0;
    for (let j = i - period + 1; j <= i; j++) sq += (closes[j] - m) ** 2;
    const sd = Math.sqrt(sq / period);
    const u = m + mult * sd;
    const l = m - mult * sd;
    upper[i] = u;
    lower[i] = l;
    bandwidth[i] = m !== 0 ? (u - l) / m : null;
    percentB[i] = u !== l ? (closes[i] - l) / (u - l) : null;
  }
  return { mid, upper, lower, percentB, bandwidth };
}

export interface StochasticResult {
  k: Series;
  d: Series;
}

/** ストキャスティクス（slow: %K = SMA(smooth) of rawK, %D = SMA(dPeriod) of %K） */
export function stochastic(
  highs: number[],
  lows: number[],
  closes: number[],
  kPeriod = 14,
  smooth = 3,
  dPeriod = 3
): StochasticResult {
  const rawK: Series = new Array(closes.length).fill(null);
  for (let i = kPeriod - 1; i < closes.length; i++) {
    let hh = -Infinity;
    let ll = Infinity;
    for (let j = i - kPeriod + 1; j <= i; j++) {
      if (highs[j] > hh) hh = highs[j];
      if (lows[j] < ll) ll = lows[j];
    }
    rawK[i] = hh !== ll ? ((closes[i] - ll) / (hh - ll)) * 100 : null;
  }
  const k = smaNullable(rawK, smooth);
  const d = smaNullable(k, dPeriod);
  return { k, d };
}

// ===== ボラティリティ・出来高 =====

/** ATR（Wilder） */
export function atr(highs: number[], lows: number[], closes: number[], period = 14): Series {
  const n = closes.length;
  const out: Series = new Array(n).fill(null);
  if (n <= period) return out;
  const tr: number[] = new Array(n).fill(0);
  tr[0] = highs[0] - lows[0];
  for (let i = 1; i < n; i++) {
    tr[i] = Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i] - closes[i - 1])
    );
  }
  // Wilder シード: tr[1..period] の平均を index=period に置く
  let sum = 0;
  for (let i = 1; i <= period; i++) sum += tr[i];
  let prev = sum / period;
  out[period] = prev;
  for (let i = period + 1; i < n; i++) {
    prev = (prev * (period - 1) + tr[i]) / period;
    out[i] = prev;
  }
  return out;
}

/** OBV（On-Balance Volume）。チャート表示用（スナップショット非対象） */
export function obv(closes: number[], volumes: number[]): number[] {
  const out: number[] = new Array(closes.length).fill(0);
  for (let i = 1; i < closes.length; i++) {
    const dir = closes[i] > closes[i - 1] ? 1 : closes[i] < closes[i - 1] ? -1 : 0;
    out[i] = out[i - 1] + dir * volumes[i];
  }
  return out;
}

// ===== 一目均衡表 =====

function midline(highs: number[], lows: number[], period: number): Series {
  const out: Series = new Array(highs.length).fill(null);
  for (let i = period - 1; i < highs.length; i++) {
    let hh = -Infinity;
    let ll = Infinity;
    for (let j = i - period + 1; j <= i; j++) {
      if (highs[j] > hh) hh = highs[j];
      if (lows[j] < ll) ll = lows[j];
    }
    out[i] = (hh + ll) / 2;
  }
  return out;
}

export interface IchimokuResult {
  tenkan: Series; // 転換線(9)
  kijun: Series; // 基準線(26)
  senkouA: Series; // 先行スパンA（描画は +26 前方シフト）
  senkouB: Series; // 先行スパンB(52)（描画は +26 前方シフト）
}

/** 一目均衡表の各線（前方シフトなしの素の値。チャート側で 26 本シフトして描画する） */
export function ichimoku(highs: number[], lows: number[], closes: number[]): IchimokuResult {
  const tenkan = midline(highs, lows, 9);
  const kijun = midline(highs, lows, 26);
  const senkouA: Series = closes.map((_, i) =>
    tenkan[i] != null && kijun[i] != null ? ((tenkan[i] as number) + (kijun[i] as number)) / 2 : null
  );
  const senkouB = midline(highs, lows, 52);
  return { tenkan, kijun, senkouA, senkouB };
}

/** 一目「雲」と現値の位置関係。今日の雲は 26 本前に算出された先行スパン */
export function ichimokuState(
  highs: number[],
  lows: number[],
  closes: number[],
  displacement = 26
): IchimokuState | null {
  const { senkouA, senkouB } = ichimoku(highs, lows, closes);
  const n = closes.length;
  const idx = n - 1 - displacement;
  if (idx < 0) return null;
  const a = senkouA[idx];
  const b = senkouB[idx];
  if (a == null || b == null) return null;
  const top = Math.max(a, b);
  const bot = Math.min(a, b);
  const c = closes[n - 1];
  return c > top ? 'above' : c < bot ? 'below' : 'inside';
}

// ===== クロス検出 =====

/** 直近のクロス（short が long を上抜け=golden / 下抜け=dead）と経過営業日（0=当日） */
export function detectCross(short: Series, long: Series): Cross {
  const n = short.length;
  let lastIdx = -1;
  let type: CrossType | null = null;
  let prevSign = 0;
  for (let i = 0; i < n; i++) {
    const s = short[i];
    const l = long[i];
    if (s == null || l == null) continue;
    const diff = s - l;
    const sign = diff > 0 ? 1 : diff < 0 ? -1 : 0;
    if (sign !== 0 && prevSign !== 0 && sign !== prevSign) {
      lastIdx = i;
      type = sign > 0 ? 'golden' : 'dead';
    }
    if (sign !== 0) prevSign = sign;
  }
  if (lastIdx < 0 || type == null) return null;
  return { type, age: n - 1 - lastIdx };
}

// ===== スナップショット =====

/**
 * 最新バー1点ぶんのテクニカルスナップショットを計算。
 * bars は昇順（古い→新しい）の adj 系 OHLCV を渡す。
 */
export function computeTechnicalSnapshot(bars: Bar[]): TechnicalSnapshot | null {
  if (bars.length === 0) return null;
  const highs = bars.map((b) => b.high);
  const lows = bars.map((b) => b.low);
  const closes = bars.map((b) => b.close);
  const vols = bars.map((b) => b.volume);
  const n = closes.length;
  const i = n - 1;
  const close = closes[i];

  const s25 = sma(closes, 25)[i];
  const s75 = sma(closes, 75)[i];
  const s200 = sma(closes, 200)[i];
  const dev = (m: number | null | undefined): number | null =>
    m != null && m !== 0 ? round(((close - m) / m) * 100, 2) : null;

  const r = rsi(closes, 14)[i];
  const m = macd(closes, 12, 26, 9);
  const bb = bollinger(closes, 20, 2);
  const st = stochastic(highs, lows, closes, 14, 3, 3);
  const a14 = atr(highs, lows, closes, 14)[i];
  const cross = detectCross(sma(closes, 25), sma(closes, 75));
  const avgVol20 = sma(vols, 20)[i];
  const latestVol = vols[i];

  return {
    close: round(close, 2),
    sma_25: round(s25, 4),
    sma_75: round(s75, 4),
    sma_200: round(s200, 4),
    dev_25: dev(s25),
    dev_75: dev(s75),
    dev_200: dev(s200),
    above_sma200: s200 != null ? close > s200 : null,
    cross_25_75: cross?.type ?? null,
    cross_25_75_age: cross?.age ?? null,
    rsi_14: round(r, 2),
    macd: round(last(m.macd), 4),
    macd_signal: round(last(m.signal), 4),
    macd_hist: round(last(m.hist), 4),
    stoch_k: round(last(st.k), 2),
    stoch_d: round(last(st.d), 2),
    bb_percent_b: round(last(bb.percentB), 4),
    bb_bandwidth: round(last(bb.bandwidth), 4),
    atr_14: round(a14, 4),
    atr_pct: a14 != null && close !== 0 ? round((a14 / close) * 100, 2) : null,
    vol_ratio_20: avgVol20 != null && avgVol20 !== 0 ? round(latestVol / avgVol20, 3) : null,
    ichimoku_state: ichimokuState(highs, lows, closes),
  };
}
