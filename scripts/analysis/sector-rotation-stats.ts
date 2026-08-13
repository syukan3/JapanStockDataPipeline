/**
 * セクターローテーション遡及評価の統計ユーティリティ（純関数）
 *
 * @description Newey-West(HAC) 標準誤差・重複窓ラグ・順位相関・ドローダウン等。
 * 実装は JapanStockScouter の `src/backtest/simulation/hac.ts` と同一の定義を移植したもの
 * （リポジトリを跨いだ import ができないため。数値が一致することは意図的）。
 *
 * 正規化の約束（Scouter版と同じ）:
 * - lag=0 のとき hacStdError は sd/√n に一致する
 * - hacRegression の SE は HC0 × √(n/(n−1))。古典的OLSでもHC1でもない
 */

/** ラグ j の自己共分散 γ_j（平均まわり、1/(n-1) 正規化） */
function autoCovariance(xs: number[], mean: number, j: number): number {
  const n = xs.length;
  if (j >= n) return 0;
  let acc = 0;
  for (let t = j; t < n; t++) acc += (xs[t] - mean) * (xs[t - j] - mean);
  return acc / (n - 1);
}

/** S = γ0 + 2 Σ_{j=1..L} (1 - j/(L+1)) γ_j （Bartlett カーネル） */
export function longRunVariance(xs: number[], lag: number): number {
  const n = xs.length;
  if (n < 2) return 0;
  const mean = xs.reduce((a, b) => a + b, 0) / n;
  const L = Math.max(0, Math.min(Math.floor(lag), n - 1));
  let s = autoCovariance(xs, mean, 0);
  for (let j = 1; j <= L; j++) {
    s += 2 * (1 - j / (L + 1)) * autoCovariance(xs, mean, j);
  }
  return Math.max(0, s);
}

export function meanOf(xs: number[]): number {
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : 0;
}

export function stdevOf(xs: number[]): number {
  const n = xs.length;
  if (n < 2) return 0;
  const m = meanOf(xs);
  return Math.sqrt(xs.reduce((s, v) => s + (v - m) ** 2, 0) / (n - 1));
}

/** HAC 標準誤差（平均の SE）。lag=0 なら sd/√n */
export function hacStdError(xs: number[], lag: number): number {
  const n = xs.length;
  if (n < 2) return 0;
  return Math.sqrt(longRunVariance(xs, lag) / n);
}

/** 平均が 0 と異なるかの HAC t 値 */
export function hacTStat(xs: number[], lag: number): number {
  const se = hacStdError(xs, lag);
  return se > 0 ? meanOf(xs) / se : 0;
}

/**
 * 重複窓のラグ数: ceil(horizonDays / periodBusinessDays) - 1
 * 週次(5営業日)なら 20日→3 / 60日→11 / 120日→23
 */
export function overlapLag(horizonDays: number, periodBusinessDays: number): number {
  if (!(horizonDays > 0) || !(periodBusinessDays > 0)) return 0;
  return Math.max(0, Math.ceil(horizonDays / periodBusinessDays) - 1);
}

export interface HacRegression {
  beta: number;
  alpha: number;
  tStat: number;
  stdError: number;
  n: number;
}

/** 単回帰 y = alpha + beta·x の HAC サンドイッチ推定 */
export function hacRegression(x: number[], y: number[], lag: number): HacRegression {
  const n = Math.min(x.length, y.length);
  if (n < 3) return { beta: 0, alpha: 0, tStat: 0, stdError: 0, n };
  const xs = x.slice(0, n);
  const ys = y.slice(0, n);
  const xBar = meanOf(xs);
  const yBar = meanOf(ys);
  let sxx = 0;
  let sxy = 0;
  for (let i = 0; i < n; i++) {
    const z = xs[i] - xBar;
    sxx += z * z;
    sxy += z * (ys[i] - yBar);
  }
  if (!(sxx > 0)) return { beta: 0, alpha: yBar, tStat: 0, stdError: 0, n };
  const beta = sxy / sxx;
  const alpha = yBar - beta * xBar;
  const u = new Array<number>(n);
  for (let i = 0; i < n; i++) u[i] = (xs[i] - xBar) * (ys[i] - alpha - beta * xs[i]);
  const stdError = Math.sqrt(n * longRunVariance(u, lag)) / sxx;
  return { beta, alpha, tStat: stdError > 0 ? beta / stdError : 0, stdError, n };
}

/** 標準正規の累積分布（Abramowitz-Stegun 7.1.26 の erf 近似） */
export function normalCdf(z: number): number {
  const sign = z < 0 ? -1 : 1;
  const a = Math.abs(z) / Math.SQRT2;
  const t = 1 / (1 + 0.3275911 * a);
  const y =
    1 -
    ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) *
      t *
      Math.exp(-a * a);
  return 0.5 * (1 + sign * y);
}

/** 両側 p 値（正規近似） */
export function twoSidedP(t: number): number {
  return 2 * (1 - normalCdf(Math.abs(t)));
}

/** タイ考慮の順位（平均順位）に変換 */
function toRanks(values: number[]): number[] {
  const idx = values.map((v, i) => ({ v, i })).sort((a, b) => a.v - b.v);
  const ranks = new Array<number>(values.length);
  let i = 0;
  while (i < idx.length) {
    let j = i;
    while (j + 1 < idx.length && idx[j + 1].v === idx[i].v) j++;
    const avg = (i + j) / 2 + 1;
    for (let k = i; k <= j; k++) ranks[idx[k].i] = avg;
    i = j + 1;
  }
  return ranks;
}

/** スピアマン順位相関 */
export function spearman(x: number[], y: number[]): number {
  const n = Math.min(x.length, y.length);
  if (n < 3) return 0;
  const rx = toRanks(x.slice(0, n));
  const ry = toRanks(y.slice(0, n));
  const mx = meanOf(rx);
  const my = meanOf(ry);
  let cov = 0;
  let vx = 0;
  let vy = 0;
  for (let i = 0; i < n; i++) {
    const dx = rx[i] - mx;
    const dy = ry[i] - my;
    cov += dx * dy;
    vx += dx * dx;
    vy += dy * dy;
  }
  return vx > 0 && vy > 0 ? cov / Math.sqrt(vx * vy) : 0;
}

/** 累積リターン系列（等比）から最大ドローダウン（正の小数、0.25 = -25%） */
export function maxDrawdown(returns: number[]): number {
  let level = 1;
  let peak = 1;
  let mdd = 0;
  for (const r of returns) {
    level *= 1 + r;
    if (level > peak) peak = level;
    const dd = peak > 0 ? 1 - level / peak : 0;
    if (dd > mdd) mdd = dd;
  }
  return mdd;
}

/** 期間リターン列から年率換算（periodsPerYear 期/年の等比複利） */
export function annualize(returns: number[], periodsPerYear: number): number {
  if (returns.length === 0) return 0;
  let level = 1;
  for (const r of returns) level *= 1 + r;
  return level > 0 ? level ** (periodsPerYear / returns.length) - 1 : -1;
}

/** 情報比（超過リターン平均 / 標準偏差 の年率化） */
export function informationRatio(excess: number[], periodsPerYear: number): number {
  const sd = stdevOf(excess);
  return sd > 0 ? (meanOf(excess) / sd) * Math.sqrt(periodsPerYear) : 0;
}

/** 昇順ソート済み配列に対する分位点（nearest-rank） */
export function quantileSorted(sorted: number[], q: number): number {
  if (sorted.length === 0) return NaN;
  const i = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * q)));
  return sorted[i];
}

/** value が history 内で占めるパーセンタイル（0-1） */
export function percentileRank(history: number[], value: number): number {
  if (history.length === 0) return 0.5;
  let below = 0;
  for (const h of history) if (h < value) below++;
  return below / history.length;
}
