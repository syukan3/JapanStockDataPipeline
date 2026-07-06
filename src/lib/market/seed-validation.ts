/**
 * 市場指標 seed の検証（自前計算 breadth と nikkei225jp 参照値の突合）
 *
 * @description
 * sqlite の営業日丸ごと欠落・列マッピング破壊・ユニバース崩れは、
 * 騰落レシオ(25日)が参照値から系統的に乖離する形で現れる。
 * 統計を返すだけでなく閾値ゲートとして機能させ、逸脱時は seed を失敗させる。
 *
 * 閾値の根拠（2026-07-06 実測: 5年1225日）:
 * - ratio 絶対差: mean 0.092 / p50 0.02 / p95 0.43 / max 3.19
 *   （残差はユニバース定義差・月次スナップショット近似によるもの）
 * - 新高値/新安値・売買代金は参照側の集計ユニバースが異なるため統計のみ（ゲートしない）
 */

export interface BreadthComparable {
  as_of_date: string;
  adv_dec_ratio_25d: number | null;
  new_highs: number;
  new_lows: number;
  prime_turnover_value: number;
}

export interface ReferenceRow {
  date: string;
  refAdvDecRatio: number | null;
  refNewHighs: number | null;
  refNewLows: number | null;
  refTurnoverMn: number | null;
}

export interface ValidationThresholds {
  /** ratio 絶対差の平均の上限 */
  ratioMeanMax: number;
  /** ratio 絶対差の p95 の上限 */
  ratioP95Max: number;
  /** ratio を照合できた日数の下限（ratio を持つ計算行に対する割合） */
  minMatchedRatio: number;
}

export const DEFAULT_THRESHOLDS: ValidationThresholds = {
  ratioMeanMax: 1.0,
  ratioP95Max: 3.0,
  minMatchedRatio: 0.9,
};

interface Stats {
  n: number;
  mean: number;
  p50: number;
  p95: number;
  max: number;
}

function stats(arr: number[]): Stats | null {
  if (arr.length === 0) return null;
  const sorted = [...arr].sort((a, b) => a - b);
  return {
    n: arr.length,
    mean: Number((arr.reduce((s, v) => s + v, 0) / arr.length).toFixed(3)),
    p50: Number(sorted[Math.floor(sorted.length / 2)].toFixed(3)),
    p95: Number(sorted[Math.floor(sorted.length * 0.95)].toFixed(3)),
    max: Number(sorted[sorted.length - 1].toFixed(3)),
  };
}

export interface ValidationResult {
  passed: boolean;
  failureReasons: string[];
  ratioAbsDiff: Stats | null;
  newHighsAbsDiff: Stats | null;
  newLowsAbsDiff: Stats | null;
  turnoverPctDiff: Stats | null;
  recentSamples: Array<Record<string, unknown>>;
}

/**
 * 自前計算 breadth を nikkei225jp の参照列と突合し、閾値ゲートを適用する。
 *
 * breadthRows が空（breadth フェーズ失敗など）の場合は照合対象なし＝ゲート通過とし、
 * breadth フェーズ自体の失敗報告に委ねる。
 */
export function validateAgainstReference(
  breadthRows: BreadthComparable[],
  referenceRows: ReferenceRow[],
  thresholds: ValidationThresholds = DEFAULT_THRESHOLDS
): ValidationResult {
  const refMap = new Map(referenceRows.map((r) => [r.date, r]));
  const diffs = {
    ratio: [] as number[],
    newHighs: [] as number[],
    newLows: [] as number[],
    turnoverPct: [] as number[],
  };
  let ratioComparable = 0;
  for (const row of breadthRows) {
    const ref = refMap.get(row.as_of_date);
    if (row.adv_dec_ratio_25d != null) ratioComparable++;
    if (!ref) continue;
    if (row.adv_dec_ratio_25d != null && ref.refAdvDecRatio != null) {
      diffs.ratio.push(Math.abs(row.adv_dec_ratio_25d - ref.refAdvDecRatio));
    }
    if (ref.refNewHighs != null) diffs.newHighs.push(Math.abs(row.new_highs - ref.refNewHighs));
    if (ref.refNewLows != null) diffs.newLows.push(Math.abs(row.new_lows - ref.refNewLows));
    if (ref.refTurnoverMn != null && ref.refTurnoverMn > 0) {
      diffs.turnoverPct.push(
        (Math.abs(row.prime_turnover_value / 1e6 - ref.refTurnoverMn) / ref.refTurnoverMn) * 100
      );
    }
  }

  const samples: Array<Record<string, unknown>> = [];
  for (const row of breadthRows.slice(-5)) {
    const ref = refMap.get(row.as_of_date);
    samples.push({
      date: row.as_of_date,
      computed: {
        ratio: row.adv_dec_ratio_25d,
        newHighs: row.new_highs,
        newLows: row.new_lows,
        turnoverMn: Math.round(row.prime_turnover_value / 1e6),
      },
      reference: ref
        ? {
            ratio: ref.refAdvDecRatio,
            newHighs: ref.refNewHighs,
            newLows: ref.refNewLows,
            turnoverMn: ref.refTurnoverMn,
          }
        : null,
    });
  }

  const ratioStats = stats(diffs.ratio);
  const failureReasons: string[] = [];
  if (breadthRows.length > 0 && ratioComparable > 0) {
    if (ratioStats == null) {
      failureReasons.push('ratio を1日も参照値と照合できない（参照ソース欠落/軸ズレの疑い）');
    } else {
      if (ratioStats.n < ratioComparable * thresholds.minMatchedRatio) {
        failureReasons.push(
          `ratio 照合日数不足: ${ratioStats.n}/${ratioComparable} (< ${thresholds.minMatchedRatio})`
        );
      }
      if (ratioStats.mean > thresholds.ratioMeanMax) {
        failureReasons.push(
          `ratio 平均乖離が閾値超過: mean ${ratioStats.mean} > ${thresholds.ratioMeanMax}（営業日欠落/列マッピング破壊の疑い）`
        );
      }
      if (ratioStats.p95 > thresholds.ratioP95Max) {
        failureReasons.push(`ratio p95乖離が閾値超過: ${ratioStats.p95} > ${thresholds.ratioP95Max}`);
      }
    }
  }

  return {
    passed: failureReasons.length === 0,
    failureReasons,
    ratioAbsDiff: ratioStats,
    newHighsAbsDiff: stats(diffs.newHighs),
    newLowsAbsDiff: stats(diffs.newLows),
    turnoverPctDiff: stats(diffs.turnoverPct),
    recentSamples: samples,
  };
}
