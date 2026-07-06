/**
 * 市場全体ブレッドス指標の計算コア（騰落数・騰落レシオ・新高値/新安値・売買代金）
 *
 * @description
 * 日付昇順にバーを流し込むストリーミング設計（BreadthAccumulator）。
 * - seed（Scouter backtest.sqlite の5年分）と日次更新（equity_bar_daily）で共用する。
 * - 状態（前日終値・年別高値/安値）は全銘柄で維持し、カウント対象のみ
 *   ユニバース（プライム＝2022-04再編前は東証一部）でフィルタする。
 *   → 市場区分の異動があっても銘柄自身の高値履歴は連続する。
 *
 * 新高値/新安値の定義（日経方式）:
 * - 4月〜12月: 年初来（当年1月1日以降の調整後高値/安値と比較）
 * - 1月〜3月: 前年来（前年1月1日以降と比較）
 * - 当日の高値が「当日を含まない」基準期間の最高値を上回れば新高値（安値は逆）
 * - 基準期間内に取引履歴が無い銘柄（新規上場初日等）はカウント対象外
 *
 * 騰落レシオ(25日) = Σ値上がり銘柄数(25営業日) / Σ値下がり銘柄数(25営業日) × 100
 */

export interface BreadthBar {
  code: string;
  adjClose: number | null;
  adjHigh: number | null;
  adjLow: number | null;
  turnoverValue: number | null;
}

export interface BreadthDay {
  date: string;
  advancers: number;
  decliners: number;
  unchanged: number;
  newHighs: number;
  newLows: number;
  /** プライムユニバースの売買代金合計[円] */
  turnoverValue: number;
  /** 当日バーがあったプライム銘柄数（カバレッジゲート用） */
  primeBarCount: number;
}

/** 銘柄ごとの高値/安値状態（当年・前年の2バケット） */
interface CodeState {
  prevClose: number | null;
  curYear: number;
  curMax: number;
  curMin: number;
  prevYear: number | null;
  prevMax: number;
  prevMin: number;
}

/** 日経方式の基準期間に前年を含めるか（1〜3月は前年来） */
export function includesPreviousYear(date: string): boolean {
  const month = Number(date.slice(5, 7));
  return month >= 1 && month <= 3;
}

export class BreadthAccumulator {
  private readonly states = new Map<string, CodeState>();

  /**
   * 1営業日分のバーを処理して、その日のブレッドスを返す。
   * 必ず日付昇順で呼ぶこと。
   *
   * @param date YYYY-MM-DD
   * @param bars 当日の全銘柄バー（プライム以外も含めてよい。状態維持のため全銘柄推奨）
   * @param primeSet カウント対象ユニバース（当該日時点のプライム＝旧一部の local_code 集合）
   */
  addDay(date: string, bars: BreadthBar[], primeSet: ReadonlySet<string>): BreadthDay {
    const year = Number(date.slice(0, 4));
    const usePrevYear = includesPreviousYear(date);

    let advancers = 0;
    let decliners = 0;
    let unchanged = 0;
    let newHighs = 0;
    let newLows = 0;
    let turnoverValue = 0;
    let primeBarCount = 0;

    for (const bar of bars) {
      const state = this.rolledState(bar.code, year);
      const close = bar.adjClose;
      const high = bar.adjHigh ?? close;
      const low = bar.adjLow ?? close;

      if (primeSet.has(bar.code)) {
        primeBarCount++;
        if (bar.turnoverValue != null) turnoverValue += bar.turnoverValue;

        if (state && close != null && state.prevClose != null) {
          if (close > state.prevClose) advancers++;
          else if (close < state.prevClose) decliners++;
          else unchanged++;
        }

        if (state) {
          // 基準期間（当日を含まない）の最高値/最安値
          let baseMax = state.curYear === year ? state.curMax : -Infinity;
          let baseMin = state.curYear === year ? state.curMin : Infinity;
          if (usePrevYear && state.prevYear === year - 1) {
            baseMax = Math.max(baseMax, state.prevMax);
            baseMin = Math.min(baseMin, state.prevMin);
          }
          if (baseMax > -Infinity && high != null && high > baseMax) newHighs++;
          if (baseMin < Infinity && low != null && low < baseMin) newLows++;
        }
      }

      this.updateState(bar.code, year, close, high, low);
    }

    return { date, advancers, decliners, unchanged, newHighs, newLows, turnoverValue, primeBarCount };
  }

  /** 現在の状態を返す（年をまたいだ銘柄はバケットをロールした形で参照） */
  private rolledState(code: string, currentYear: number): CodeState | null {
    const s = this.states.get(code);
    if (!s) return null;
    if (s.curYear === currentYear) return s;
    if (s.curYear === currentYear - 1) {
      // 年初のロール: 当年バケット→前年バケットへ
      return {
        prevClose: s.prevClose,
        curYear: currentYear,
        curMax: -Infinity,
        curMin: Infinity,
        prevYear: s.curYear,
        prevMax: s.curMax,
        prevMin: s.curMin,
      };
    }
    // 2年以上取引が無い銘柄: 基準期間内の履歴なし扱い
    return {
      prevClose: s.prevClose,
      curYear: currentYear,
      curMax: -Infinity,
      curMin: Infinity,
      prevYear: null,
      prevMax: -Infinity,
      prevMin: Infinity,
    };
  }

  private updateState(
    code: string,
    year: number,
    close: number | null,
    high: number | null,
    low: number | null
  ): void {
    let s = this.states.get(code);
    if (!s || s.curYear !== year) {
      const rolled = s ? this.rolledState(code, year) : null;
      s = rolled ?? {
        prevClose: null,
        curYear: year,
        curMax: -Infinity,
        curMin: Infinity,
        prevYear: null,
        prevMax: -Infinity,
        prevMin: Infinity,
      };
      this.states.set(code, s);
    }
    if (close != null) s.prevClose = close;
    if (high != null) s.curMax = Math.max(s.curMax, high);
    if (low != null) s.curMin = Math.min(s.curMin, low);
  }
}

/**
 * 騰落レシオ(25日)を計算。窓が揃わない場合は null。
 *
 * @param advancers 直近25営業日の値上がり銘柄数（古い→新しい、当日を含む）
 * @param decliners 同・値下がり銘柄数
 */
export function computeAdvDecRatio25(
  advancers: Array<number | null | undefined>,
  decliners: Array<number | null | undefined>
): number | null {
  if (advancers.length !== 25 || decliners.length !== 25) return null;
  let advSum = 0;
  let decSum = 0;
  for (let i = 0; i < 25; i++) {
    const a = advancers[i];
    const d = decliners[i];
    if (a == null || d == null) return null;
    advSum += a;
    decSum += d;
  }
  if (decSum === 0) return null;
  return Number(((advSum / decSum) * 100).toFixed(2));
}

/** プライム（2022-04-04再編後 0111）＋旧東証一部（0101）の market_code 集合 */
export const PRIME_MARKET_CODES = new Set(['0111', '0101']);
