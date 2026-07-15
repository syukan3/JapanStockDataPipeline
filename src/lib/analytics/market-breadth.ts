/**
 * 市場全体ブレッドス指標の計算コア（騰落数・騰落レシオ・新高値/新安値・売買代金・SMA上回り比率）
 *
 * @description
 * 日付昇順にバーを流し込むストリーミング設計（BreadthAccumulator）。
 * - seed（Scouter backtest.sqlite の5年分）と日次更新（equity_bar_daily）で共用する。
 * - 状態（前日終値・年別高値/安値・SMA用リングバッファ）は全銘柄で維持し、
 *   カウント対象のみユニバース（プライム＝2022-04再編前は東証一部）でフィルタする。
 *   → 市場区分の異動があっても銘柄自身の高値履歴・SMA履歴は連続する。
 *
 * 新高値/新安値の定義（日経方式）:
 * - 4月〜12月: 年初来（当年1月1日以降の調整後高値/安値と比較）
 * - 1月〜3月: 前年来（前年1月1日以降と比較）
 * - 当日の高値が「当日を含まない」基準期間の最高値を上回れば新高値（安値は逆）
 * - 基準期間内に取引履歴が無い銘柄（新規上場初日等）はカウント対象外
 *
 * 騰落レシオ(25日) = Σ値上がり銘柄数(25営業日) / Σ値下がり銘柄数(25営業日) × 100
 *
 * breadth %（SMA上回り比率）:
 * - 銘柄ごとに直近終値（調整後）のリングバッファ（上限200件）を保持し、
 *   当日終値を含む直近25件/200件の単純移動平均と当日終値を比較する。
 * - 分母は「リングバッファがwindow件数以上たまっている（=SMAが計算できる）」
 *   プライム銘柄のみ（上場直後銘柄等はSMAが計算できるまで分母除外、0とはしない）。
 * - ユニバース内でSMAが計算できる銘柄が1件も無い日は null（0%ではない）。
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
  /** SMA25(25営業日)を上回るプライム銘柄の比率[%]。分母不足（0件）なら null */
  pctAboveSma25: number | null;
  /** SMA200(200営業日)を上回るプライム銘柄の比率[%]。分母不足（0件）なら null */
  pctAboveSma200: number | null;
}

/** SMA（breadth %）の窓（営業日数） */
export const SMA_SHORT_WINDOW = 25;
export const SMA_LONG_WINDOW = 200;
/**
 * SMA200暖機に必要な暦日数の目安（cron の feedStart 逆算専用）。
 * プライム市場の年間営業日数（約245日）から200営業日分を暦日換算し、
 * 祝日偏在・閏年を見込んで約15%のバッファを加算（200/245*365 ≈ 298 → 340）。
 * リングバッファ自体は営業日ベースで動くため、この定数はあくまで
 * 「最低限これだけ遡ってbarを流し込めば十分」という目安値。
 */
export const SMA_LONG_WARMUP_CALENDAR_DAYS = 340;

/**
 * pct_above_sma25/200 が「原理的に算出可能になる」開始日の目安。
 * breadth系のバックフィル元（Scouter backtest.sqlite）は 2021-02-08 収録開始のため、
 * そこから window 営業日分（+マージン）が経過するまでは、ユニバース全銘柄が
 * window件の終値を持たず「全銘柄不足日はnull」（本ファイル冒頭のbreadth %節参照）が
 * 恒久的に成立する。この期間のnullは「未計算」ではなく「期待される恒久null」であり、
 * cron/seed の pending・missing 判定に含めてはならない（含めると原理的に埋まらない
 * 行を毎回pending扱いし続け、feedStartの逆算で全期間再スキャンが恒久化する）。
 * 値は 2021-02-08 + window営業日 を暦換算（約1.45倍）した上で安全側に切り上げた月初。
 */
export const PCT_SMA25_EXPECTED_FROM = '2021-04-01';
export const PCT_SMA200_EXPECTED_FROM = '2021-12-01';

/**
 * pct_above_sma25 が null の日を pending/missing（再計算対象）として扱うべきか。
 * 境界日（PCT_SMA25_EXPECTED_FROM）より前の null は「期待される恒久null」であり
 * false を返す（= pending扱いしない）。cron の fillBreadth と seed の missing判定で共用。
 */
export function isPctSma25Pending(date: string, pctAboveSma25: number | null | undefined): boolean {
  return date >= PCT_SMA25_EXPECTED_FROM && pctAboveSma25 == null;
}

/** pct_above_sma200 版。判定基準は isPctSma25Pending と同様（境界日は PCT_SMA200_EXPECTED_FROM）。 */
export function isPctSma200Pending(date: string, pctAboveSma200: number | null | undefined): boolean {
  return date >= PCT_SMA200_EXPECTED_FROM && pctAboveSma200 == null;
}

/** 銘柄ごとの高値/安値状態（当年・前年の2バケット）＋SMA用の終値リングバッファ */
interface CodeState {
  prevClose: number | null;
  curYear: number;
  curMax: number;
  curMin: number;
  prevYear: number | null;
  prevMax: number;
  prevMin: number;
  /** 直近終値（調整後）のリングバッファ。古い→新しい順、上限 SMA_LONG_WINDOW 件 */
  closes: number[];
}

/**
 * 終値配列（古い→新しい）からSMAを計算する純関数。
 * 件数が window 未満なら null（=分母除外。まだSMAが計算できない）。
 */
export function computeSma(closes: readonly number[], window: number): number | null {
  if (closes.length < window) return null;
  let sum = 0;
  for (let i = closes.length - window; i < closes.length; i++) sum += closes[i];
  return sum / window;
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
    let sma25Above = 0;
    let sma25Denom = 0;
    let sma200Above = 0;
    let sma200Denom = 0;

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

      // breadth %: 当日終値を含むリングバッファでSMAを計算（更新後の状態を参照）
      if (primeSet.has(bar.code) && close != null) {
        const closes = this.states.get(bar.code)!.closes;
        const sma25 = computeSma(closes, SMA_SHORT_WINDOW);
        if (sma25 != null) {
          sma25Denom++;
          if (close > sma25) sma25Above++;
        }
        const sma200 = computeSma(closes, SMA_LONG_WINDOW);
        if (sma200 != null) {
          sma200Denom++;
          if (close > sma200) sma200Above++;
        }
      }
    }

    const pctAboveSma25 =
      sma25Denom > 0 ? Number(((sma25Above / sma25Denom) * 100).toFixed(1)) : null;
    const pctAboveSma200 =
      sma200Denom > 0 ? Number(((sma200Above / sma200Denom) * 100).toFixed(1)) : null;

    return {
      date,
      advancers,
      decliners,
      unchanged,
      newHighs,
      newLows,
      turnoverValue,
      primeBarCount,
      pctAboveSma25,
      pctAboveSma200,
    };
  }

  /** 現在の状態を返す（年をまたいだ銘柄はバケットをロールした形で参照）。closesは年をまたいでも引き継ぐ */
  private rolledState(code: string, currentYear: number): CodeState | null {
    const s = this.states.get(code);
    if (!s) return null;
    if (s.curYear === currentYear) return s;
    if (s.curYear === currentYear - 1) {
      // 年初のロール: 当年バケット→前年バケットへ（SMAリングバッファは年をまたいで連続）
      return {
        prevClose: s.prevClose,
        curYear: currentYear,
        curMax: -Infinity,
        curMin: Infinity,
        prevYear: s.curYear,
        prevMax: s.curMax,
        prevMin: s.curMin,
        closes: s.closes,
      };
    }
    // 2年以上取引が無い銘柄: 基準期間内の履歴なし扱い（SMAリングバッファは引き継ぐ）
    return {
      prevClose: s.prevClose,
      curYear: currentYear,
      curMax: -Infinity,
      curMin: Infinity,
      prevYear: null,
      prevMax: -Infinity,
      prevMin: Infinity,
      closes: s.closes,
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
        closes: [],
      };
      this.states.set(code, s);
    }
    if (close != null) s.prevClose = close;
    if (high != null) s.curMax = Math.max(s.curMax, high);
    if (low != null) s.curMin = Math.min(s.curMin, low);
    if (close != null) {
      s.closes.push(close);
      if (s.closes.length > SMA_LONG_WINDOW) s.closes.shift();
    }
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
