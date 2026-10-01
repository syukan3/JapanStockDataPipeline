/**
 * J-Quants 代替価格の純ロジック（Yahoo 日足 → alt_equity_bar_daily / TOPIX 推計）
 *
 * 設計正本: ../../../../docs/PLANS-jquants-off-switch-2026-10.md §4.2・§4.3
 */

import type { TopixBarDailyRecord } from '../jquants/types';
import type { YahooEquityBar, YahooSplitEvent } from './yahoo-equity-client';

/** TOPIX 推計に使う ETF（TOPIX 連動・1306） */
export const TOPIX_PROXY_ETF_CODE = '13060';

/** マクロ対立軸レポートの8本（JapanStockScouter src/strategies/macro-pair-rotation/definitions.ts と一致させる） */
export const MACRO_PAIR_ETF_CODES = ['16150', '16330', '16180', '16280', '16220', '16300', '16210', '16250'] as const;

/** 保有・ウォッチに関係なく毎日取る銘柄 */
export const FIXED_ALT_CODES: readonly string[] = [TOPIX_PROXY_ETF_CODE, ...MACRO_PAIR_ETF_CODES];

/** 前日比がこれを超え、分割イベントも無い日は異常値として採用しない */
export const MAX_DAILY_MOVE = 0.3;

/** alt_equity_bar_daily の1行 */
export interface AltBarRecord {
  local_code: string;
  trade_date: string;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number;
  volume: number | null;
  adj_open: number | null;
  adj_high: number | null;
  adj_low: number | null;
  adj_close: number;
  adj_volume: number | null;
  source: 'yahoo';
}

/** 異常値として採用しなかった日 */
export interface RejectedBar {
  date: string;
  adjClose: number;
  prevAdjClose: number;
}

function round2(v: number): number {
  return Math.round(v * 100) / 100;
}

/**
 * その日より後の分割比率の積（生値 = 調整値 × 積）
 */
export function cumulativeSplitRatioAfter(date: string, splits: YahooSplitEvent[]): number {
  return splits.filter((s) => s.exDate > date).reduce((acc, s) => acc * s.ratio, 1);
}

/**
 * 調整値（Yahoo の quote）から生値を逆算して alt_equity_bar_daily の行を作る（純関数）
 *
 * @param splits 取得期間内の分割イベント（Yahoo が報告したもの）
 */
export function toAltBarRecords(
  localCode: string,
  bars: YahooEquityBar[],
  splits: YahooSplitEvent[]
): AltBarRecord[] {
  return bars.map((b) => {
    const k = cumulativeSplitRatioAfter(b.date, splits);
    const raw = (v: number | null): number | null => (v == null ? null : round2(v * k));
    return {
      local_code: localCode,
      trade_date: b.date,
      open: raw(b.adjOpen),
      high: raw(b.adjHigh),
      low: raw(b.adjLow),
      close: round2(b.adjClose * k),
      volume: b.adjVolume == null ? null : Math.round(b.adjVolume / k),
      adj_open: b.adjOpen,
      adj_high: b.adjHigh,
      adj_low: b.adjLow,
      adj_close: b.adjClose,
      adj_volume: b.adjVolume,
      source: 'yahoo',
    };
  });
}

/**
 * 異常値ガード（純関数）
 *
 * 調整値の系列は分割をまたいでも連続なので、前日比 ±MAX_DAILY_MOVE 超は異常として扱う
 * （実例: 1306.T の 2026-03-30/31 が本来の1/10で配信された）。採用しなかった日は比較基準を
 * 更新しない（異常値どうしを比べて正常扱いにしない）。
 *
 * @param prevAdjClose 期間より前の最後の調整後終値（無ければ最初の日は無条件で採用）
 */
export function filterAnomalies(
  bars: YahooEquityBar[],
  prevAdjClose: number | null,
  maxMove: number = MAX_DAILY_MOVE
): { accepted: YahooEquityBar[]; rejected: RejectedBar[] } {
  const accepted: YahooEquityBar[] = [];
  const rejected: RejectedBar[] = [];
  let prev = prevAdjClose;
  for (const b of bars) {
    if (prev != null && prev > 0 && Math.abs(b.adjClose / prev - 1) > maxMove) {
      rejected.push({ date: b.date, adjClose: b.adjClose, prevAdjClose: prev });
      continue;
    }
    accepted.push(b);
    prev = b.adjClose;
  }
  return { accepted, rejected };
}

/** TOPIX 推計の材料（ETF の調整後 OHLC） */
export interface EtfDay {
  date: string;
  adjOpen: number | null;
  adjHigh: number | null;
  adjLow: number | null;
  adjClose: number;
}

/**
 * 最後の公式 TOPIX を ETF の騰落で延長した推計行を作る（純関数）
 *
 * 推計値 = TOPIX_A × ETF_t / ETF_A（A = 公式 TOPIX の最終日。OHLC もそれぞれ同じ式）。
 * A より後の日だけを返す（公式行を上書きしない）。ETF に A の値が無ければ例外（基準が作れない）。
 */
export function computeTopixProxy(
  anchor: { date: string; close: number },
  etf: EtfDay[]
): TopixBarDailyRecord[] {
  const base = etf.find((d) => d.date === anchor.date);
  if (!base || !(base.adjClose > 0)) {
    throw new Error(`TOPIX 推計の基準日 ${anchor.date} に 1306 の終値がありません`);
  }
  const f = anchor.close / base.adjClose;
  const scale = (v: number | null): number | undefined => (v == null ? undefined : round2(v * f));
  return etf
    .filter((d) => d.date > anchor.date)
    .sort((a, b) => a.date.localeCompare(b.date))
    .map((d) => ({
      trade_date: d.date,
      open: scale(d.adjOpen),
      high: scale(d.adjHigh),
      low: scale(d.adjLow),
      close: round2(d.adjClose * f),
      source: 'proxy_etf_13060' as const,
    }));
}
