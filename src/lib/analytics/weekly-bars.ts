/**
 * 日足 → 週足（ISO週）集計の純関数
 *
 * @description
 * 追跡銘柄（保有∪ウォッチ）限定の長期週足テーブル analytics.equity_bar_weekly を作る集計本体。
 * 設計正本: ../../../docs/PLANS-longterm-log-chart-2026-08.md（§4.2 / §4.3）
 *
 * **集計規約は migration 00124 の analytics.apply_weekly_rebase_event 内の SQL 集計と
 * 完全に一致させること**（分割イベント週は SQL 側が日足から再集計して行ごと置換するため、
 * 両者がずれると同一週の値が経路によって変わる）。対応は以下:
 *
 * | 列 | SQL | 本モジュール |
 * |---|---|---|
 * | open / adj_open | `(array_agg(open order by trade_date asc))[1]` | 週内最初の営業日の値（null でもそのまま） |
 * | close / adj_close | `(array_agg(close order by trade_date desc))[1]` | 週内最後の営業日の値（null でもそのまま） |
 * | high / adj_high | `max(...)` | null を無視した最大。全 null なら null |
 * | low / adj_low | `min(...)` | null を無視した最小。全 null なら null |
 * | volume / turnover_value / adj_volume | `sum(...)` | null を無視した合計。全 null なら null |
 * | adjustment_factor | `numeric_product(coalesce(factor, 1))` | 週内 factor（null は 1）の積 |
 * | week_start | `date_trunc('week', event_date)` | ISO週の月曜 |
 * | week_end | `max(trade_date)` | 週内最終営業日 |
 *
 * SQL 側は `session = 'DAY'` で絞り込むため、本モジュールも session を持つ行は DAY のみ採用する。
 */

/** YYYY-MM-DD */
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

/**
 * 集計の入力行。`EquityBarBasicRecord`（jquants/endpoints/equity-bars-daily）や
 * PostgREST の生行をそのまま渡せるよう、数値列は unknown で受けて Number() 正規化する
 * （analytics/jquants_core の numeric は PostgREST 経由で文字列になり得る）。
 */
export interface WeeklyBarSourceRow {
  trade_date: string;
  local_code: string;
  /** 未指定なら DAY とみなす。DAY 以外（AM/PM）は集計対象外 */
  session?: string | null;
  open?: unknown;
  high?: unknown;
  low?: unknown;
  close?: unknown;
  volume?: unknown;
  turnover_value?: unknown;
  adjustment_factor?: unknown;
  adj_open?: unknown;
  adj_high?: unknown;
  adj_low?: unknown;
  adj_close?: unknown;
  adj_volume?: unknown;
}

/** analytics.equity_bar_weekly の1行（ingested_at は DB default に委ねるので持たない） */
export interface WeeklyBarRecord {
  local_code: string;
  /** ISO週の月曜（PK の一部） */
  week_start: string;
  /** 週内最終営業日（金曜とは限らない） */
  week_end: string;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume: number | null;
  turnover_value: number | null;
  adjustment_factor: number;
  adj_open: number | null;
  adj_high: number | null;
  adj_low: number | null;
  adj_close: number | null;
  adj_volume: number | null;
}

/** PostgREST の numeric（文字列/数値/null/undefined 混在）を number|null に統一する */
function toNumberOrNull(value: unknown): number | null {
  if (value == null || value === '') return null;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

/**
 * YYYY-MM-DD が属する ISO週の月曜を返す（UTC 計算・ローカルタイムゾーン非依存）
 *
 * @throws 形式不正・実在しない日付
 */
export function isoWeekStart(date: string): string {
  if (!DATE_RE.test(date)) {
    throw new Error(`isoWeekStart: YYYY-MM-DD 形式で指定してください: ${date}`);
  }
  const ms = Date.parse(`${date}T00:00:00Z`);
  const d = new Date(ms);
  // V8 は 2026-02-30 のような不正日付をロールオーバーして受理するため往復で検証する
  if (Number.isNaN(ms) || d.toISOString().slice(0, 10) !== date) {
    throw new Error(`isoWeekStart: 実在しない日付です: ${date}`);
  }
  // getUTCDay(): 0=日曜 → 月曜起点のオフセットに変換
  const offset = (d.getUTCDay() + 6) % 7;
  d.setUTCDate(d.getUTCDate() - offset);
  return d.toISOString().slice(0, 10);
}

/** null を無視した最大。全 null なら null（SQL の max と同じ） */
function maxOf(values: (number | null)[]): number | null {
  let result: number | null = null;
  for (const v of values) {
    if (v === null) continue;
    if (result === null || v > result) result = v;
  }
  return result;
}

/** null を無視した最小。全 null なら null（SQL の min と同じ） */
function minOf(values: (number | null)[]): number | null {
  let result: number | null = null;
  for (const v of values) {
    if (v === null) continue;
    if (result === null || v < result) result = v;
  }
  return result;
}

/** null を無視した合計。全 null なら null（SQL の sum と同じ） */
function sumOf(values: (number | null)[]): number | null {
  let result: number | null = null;
  for (const v of values) {
    if (v === null) continue;
    result = (result ?? 0) + v;
  }
  return result;
}

/**
 * 単一銘柄の日足を ISO週で集計して週足レコード配列（week_start 昇順）を返す
 *
 * - 入力は trade_date 昇順でなくてよい（内部でソートする）
 * - session を持つ行は DAY のみ採用（SQL 側の `session = 'DAY'` に合わせる）
 * - 複数銘柄が混在した入力は呼び出し側のバグなので throw する
 *
 * @throws 複数銘柄混在・日付形式不正
 */
export function aggregateWeeklyBars(bars: WeeklyBarSourceRow[]): WeeklyBarRecord[] {
  const dayBars = bars.filter((b) => b.session == null || b.session === 'DAY');
  if (dayBars.length === 0) return [];

  const localCode = dayBars[0].local_code;
  for (const bar of dayBars) {
    if (bar.local_code !== localCode) {
      throw new Error(
        `aggregateWeeklyBars: 単一銘柄の日足のみ渡してください（${localCode} と ${bar.local_code} が混在）`
      );
    }
  }

  const sorted = [...dayBars].sort((a, b) => (a.trade_date < b.trade_date ? -1 : a.trade_date > b.trade_date ? 1 : 0));

  const groups = new Map<string, WeeklyBarSourceRow[]>();
  for (const bar of sorted) {
    const weekStart = isoWeekStart(bar.trade_date);
    const group = groups.get(weekStart);
    if (group) group.push(bar);
    else groups.set(weekStart, [bar]);
  }

  const records: WeeklyBarRecord[] = [];
  for (const [weekStart, weekBars] of groups) {
    const first = weekBars[0];
    const last = weekBars[weekBars.length - 1];

    // 週内 factor の積（null は 1 として扱う = SQL の coalesce(factor, 1)）
    let factorProduct = 1;
    for (const bar of weekBars) {
      factorProduct *= toNumberOrNull(bar.adjustment_factor) ?? 1;
    }

    records.push({
      local_code: localCode,
      week_start: weekStart,
      week_end: last.trade_date,
      open: toNumberOrNull(first.open),
      high: maxOf(weekBars.map((b) => toNumberOrNull(b.high))),
      low: minOf(weekBars.map((b) => toNumberOrNull(b.low))),
      close: toNumberOrNull(last.close),
      volume: sumOf(weekBars.map((b) => toNumberOrNull(b.volume))),
      turnover_value: sumOf(weekBars.map((b) => toNumberOrNull(b.turnover_value))),
      adjustment_factor: factorProduct,
      adj_open: toNumberOrNull(first.adj_open),
      adj_high: maxOf(weekBars.map((b) => toNumberOrNull(b.adj_high))),
      adj_low: minOf(weekBars.map((b) => toNumberOrNull(b.adj_low))),
      adj_close: toNumberOrNull(last.adj_close),
      adj_volume: sumOf(weekBars.map((b) => toNumberOrNull(b.adj_volume))),
    });
  }

  return records.sort((a, b) => (a.week_start < b.week_start ? -1 : a.week_start > b.week_start ? 1 : 0));
}
