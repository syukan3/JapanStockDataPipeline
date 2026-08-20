/**
 * 週足日次メンテ（Cron A 新ステップ）のロジック部
 *
 * @description
 * 設計正本: ../../../docs/PLANS-longterm-log-chart-2026-08.md §4.3
 * オーケストレーションは scripts/cron/refresh-weekly-bars.ts（rebase-adjusted-bars.ts と同じ分離）。
 *
 * 処理順（順序自体が正しさの前提）:
 *   1. 追跡銘柄（保有∪ウォッチ）を取得
 *   2. 新規追跡銘柄（週足0行）を API から10年バックフィル
 *   3. 分割・併合を7日窓で検知し、台帳未記録のものを **イベント日の降順** に RPC 適用
 *   4. 直近2 ISO週の週足を equity_bar_daily から再集計して upsert（API 不要）
 *   5. weekly と daily の直近 adj_close を突合して warn（処理は落とさない）
 *
 * 3 が 4 より前なのは、後段の日足由来上書き（4）が万一の二重適用を清算する側に働くため。
 * 3 の降順は、同一 run 内に別週の2イベントがある場合に「絶対値上書きの再集計」が
 * 常に「増分適用」より後に来るようにするため（昇順だと古い週へ新イベント係数が二重に乗る）。
 */

import { chunkArray } from '../utils/batch';
import {
  subtractDays,
  DETECT_LOOKBACK_DAYS,
  type AdjustmentEvent,
  type CoreClient,
} from './rebase-adjusted-bars';
import {
  aggregateWeeklyBars,
  isoWeekStart,
  type WeeklyBarRecord,
  type WeeklyBarSourceRow,
} from './weekly-bars';
import {
  WEEKLY_BARS_TABLE,
  WEEKLY_REBASE_EVENTS_TABLE,
  type AnalyticsClient,
} from './weekly-bars-backfill';

/** 直近何 ISO週を再集計するか（週明けの取りこぼし・障害復旧の前方フィルを吸収する） */
export const RECENT_WEEKS = 2;

/** PostgREST の1リクエスト取得件数 */
export const PAGE_SIZE = 1000;

/** `.in()` に載せる銘柄コードの最大数（URL 長対策。追跡は数十銘柄想定なので通常1チャンク） */
export const CODE_CHUNK_SIZE = 100;

/** weekly と daily の adj_close 突合で警告する相対差（0.1%） */
export const ADJ_CLOSE_TOLERANCE = 0.001;

/** 日足から週足を作るのに必要な列（equity_bar_daily） */
const DAILY_COLUMNS =
  'trade_date, local_code, session, open, high, low, close, volume, turnover_value, ' +
  'adjustment_factor, adj_open, adj_high, adj_low, adj_close, adj_volume';

/** 1イベントの RPC 適用結果 */
export interface WeeklyRebaseApplyResult {
  local_code: string;
  event_date: string;
  adjustment_factor: number;
  /** RPC 戻り値。-1 = 台帳に記録済みでスキップ、それ以外は更新・置換した週足行数 */
  affected_rows: number;
}

/** 突合検証で読み戻す週足行 */
export interface WeeklyCloseRow {
  local_code: string;
  week_start: string;
  adj_close: number | string | null;
}

/** 週足と日足の adj_close が食い違った銘柄 */
export interface WeeklyDailyMismatch {
  local_code: string;
  /** weekly_row_missing = 直近週の週足行が読み戻せない / adj_close_diff = 値が乖離 */
  reason: 'weekly_row_missing' | 'adj_close_diff';
  week_start: string | null;
  weekly_adj_close: number | null;
  trade_date: string;
  daily_adj_close: number;
  /** |weekly - daily| / |daily|。比較できない場合は null */
  relative_diff: number | null;
}

/** 台帳の突き合わせキー */
export function rebaseEventKey(localCode: string, eventDate: string): string {
  return `${localCode}|${eventDate}`;
}

/**
 * 追跡銘柄に絞って分割・併合イベント（factor≠1）を検知窓内で拾う
 *
 * 窓の定義は既存 rebase-adjusted-bars.ts の `detectEventsInWindow` と同じ
 * （終端日から DETECT_LOOKBACK_DAYS 暦日遡る。当日だけ見ないのは forward-fill の
 * catch-up 中にある権利落ち日を取りこぼさないため）。違いは3点:
 *   - 追跡銘柄で SQL 側から絞る（週足の対象は数十銘柄。全銘柄を JS 側で捨てない）
 *   - session='DAY' 固定（週足の他経路と同じ基準に揃える）
 *   - **ページング**する。PostgREST は Max rows（既定1000）で黙って打ち切るため、
 *     1ページ取得のままだと権利落ちが集中した週に後続ページのイベントが落ち、
 *     再基準化されないまま7日窓を過ぎて恒久的な段差になる
 */
export async function detectTrackedEventsInWindow(
  core: CoreClient,
  codes: string[],
  endDate: string,
  lookbackDays: number = DETECT_LOOKBACK_DAYS
): Promise<AdjustmentEvent[]> {
  const startDate = subtractDays(endDate, lookbackDays);
  const rows: AdjustmentEvent[] = [];
  if (codes.length === 0) return rows;

  for (const chunk of chunkArray(codes, CODE_CHUNK_SIZE)) {
    for (let offset = 0; ; offset += PAGE_SIZE) {
      const { data, error } = await core
        .from('equity_bar_daily')
        .select('local_code, trade_date, adjustment_factor')
        .in('local_code', chunk)
        // DAY 固定。週足集計（aggregateWeeklyBars）・RPC のイベント週再集計・バックフィルの
        // 台帳シーディングが全て DAY 基準なので、検知だけ AM/PM を混ぜると経路間で
        // イベント集合がずれる。副次的に PK 前方一致 (local_code, trade_date) が一意になり、
        // LIMIT/OFFSET ページングの順序が全順序として確定する。
        .eq('session', 'DAY')
        .gte('trade_date', startDate)
        .lte('trade_date', endDate)
        .not('adjustment_factor', 'is', null)
        .neq('adjustment_factor', 1)
        .order('local_code', { ascending: true })
        .order('trade_date', { ascending: true })
        .range(offset, offset + PAGE_SIZE - 1);
      if (error) {
        throw new Error(
          `分割・併合イベントの検知に失敗しました (${startDate}..${endDate}): ${error.message}`
        );
      }
      const page = (data as AdjustmentEvent[] | null) ?? [];
      rows.push(...page);
      if (page.length < PAGE_SIZE) break;
    }
  }
  return normalizeDetectedEvents(rows);
}

/**
 * 同一 (local_code, trade_date) の複数 session 行を1イベントに正規化する
 * （rebase-adjusted-bars.ts の normalizeEvents と同規約。numeric は文字列で返り得るので数値化）。
 * 数値化できない係数は捨てる（NaN を RPC に渡すと Postgres の NaN::numeric として通ってしまう）。
 */
function normalizeDetectedEvents(rows: AdjustmentEvent[]): AdjustmentEvent[] {
  const seen = new Set<string>();
  const events: AdjustmentEvent[] = [];
  for (const row of rows) {
    const factor = Number(row.adjustment_factor);
    if (!Number.isFinite(factor)) continue;
    const key = rebaseEventKey(row.local_code, row.trade_date);
    if (seen.has(key)) continue;
    seen.add(key);
    events.push({ local_code: row.local_code, trade_date: row.trade_date, adjustment_factor: factor });
  }
  return events;
}

/**
 * 週足が1行も無い銘柄（＝新規追跡銘柄）を返す
 *
 * 銘柄ごとに1行だけ引く（PostgREST に DISTINCT が無いため。追跡は数十銘柄なので直列で十分）。
 */
export async function findCodesWithoutWeeklyBars(
  analytics: AnalyticsClient,
  codes: string[]
): Promise<string[]> {
  const missing: string[] = [];
  for (const code of codes) {
    const { data, error } = await analytics
      .from(WEEKLY_BARS_TABLE)
      .select('week_start')
      .eq('local_code', code)
      .limit(1);
    if (error) {
      throw new Error(`equity_bar_weekly の存在確認に失敗しました (${code}): ${error.message}`);
    }
    if (((data as unknown[] | null) ?? []).length === 0) missing.push(code);
  }
  return missing;
}

/**
 * 指定期間に台帳へ記録済みのイベントキー集合を返す（`local_code|event_date`）
 */
export async function fetchAppliedRebaseEventKeys(
  analytics: AnalyticsClient,
  codes: string[],
  from: string,
  to: string
): Promise<Set<string>> {
  const keys = new Set<string>();
  if (codes.length === 0) return keys;

  for (const chunk of chunkArray(codes, CODE_CHUNK_SIZE)) {
    for (let offset = 0; ; offset += PAGE_SIZE) {
      const { data, error } = await analytics
        .from(WEEKLY_REBASE_EVENTS_TABLE)
        .select('local_code, event_date')
        .in('local_code', chunk)
        .gte('event_date', from)
        .lte('event_date', to)
        .order('local_code', { ascending: true })
        .order('event_date', { ascending: true })
        .range(offset, offset + PAGE_SIZE - 1);
      if (error) {
        throw new Error(
          `equity_bar_weekly_rebase_events の取得に失敗しました (${from}..${to}): ${error.message}`
        );
      }
      const page = (data as { local_code: string; event_date: string }[] | null) ?? [];
      for (const row of page) keys.add(rebaseEventKey(row.local_code, row.event_date));
      if (page.length < PAGE_SIZE) break;
    }
  }
  return keys;
}

/**
 * 検知イベントから「追跡銘柄かつ台帳未記録」のものだけを **イベント日の降順** で返す（純関数）
 *
 * 同日内は銘柄コード昇順（実行順を決定的にするため。銘柄が違えば適用は互いに独立）。
 */
export function selectUnappliedEvents(
  events: AdjustmentEvent[],
  trackedCodes: string[],
  appliedKeys: Set<string>
): AdjustmentEvent[] {
  const tracked = new Set(trackedCodes);
  return events
    .filter((e) => tracked.has(e.local_code))
    .filter((e) => !appliedKeys.has(rebaseEventKey(e.local_code, e.trade_date)))
    .sort((a, b) => {
      if (a.trade_date !== b.trade_date) return a.trade_date < b.trade_date ? 1 : -1;
      return a.local_code < b.local_code ? -1 : a.local_code > b.local_code ? 1 : 0;
    });
}

/**
 * イベントを1件ずつ RPC `analytics.apply_weekly_rebase_event` で適用する
 *
 * 台帳記録まで含めて1イベント＝1トランザクション（00124）。**渡された順序で直列実行**するので、
 * 呼び出し側は selectUnappliedEvents の降順を崩さないこと。失敗は throw（Cron 側で失敗検知）。
 */
export async function applyWeeklyRebaseEvents(
  analytics: AnalyticsClient,
  events: AdjustmentEvent[]
): Promise<WeeklyRebaseApplyResult[]> {
  const results: WeeklyRebaseApplyResult[] = [];
  for (const event of events) {
    const { data, error } = await analytics.rpc('apply_weekly_rebase_event', {
      p_local_code: event.local_code,
      p_event_date: event.trade_date,
      p_factor: event.adjustment_factor,
    });
    if (error) {
      throw new Error(
        `apply_weekly_rebase_event failed for ${event.local_code} on ${event.trade_date}: ${error.message}`
      );
    }
    results.push({
      local_code: event.local_code,
      event_date: event.trade_date,
      adjustment_factor: event.adjustment_factor,
      affected_rows: Number(data ?? 0),
    });
  }
  return results;
}

/**
 * 直近 RECENT_WEEKS 週分の再集計対象期間の開始日（ISO週月曜）を返す
 *
 * 最新 trade_date の属する週の月曜から (RECENT_WEEKS - 1) 週遡る。
 */
export function recentWeeksStart(latestTradeDate: string, weeks: number = RECENT_WEEKS): string {
  const currentWeekStart = isoWeekStart(latestTradeDate);
  const d = new Date(`${currentWeekStart}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() - 7 * (weeks - 1));
  return d.toISOString().slice(0, 10);
}

/**
 * 追跡銘柄の日足（session='DAY'）を期間指定で取得する（コードチャンク × ページング）
 */
export async function fetchDailyBarsForCodes(
  core: CoreClient,
  codes: string[],
  from: string,
  to: string
): Promise<WeeklyBarSourceRow[]> {
  const rows: WeeklyBarSourceRow[] = [];
  if (codes.length === 0) return rows;

  for (const chunk of chunkArray(codes, CODE_CHUNK_SIZE)) {
    for (let offset = 0; ; offset += PAGE_SIZE) {
      const { data, error } = await core
        .from('equity_bar_daily')
        .select(DAILY_COLUMNS)
        .eq('session', 'DAY')
        .in('local_code', chunk)
        .gte('trade_date', from)
        .lte('trade_date', to)
        .order('local_code', { ascending: true })
        .order('trade_date', { ascending: true })
        .range(offset, offset + PAGE_SIZE - 1);
      if (error) {
        throw new Error(`equity_bar_daily の取得に失敗しました (${from}..${to}): ${error.message}`);
      }
      const page = (data as WeeklyBarSourceRow[] | null) ?? [];
      rows.push(...page);
      if (page.length < PAGE_SIZE) break;
    }
  }
  return rows;
}

/**
 * 複数銘柄混在の日足を銘柄ごとに週足へ集計する（純関数）
 *
 * 戻りは local_code 昇順 → week_start 昇順。
 */
export function aggregateWeeklyBarsByCode(bars: WeeklyBarSourceRow[]): WeeklyBarRecord[] {
  const byCode = new Map<string, WeeklyBarSourceRow[]>();
  for (const bar of bars) {
    const group = byCode.get(bar.local_code);
    if (group) group.push(bar);
    else byCode.set(bar.local_code, [bar]);
  }

  const records: WeeklyBarRecord[] = [];
  for (const codeBars of byCode.values()) {
    records.push(...aggregateWeeklyBars(codeBars));
  }
  return records.sort((a, b) => {
    if (a.local_code !== b.local_code) return a.local_code < b.local_code ? -1 : 1;
    return a.week_start < b.week_start ? -1 : a.week_start > b.week_start ? 1 : 0;
  });
}

/**
 * 突合検証用に、指定週以降の週足を読み戻す（コードチャンク × ページング）
 */
export async function fetchWeeklyClosesSince(
  analytics: AnalyticsClient,
  codes: string[],
  weekStart: string
): Promise<WeeklyCloseRow[]> {
  const rows: WeeklyCloseRow[] = [];
  if (codes.length === 0) return rows;

  for (const chunk of chunkArray(codes, CODE_CHUNK_SIZE)) {
    for (let offset = 0; ; offset += PAGE_SIZE) {
      const { data, error } = await analytics
        .from(WEEKLY_BARS_TABLE)
        .select('local_code, week_start, adj_close')
        .in('local_code', chunk)
        .gte('week_start', weekStart)
        .order('local_code', { ascending: true })
        .order('week_start', { ascending: true })
        .range(offset, offset + PAGE_SIZE - 1);
      if (error) {
        throw new Error(`equity_bar_weekly の読み戻しに失敗しました (>= ${weekStart}): ${error.message}`);
      }
      const page = (data as WeeklyCloseRow[] | null) ?? [];
      rows.push(...page);
      if (page.length < PAGE_SIZE) break;
    }
  }
  return rows;
}

/** PostgREST の numeric（文字列/数値/null）を number|null に統一する */
function toNumberOrNull(value: unknown): number | null {
  if (value == null || value === '') return null;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

/**
 * 書き込み後の週足と日足の直近 adj_close を突合し、食い違った銘柄を返す（純関数）
 *
 * 分割の検知漏れがあると片対数チャートに偽の段差が出るため（計画書 §7）、その兆候を拾う
 * 軽い安全網。日足に直近バーが無い銘柄（上場廃止・売買停止）は比較対象にしない。
 */
export function findWeeklyDailyMismatches(
  weeklyRows: WeeklyCloseRow[],
  dailyBars: WeeklyBarSourceRow[],
  tolerance: number = ADJ_CLOSE_TOLERANCE
): WeeklyDailyMismatch[] {
  // 銘柄ごとの最新日足（adj_close を持つ行のみ）
  const latestDaily = new Map<string, { trade_date: string; adj_close: number }>();
  for (const bar of dailyBars) {
    if (bar.session != null && bar.session !== 'DAY') continue;
    const adjClose = toNumberOrNull(bar.adj_close);
    if (adjClose === null) continue;
    const current = latestDaily.get(bar.local_code);
    if (!current || bar.trade_date > current.trade_date) {
      latestDaily.set(bar.local_code, { trade_date: bar.trade_date, adj_close: adjClose });
    }
  }

  // 銘柄ごとの最新週足
  const latestWeekly = new Map<string, WeeklyCloseRow>();
  for (const row of weeklyRows) {
    const current = latestWeekly.get(row.local_code);
    if (!current || row.week_start > current.week_start) {
      latestWeekly.set(row.local_code, row);
    }
  }

  const mismatches: WeeklyDailyMismatch[] = [];
  for (const localCode of [...latestDaily.keys()].sort()) {
    const daily = latestDaily.get(localCode)!;
    const weekly = latestWeekly.get(localCode);

    if (!weekly) {
      mismatches.push({
        local_code: localCode,
        reason: 'weekly_row_missing',
        week_start: null,
        weekly_adj_close: null,
        trade_date: daily.trade_date,
        daily_adj_close: daily.adj_close,
        relative_diff: null,
      });
      continue;
    }

    const weeklyClose = toNumberOrNull(weekly.adj_close);
    if (weeklyClose === null) {
      mismatches.push({
        local_code: localCode,
        reason: 'adj_close_diff',
        week_start: weekly.week_start,
        weekly_adj_close: null,
        trade_date: daily.trade_date,
        daily_adj_close: daily.adj_close,
        relative_diff: null,
      });
      continue;
    }

    const diff = Math.abs(weeklyClose - daily.adj_close);
    // 日足が0（値付かず）の時は差の有無だけで判定する（0除算を避ける）
    const relativeDiff = daily.adj_close === 0 ? (diff === 0 ? 0 : Infinity) : diff / Math.abs(daily.adj_close);
    if (relativeDiff > tolerance) {
      mismatches.push({
        local_code: localCode,
        reason: 'adj_close_diff',
        week_start: weekly.week_start,
        weekly_adj_close: weeklyClose,
        trade_date: daily.trade_date,
        daily_adj_close: daily.adj_close,
        relative_diff: relativeDiff,
      });
    }
  }
  return mismatches;
}
