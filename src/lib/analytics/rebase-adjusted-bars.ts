/**
 * adj_* 再基準化エンジンのロジック部（scripts/cron/rebase-adjusted-bars.ts から使用）
 *
 * @description
 * 株式分割・併合（adjustment_factor ≠ 1）を検知し、対象銘柄の全履歴 adj_* を
 * SQL 関数 jquants_core.rebase_adjusted_bars(local_code) で再計算させる。
 * 検知・引数パース・オーケストレーションを純粋な関数に分離し、単体テスト可能にする。
 */

/** 実行モード */
export type RebaseMode = 'detect' | 'codes' | 'all';

export interface RebaseArgs {
  /** detect: 検知窓（終端日から DETECT_LOOKBACK_DAYS 遡る）の factor≠1 銘柄のみ / codes: 指定銘柄を強制 / all: 履歴に factor≠1 を持つ全銘柄 */
  mode: RebaseMode;
  /** detect モードの検知窓の終端日（YYYY-MM-DD）。未指定なら最新 trade_date */
  date?: string;
  /** codes モードの対象銘柄 */
  codes?: string[];
  /** 検知結果の列挙のみ（RPC を呼ばない） */
  dryRun: boolean;
}

/** 分割・併合イベント（equity_bar_daily の factor≠1 行） */
export interface AdjustmentEvent {
  local_code: string;
  trade_date: string;
  adjustment_factor: number;
}

/** 銘柄ごとの rebase 実行結果 */
export interface RebaseResult {
  local_code: string;
  updated_rows: number;
}

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

/**
 * 既定検知の lookback 窓（暦日）。
 * Cron A は forward-fill 方式で障害後に複数日を一括取り込みするため、
 * 最新日だけの走査では catch-up 範囲の途中にある権利落ち日を見逃す。
 * 窓で拾い直しても rebase は冪等（値が変わらなければ0行更新）なので再検知は無害。
 */
export const DETECT_LOOKBACK_DAYS = 7;

/**
 * CLI 引数をパースする（process.argv.slice(2) を渡す）
 *
 * - `--code` と `--all` は排他
 * - `--date` は検知モード専用（--code/--all と併用不可）
 */
export function parseRebaseArgs(argv: string[]): RebaseArgs {
  let date: string | undefined;
  let codes: string[] | undefined;
  let all = false;
  let dryRun = false;

  for (const arg of argv) {
    if (arg === '--dry-run') {
      dryRun = true;
    } else if (arg === '--all') {
      all = true;
    } else if (arg.startsWith('--date=')) {
      // 検知窓の終端日（この日から DETECT_LOOKBACK_DAYS 遡って検知する）
      const value = arg.slice('--date='.length);
      if (!DATE_RE.test(value) || Number.isNaN(Date.parse(`${value}T00:00:00Z`))) {
        throw new Error(`--date は YYYY-MM-DD 形式で指定してください: ${value}`);
      }
      date = value;
    } else if (arg.startsWith('--code=')) {
      const values = arg
        .slice('--code='.length)
        .split(',')
        .map((c) => c.trim())
        .filter((c) => c.length > 0);
      if (values.length === 0) {
        throw new Error('--code に銘柄コードが指定されていません（例: --code=31100,72360）');
      }
      codes = [...new Set(values)];
    } else {
      throw new Error(`不明な引数です: ${arg}`);
    }
  }

  if (codes && all) {
    throw new Error('--code と --all は併用できません');
  }
  if (date && (codes || all)) {
    throw new Error('--date は検知モード専用です（--code / --all と併用不可）');
  }

  if (codes) return { mode: 'codes', codes, dryRun };
  if (all) return { mode: 'all', dryRun };
  return { mode: 'detect', date, dryRun };
}

/** supabase-js クライアント（スキーマ jquants_core）。テスト容易性のため最小型で受ける */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type CoreClient = any;

/** 最新 trade_date を取得する */
export async function getLatestTradeDate(core: CoreClient): Promise<string> {
  const { data, error } = await core
    .from('equity_bar_daily')
    .select('trade_date')
    .order('trade_date', { ascending: false })
    .limit(1)
    .single();
  if (error || !data) {
    throw new Error(`Failed to get latest trade_date: ${error?.message ?? 'no rows'}`);
  }
  return (data as { trade_date: string }).trade_date;
}

/** 暦日ベースで days 日前の日付（YYYY-MM-DD）を返す */
export function subtractDays(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

/**
 * 終端日から lookbackDays 遡る窓内の分割・併合イベント（factor≠1）を検知する。
 * 単一日走査だと forward-fill の catch-up（複数日一括取り込み）中の権利落ち日を
 * 見逃すため、窓で走査する。部分インデックス前提で安価。
 * 翌日以降の実行で同じイベントが再検知されるのは冪等（0行更新）なので許容。
 */
export async function detectEventsInWindow(
  core: CoreClient,
  endDate: string,
  lookbackDays: number = DETECT_LOOKBACK_DAYS
): Promise<AdjustmentEvent[]> {
  const startDate = subtractDays(endDate, lookbackDays);
  const { data, error } = await core
    .from('equity_bar_daily')
    .select('local_code, trade_date, adjustment_factor')
    .gte('trade_date', startDate)
    .lte('trade_date', endDate)
    .not('adjustment_factor', 'is', null)
    .neq('adjustment_factor', 1)
    .order('local_code', { ascending: true })
    .order('trade_date', { ascending: true });
  if (error) {
    throw new Error(
      `Failed to detect adjustment events in ${startDate}..${endDate}: ${error.message}`
    );
  }
  return normalizeEvents((data as AdjustmentEvent[] | null) ?? []);
}

/**
 * 履歴に factor≠1 を一度でも持つ全イベントを検知する（バックフィル用）。
 * 部分インデックス idx_equity_bar_adjustment_events 前提の全期間走査。
 */
export async function detectEventsAll(core: CoreClient): Promise<AdjustmentEvent[]> {
  const events: AdjustmentEvent[] = [];
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await core
      .from('equity_bar_daily')
      .select('local_code, trade_date, adjustment_factor')
      .not('adjustment_factor', 'is', null)
      .neq('adjustment_factor', 1)
      .order('local_code', { ascending: true })
      .order('trade_date', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) {
      throw new Error(`Failed to detect adjustment events (all): ${error.message}`);
    }
    const rows = (data as AdjustmentEvent[] | null) ?? [];
    events.push(...rows);
    if (rows.length < PAGE) break;
  }
  return normalizeEvents(events);
}

/** 同一 (local_code, trade_date) の複数 session 行を1イベントに正規化する */
function normalizeEvents(rows: AdjustmentEvent[]): AdjustmentEvent[] {
  const seen = new Set<string>();
  const events: AdjustmentEvent[] = [];
  for (const row of rows) {
    const key = `${row.local_code}:${row.trade_date}`;
    if (seen.has(key)) continue;
    seen.add(key);
    events.push({
      local_code: row.local_code,
      trade_date: row.trade_date,
      // numeric 列は PostgREST 経由で文字列になり得るため数値化
      adjustment_factor: Number(row.adjustment_factor),
    });
  }
  return events;
}

/** イベント列から対象銘柄コードを重複排除して返す（出現順を維持） */
export function uniqueCodes(events: AdjustmentEvent[]): string[] {
  return [...new Set(events.map((e) => e.local_code))];
}

/**
 * 対象銘柄を順次 rebase する（RPC: jquants_core.rebase_adjusted_bars）。
 * 対象は分割検知銘柄のみで少数のため直列実行。失敗は throw（Cron 側で失敗検知させる）。
 */
export async function rebaseCodes(
  core: CoreClient,
  codes: string[]
): Promise<RebaseResult[]> {
  const results: RebaseResult[] = [];
  for (const code of codes) {
    const { data, error } = await core.rpc('rebase_adjusted_bars', { p_local_code: code });
    if (error) {
      throw new Error(`rebase_adjusted_bars failed for ${code}: ${error.message}`);
    }
    results.push({ local_code: code, updated_rows: Number(data ?? 0) });
  }
  return results;
}
