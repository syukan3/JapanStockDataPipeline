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
  /** detect: 指定日（既定は最新 trade_date）の factor≠1 銘柄のみ / codes: 指定銘柄を強制 / all: 履歴に factor≠1 を持つ全銘柄 */
  mode: RebaseMode;
  /** detect モードの対象日（YYYY-MM-DD）。未指定なら最新 trade_date */
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

/** 指定日の分割・併合イベント（factor≠1）を検知する */
export async function detectEventsOnDate(
  core: CoreClient,
  date: string
): Promise<AdjustmentEvent[]> {
  const { data, error } = await core
    .from('equity_bar_daily')
    .select('local_code, trade_date, adjustment_factor')
    .eq('trade_date', date)
    .not('adjustment_factor', 'is', null)
    .neq('adjustment_factor', 1)
    .order('local_code', { ascending: true });
  if (error) {
    throw new Error(`Failed to detect adjustment events on ${date}: ${error.message}`);
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
