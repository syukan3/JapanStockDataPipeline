/**
 * J-Quants の ON/OFF モード（00132 の ops.data_source_switches）
 *
 * @description
 * J-Quants を解約しても運用を続けるためのスイッチの読み取り口。
 * 設計正本: ../../../../docs/PLANS-jquants-off-switch-2026-10.md §3
 *
 * - 読み取りは橋渡しRPC `jquants_ingest.get_data_source_mode`（service_role 専用）。
 * - 読めないとき（RPCエラー・行なし）は例外にする。ON/OFF を推測で選ばない
 *   （ON と推測すると解約後に API を叩き、OFF と推測するとデータ取得を黙って止めてしまう）。
 * - プロセス内で短時間キャッシュする（J-Quants クライアントはページごとに確認するため）。
 */

import { createAdminClient } from '../supabase/admin';

/** モード（RPC の1行） */
export interface JQuantsMode {
  enabled: boolean;
  /** OFF にした時点の equity_bar_daily（DAY）の最大日。ON 復帰の埋め戻し起点 */
  lastOfficialTradeDate: string | null;
  /** OFF 中に dispatch・未達検知を止めるワークフロー（ON 中は空） */
  skippedWorkflows: string[];
  changedAt: string | null;
  reason: string | null;
}

/** OFF 中に J-Quants を呼ぼうとしたときの例外 */
export class JQuantsDisabledError extends Error {
  constructor(context?: string) {
    super(
      `J-Quants は OFF です（ops.data_source_switches）${context ? `: ${context}` : ''}。` +
        ' 再開は scripts/ops/jquants-toggle.sh on'
    );
    this.name = 'JQuantsDisabledError';
  }
}

interface ModeRow {
  enabled: boolean;
  last_official_trade_date: string | null;
  skipped_workflows: string[] | null;
  changed_at: string | null;
  reason: string | null;
}

/** キャッシュの有効期間。Vercel の常駐プロセスでも切替が1分以内に効くようにする */
const CACHE_TTL_MS = 60_000;

let cache: { mode: JQuantsMode; fetchedAt: number } | null = null;

/** RPC の結果をモードへ変換する（純関数・テスト対象） */
export function parseModeRows(rows: unknown): JQuantsMode {
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error(
      'J-Quants モードが取得できません（ops.data_source_switches に jquants 行が無い。00132 未適用の可能性）'
    );
  }
  const row = rows[0] as ModeRow;
  if (typeof row.enabled !== 'boolean') {
    throw new Error(`J-Quants モードの enabled が不正です: ${JSON.stringify(row.enabled)}`);
  }
  return {
    enabled: row.enabled,
    lastOfficialTradeDate: row.last_official_trade_date ?? null,
    skippedWorkflows: row.skipped_workflows ?? [],
    changedAt: row.changed_at ?? null,
    reason: row.reason ?? null,
  };
}

/**
 * 現在のモードを返す
 *
 * @param options.fresh true ならキャッシュを使わない（切替コマンド・GH Actions の判定ステップ用）
 */
export async function getJQuantsMode(options?: { fresh?: boolean }): Promise<JQuantsMode> {
  const now = Date.now();
  if (!options?.fresh && cache && now - cache.fetchedAt < CACHE_TTL_MS) {
    return cache.mode;
  }

  const ingest = createAdminClient('jquants_ingest');
  const { data, error } = await ingest.rpc('get_data_source_mode', { p_provider: 'jquants' });
  if (error) {
    throw new Error(`J-Quants モードの取得に失敗しました: ${error.message}`);
  }

  const mode = parseModeRows(data);
  cache = { mode, fetchedAt: now };
  return mode;
}

/** OFF なら JQuantsDisabledError を投げる */
export async function assertJQuantsEnabled(context?: string): Promise<void> {
  const mode = await getJQuantsMode();
  if (!mode.enabled) {
    throw new JQuantsDisabledError(context);
  }
}

/** テスト用: キャッシュを消す */
export function resetJQuantsModeCache(): void {
  cache = null;
}
