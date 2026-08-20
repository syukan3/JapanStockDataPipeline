/**
 * 追跡銘柄（保有 ∪ ウォッチリスト）の取得
 *
 * @description
 * 長期週足（analytics.equity_bar_weekly）の対象銘柄リスト。portfolio スキーマを
 * service_role で直接読む（スキーマ所有は Portfolio リポのまま。ここは読むだけ）。
 * 設計正本: ../../../docs/PLANS-longterm-log-chart-2026-08.md §4.1
 *
 * **保有の導出条件は JapanStockScouter の src/lib/fetch-holdings.ts と同一挙動にすること**:
 * - `portfolio.portfolios` はソフトデリート（00008）を除外（`deleted_at is null`）
 * - `portfolio.transactions` を local_code ごとに buy 数量 - sell 数量 で集計し、純数量 > 0 を保有とする
 * - 複数ポートフォリオは合算（同一銘柄を複数口座で持っても1銘柄）
 * - transactions 自体に deleted_at 列は無いため、親のポートフォリオでのみ絞り込む
 */

import { createAdminClient } from '../supabase/admin';

/** portfolio スキーマ束縛の supabase-js クライアント（スキーマ動的型のため any で受ける・repo 慣習） */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type PortfolioClient = any;

/** PostgREST の1リクエスト取得件数 */
const PAGE_SIZE = 1000;

interface PortfolioRow {
  id: string;
}

interface TransactionRow {
  local_code: string;
  trade_type: 'buy' | 'sell';
  quantity: number | string;
}

interface WatchlistItemRow {
  local_code: string;
}

/**
 * ページングして全行取得する。
 * `buildQuery` はページごとに新しいクエリビルダを返すこと（使い回しは PostgREST 側で不正）。
 */
async function fetchAllRows<T>(
  buildQuery: () => PortfolioClient,
  label: string
): Promise<T[]> {
  const rows: T[] = [];
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const { data, error } = await buildQuery().range(offset, offset + PAGE_SIZE - 1);
    if (error) {
      throw new Error(`${label} の取得に失敗しました: ${error.message}`);
    }
    const page = (data as T[] | null) ?? [];
    rows.push(...page);
    if (page.length < PAGE_SIZE) break;
  }
  return rows;
}

/**
 * 保有銘柄コード（純数量 > 0）を返す。
 * 未削除ポートフォリオが1件も無ければ空配列（取引を読みに行かない）。
 */
export async function fetchHeldLocalCodes(portfolio: PortfolioClient): Promise<string[]> {
  const portfolios = await fetchAllRows<PortfolioRow>(
    () => portfolio.from('portfolios').select('id').is('deleted_at', null).order('id'),
    'portfolio.portfolios'
  );
  if (portfolios.length === 0) return [];

  const portfolioIds = portfolios.map((p) => p.id);
  const transactions = await fetchAllRows<TransactionRow>(
    () =>
      portfolio
        .from('transactions')
        .select('local_code, trade_type, quantity')
        .in('portfolio_id', portfolioIds)
        .order('id'),
    'portfolio.transactions'
  );

  const net = new Map<string, number>();
  for (const transaction of transactions) {
    const quantity = Number(transaction.quantity);
    // quantity は integer NOT NULL（00001）なので通常到達しない。NaN で銘柄全体を落とさないための保険
    if (!Number.isFinite(quantity)) continue;
    const delta = transaction.trade_type === 'buy' ? quantity : -quantity;
    net.set(transaction.local_code, (net.get(transaction.local_code) ?? 0) + delta);
  }

  return [...net.entries()].filter(([, quantity]) => quantity > 0).map(([code]) => code);
}

/**
 * ウォッチリスト銘柄コードを返す（単一ユーザー運用のため user_id では絞らない）。
 */
export async function fetchWatchlistLocalCodes(portfolio: PortfolioClient): Promise<string[]> {
  const rows = await fetchAllRows<WatchlistItemRow>(
    () => portfolio.from('watchlist_items').select('local_code').order('local_code'),
    'portfolio.watchlist_items'
  );
  return [...new Set(rows.map((row) => row.local_code))];
}

/**
 * 追跡銘柄（保有 ∪ ウォッチ）の銘柄コードを重複排除・昇順で返す
 *
 * @param portfolio テスト用のクライアント注入（省略時は service_role の portfolio クライアント）
 */
export async function getTrackedLocalCodes(portfolio?: PortfolioClient): Promise<string[]> {
  const client = portfolio ?? createAdminClient('portfolio');

  const held = await fetchHeldLocalCodes(client);
  const watched = await fetchWatchlistLocalCodes(client);

  return [...new Set([...held, ...watched])].sort();
}
