/**
 * バッチ処理ユーティリティ
 *
 * @description 大量データをバッチ分割して upsert
 * @see Supabase 1MB/リクエスト制限考慮
 */

import type { SupabaseClient } from '@supabase/supabase-js';

/**
 * テーブル別バッチサイズ最適化
 * Supabase 1MB/リクエスト制限を考慮（JSONオーバーヘッド含め750KB未満に設定）
 */
const BATCH_SIZES: Record<string, number> = {
  equity_bar_daily: 500,           // ~1.5KB/行 × 500 = 750KB
  equity_master_snapshot: 250,     // ~3KB/行 × 250 = 750KB
  investor_type_trading: 1000,     // ~0.7KB/行 × 1000 = 700KB
  financial_disclosure: 300,       // ~2KB/行 × 300 = 600KB
  earnings_calendar: 700,          // ~1KB/行 × 700 = 700KB
  trading_calendar: 1500,          // ~0.5KB/行 × 1500 = 750KB
  topix_bar_daily: 1500,           // ~0.5KB/行 × 1500 = 750KB
};

const DEFAULT_BATCH_SIZE = 500;

export interface BatchUpsertOptions {
  /** 上書きするバッチサイズ */
  batchSize?: number;
  /** エラー時に続行するか（デフォルト: false） */
  continueOnError?: boolean;
  /** 各バッチ処理後のコールバック */
  onBatchComplete?: (batchIndex: number, inserted: number, total: number) => void;
}

export interface BatchUpsertResult {
  /** 挿入/更新された行数 */
  inserted: number;
  /** 発生したエラー */
  errors: Error[];
  /** 処理したバッチ数 */
  batchCount: number;
}

/**
 * 配列を指定サイズのチャンクに分割
 */
export function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

/**
 * バッチ分割して upsert を実行
 *
 * @param supabase Supabaseクライアント
 * @param table テーブル名（スキーマ.テーブル名 形式可）
 * @param data 挿入/更新するデータ
 * @param onConflict ON CONFLICT で使用するカラム（カンマ区切り）
 * @param options オプション
 *
 * @example
 * ```typescript
 * const result = await batchUpsert(
 *   supabaseAdmin,
 *   'equity_bar_daily',
 *   records,
 *   'local_code,trade_date,session',
 *   { onBatchComplete: (i, n, t) => console.log(`Batch ${i}: ${n}/${t}`) }
 * );
 * ```
 */
export async function batchUpsert<T extends Record<string, unknown>>(
  supabase: SupabaseClient,
  table: string,
  data: T[],
  onConflict: string,
  options?: BatchUpsertOptions
): Promise<BatchUpsertResult> {
  if (data.length === 0) {
    return { inserted: 0, errors: [], batchCount: 0 };
  }

  // テーブル名から最適なバッチサイズを選択
  const tableName = table.split('.').pop() ?? table;
  const batchSize = options?.batchSize ?? BATCH_SIZES[tableName] ?? DEFAULT_BATCH_SIZE;
  const continueOnError = options?.continueOnError ?? false;

  const chunks = chunkArray(data, batchSize);
  let inserted = 0;
  const errors: Error[] = [];

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];

    try {
      const { error, count } = await supabase
        .from(table)
        .upsert(chunk, {
          onConflict,
          count: 'exact',
        });

      if (error) {
        const batchError = new Error(
          `Batch ${i + 1}/${chunks.length} failed: ${error.message}`
        );
        errors.push(batchError);

        if (!continueOnError) {
          throw batchError;
        }
      } else {
        inserted += count ?? chunk.length;
      }

      if (options?.onBatchComplete) {
        options.onBatchComplete(i + 1, inserted, data.length);
      }
    } catch (error) {
      if (!continueOnError) {
        throw error;
      }
      errors.push(error as Error);
    }
  }

  return {
    inserted,
    errors,
    batchCount: chunks.length,
  };
}

/**
 * バッチ分割して SELECT を実行（大量データ取得用）
 *
 * @param supabase Supabaseクライアント
 * @param table テーブル名
 * @param options オプション
 */
export async function batchSelect<T>(
  supabase: SupabaseClient,
  table: string,
  options?: {
    pageSize?: number;
    maxPages?: number;
    columns?: string;
    filter?: { column: string; operator: string; value: unknown };
    orderBy?: { column: string; ascending?: boolean };
  }
): Promise<T[]> {
  const pageSize = options?.pageSize ?? 1000;
  const maxPages = options?.maxPages ?? 100;
  const columns = options?.columns ?? '*';
  const results: T[] = [];

  for (let page = 0; page < maxPages; page++) {
    const from = page * pageSize;
    const to = from + pageSize - 1;

    let queryBuilder = supabase.from(table).select(columns);

    // フィルター適用
    if (options?.filter) {
      const { column, operator, value } = options.filter;
      switch (operator) {
        case 'eq':
          queryBuilder = queryBuilder.eq(column, value);
          break;
        case 'gte':
          queryBuilder = queryBuilder.gte(column, value);
          break;
        case 'lte':
          queryBuilder = queryBuilder.lte(column, value);
          break;
        case 'gt':
          queryBuilder = queryBuilder.gt(column, value);
          break;
        case 'lt':
          queryBuilder = queryBuilder.lt(column, value);
          break;
      }
    }

    // ソート適用
    if (options?.orderBy) {
      queryBuilder = queryBuilder.order(options.orderBy.column, {
        ascending: options.orderBy.ascending ?? true,
      });
    }

    const { data, error } = await queryBuilder.range(from, to);

    if (error) {
      throw new Error(`Batch select failed at page ${page}: ${error.message}`);
    }

    if (!data || data.length === 0) {
      break;
    }

    results.push(...(data as T[]));

    if (data.length < pageSize) {
      break;
    }
  }

  return results;
}

/**
 * 非同期処理をバッチ並列実行
 *
 * @param items 処理対象の配列
 * @param fn 各アイテムに対する処理関数
 * @param concurrency 同時実行数（デフォルト: 5）
 */
export async function batchProcess<T, R>(
  items: T[],
  fn: (item: T, index: number) => Promise<R>,
  concurrency: number = 5
): Promise<R[]> {
  const results: R[] = [];
  const chunks = chunkArray(items, concurrency);

  for (const chunk of chunks) {
    const chunkResults = await Promise.all(
      chunk.map((item, i) => fn(item, results.length + i))
    );
    results.push(...chunkResults);
  }

  return results;
}
