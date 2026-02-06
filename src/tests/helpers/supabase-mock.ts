/**
 * Supabase チェーンモック共通ヘルパー
 *
 * @description テスト間で繰り返されるSupabaseクエリビルダーモックを共通化
 */

import { vi } from 'vitest';

/** Supabaseクエリ結果型 */
export interface SupabaseResult {
  data: unknown;
  error: { message: string; code?: string } | null;
  count?: number | null;
}

/** チェーン可能なクエリビルダーメソッド一覧 */
const CHAIN_METHODS = [
  'select',
  'insert',
  'update',
  'upsert',
  'delete',
  'eq',
  'neq',
  'gt',
  'gte',
  'lt',
  'lte',
  'in',
  'not',
  'or',
  'order',
  'limit',
  'range',
  'single',
  'maybeSingle',
] as const;

/**
 * チェーン可能なSupabaseクエリビルダーモックを生成
 *
 * 全メソッドが `mockReturnThis()` でチェーンされ、
 * `then` で結果を解決する thenable オブジェクト。
 *
 * @param result await 時に返す結果
 */
export function createSupabaseChainMock(result: SupabaseResult) {
  const chain: Record<string, ReturnType<typeof vi.fn>> = {};

  for (const method of CHAIN_METHODS) {
    chain[method] = vi.fn().mockReturnValue(chain);
  }

  // thenable にして await に対応
  chain.then = vi.fn().mockImplementation(
    (resolve: (value: SupabaseResult) => void) => resolve(result)
  );

  return chain;
}

/**
 * `from()` 付きのモックSupabaseクライアントを生成
 *
 * @param result 全クエリが返す結果
 */
export function createMockSupabase(result: SupabaseResult) {
  return {
    from: vi.fn(() => createSupabaseChainMock(result)),
    rpc: vi.fn().mockResolvedValue(result),
  };
}

/**
 * テーブル別に異なる結果を返すモックSupabaseクライアントを生成
 *
 * @param resultMap テーブル名 → 結果のマップ
 * @param defaultResult マッチしないテーブルのデフォルト結果
 */
export function createMockSupabaseMulti(
  resultMap: Record<string, SupabaseResult>,
  defaultResult: SupabaseResult = { data: null, error: null }
) {
  return {
    from: vi.fn((table: string) => {
      const result = resultMap[table] ?? defaultResult;
      return createSupabaseChainMock(result);
    }),
    rpc: vi.fn().mockResolvedValue(defaultResult),
  };
}
