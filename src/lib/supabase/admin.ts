/**
 * サーバーサイド専用 Supabase 管理クライアント
 *
 * @description Service Role Key を使用し、RLS をバイパスして全テーブルにアクセス
 * @warning このクライアントはサーバーサイドでのみ使用すること
 *
 * NOTE: Supabase Pooler (Transaction mode, port 6543) を使用している場合、
 * prepared statements はサーバー側で自動的に無効化されます。
 * Advisory Lock や LISTEN/NOTIFY が必要な場合は Session mode (port 5432) を使用してください。
 *
 * @see https://supabase.com/docs/guides/api/api-keys
 * @see https://supabase.com/docs/guides/database/connecting-to-postgres#connection-pooler
 */
import { createClient, type SupabaseClient } from '@supabase/supabase-js';

type SchemaName = 'jquants_core' | 'jquants_ingest' | 'public';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnySupabaseClient = SupabaseClient<any, any, any>;

/**
 * クライアントキャッシュ（接続数削減・遅延初期化のため）
 */
const clientCache = new Map<SchemaName, AnySupabaseClient>();

/**
 * 環境変数を検証
 */
function validateEnv(): { url: string; key: string } {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url) {
    throw new Error('Missing env.NEXT_PUBLIC_SUPABASE_URL');
  }
  if (!key) {
    throw new Error('Missing env.SUPABASE_SERVICE_ROLE_KEY');
  }

  return { url, key };
}

/**
 * 指定スキーマ用のSupabaseクライアントを取得（遅延初期化・キャッシュ付き）
 *
 * @param schema 対象スキーマ
 * @returns Supabaseクライアント
 *
 * @example
 * ```typescript
 * const client = getSupabaseClient('jquants_core');
 * const { data } = await client.from('trading_calendar').select('*');
 * ```
 */
function getSupabaseClient(schema: SchemaName): AnySupabaseClient {
  if (!clientCache.has(schema)) {
    const { url, key } = validateEnv();

    const options = schema === 'public'
      ? {
          auth: {
            persistSession: false,
            autoRefreshToken: false,
          },
        }
      : {
          auth: {
            persistSession: false,
            autoRefreshToken: false,
          },
          db: {
            schema,
          },
        };

    clientCache.set(schema, createClient(url, key, options));
  }

  return clientCache.get(schema)!;
}

/**
 * jquants_core スキーマ用クライアントを取得
 *
 * @description データテーブル (equity_bar_daily, trading_calendar 等) へのアクセス
 */
export function getSupabaseAdmin(): AnySupabaseClient {
  return getSupabaseClient('jquants_core');
}

/**
 * jquants_ingest スキーマ用クライアントを取得
 *
 * @description ジョブ管理テーブル (job_runs, job_locks 等) へのアクセス
 */
export function getSupabaseIngest(): AnySupabaseClient {
  return getSupabaseClient('jquants_ingest');
}

/**
 * 動的にスキーマを指定してクライアントを取得
 *
 * @param schema 対象スキーマ（デフォルト: jquants_core）
 */
export function createAdminClient(schema: SchemaName = 'jquants_core'): AnySupabaseClient {
  return getSupabaseClient(schema);
}

// 後方互換性のための直接エクスポート（getter経由）
// Note: これらは遅延評価されるため、インポート時に環境変数が未設定でもエラーになりません
export const supabaseAdmin = new Proxy({} as AnySupabaseClient, {
  get(_, prop) {
    return Reflect.get(getSupabaseAdmin(), prop);
  },
});

export const supabaseIngest = new Proxy({} as AnySupabaseClient, {
  get(_, prop) {
    return Reflect.get(getSupabaseIngest(), prop);
  },
});
