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
import { createRetryingFetch } from '../utils/retry';

/**
 * 一過性エラー（ゲートウェイの瞬断）を投げ直す fetch。
 *
 * 2026-09-12 の Supabase 障害のように、504 が1回返るだけで Cron 全体が落ちるのを防ぐ。
 * insert・RPC は取りこぼしとコミット済みを区別できないため、いずれも投げ直さない。
 */
const retryingReadFetch = createRetryingFetch();

/**
 * 読み取りに加えて upsert / update も投げ直す fetch。
 *
 * これらのスキーマの UPDATE トリガーは updated_at の更新だけで、監査行を作ったり
 * 通知を飛ばしたりしない（監査・リビジョン記録を持つのは portfolio スキーマで、
 * そちらは読み取り専用の retryingReadFetch を使う）。
 */
const retryingWriteFetch = createRetryingFetch({ retryIdempotentWrites: true });

type SchemaName = 'jquants_core' | 'jquants_ingest' | 'analytics' | 'portfolio' | 'public';

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

    // portfolio は監査・リビジョン記録のトリガーを持つので、書き込みは投げ直さない
    const fetchImpl = schema === 'portfolio' ? retryingReadFetch : retryingWriteFetch;

    const options = schema === 'public'
      ? {
          auth: {
            persistSession: false,
            autoRefreshToken: false,
          },
          global: { fetch: fetchImpl },
        }
      : {
          auth: {
            persistSession: false,
            autoRefreshToken: false,
          },
          db: {
            schema,
          },
          global: { fetch: fetchImpl },
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
