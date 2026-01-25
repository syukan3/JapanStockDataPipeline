/**
 * ブラウザ用 Supabase クライアント
 *
 * @description SSR対応のSupabaseクライアントを作成
 * @see https://supabase.com/docs/guides/auth/server-side/nextjs
 */
import { createBrowserClient } from '@supabase/ssr';

export function createClient() {
  return createBrowserClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
  );
}
