/**
 * Server Component / Route Handler 用 Supabase クライアント
 *
 * @description Next.js App Router の Server Component, Server Actions, Route Handlers で使用
 * @see https://supabase.com/docs/guides/auth/server-side/nextjs
 *
 * 重要: Server側では getSession() ではなく getUser() を使用すること
 * getSession() は受け取った Auth cookie を検証しないため、セキュリティリスクがある
 */
import { createServerClient } from '@supabase/ssr';
import { cookies } from 'next/headers';

/**
 * Server Component / Route Handler 用のSupabaseクライアントを作成
 *
 * @example
 * ```typescript
 * // Server Component
 * import { createClient } from '@/lib/supabase/server';
 *
 * export default async function Page() {
 *   const supabase = await createClient();
 *   const { data: { user } } = await supabase.auth.getUser();
 *   // ...
 * }
 * ```
 *
 * @example
 * ```typescript
 * // Route Handler
 * import { createClient } from '@/lib/supabase/server';
 * import { NextResponse } from 'next/server';
 *
 * export async function GET() {
 *   const supabase = await createClient();
 *   const { data: { user } } = await supabase.auth.getUser();
 *   // ...
 * }
 * ```
 */
export async function createClient() {
  const cookieStore = await cookies();

  return createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return cookieStore.getAll();
        },
        setAll(cookiesToSet) {
          try {
            cookiesToSet.forEach(({ name, value, options }) =>
              cookieStore.set(name, value, options)
            );
          } catch {
            // Server Component から呼ばれた場合、cookie の設定は無視される
            // これは正常な動作（refresh token の更新は Middleware で行う）
          }
        },
      },
    }
  );
}
