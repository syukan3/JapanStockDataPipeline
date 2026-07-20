/**
 * nikkei225jp.com 取得プロキシ API Route（東京リージョン固定）
 *
 * @description GitHub Actions ランナー（米国 Azure リージョン）から nikkei225jp.com へ
 * 直接リクエストすると 2026-07-17 以降 HTTP 403 が返るようになった。手元（日本）からは
 * 同一 URL・同一 User-Agent/Referer で 200 が返るため、海外IP／データセンターIPの遮断と
 * 判断し、東京リージョンの Vercel Functions を経由して取得する。
 *
 * 403 は retry 対象外（`src/lib/utils/retry.ts` の retryStatusCodes に含めない）ため、
 * リトライ回数を増やしても復旧しない。経路自体を変える必要がある。
 *
 * セキュリティ:
 * - 取得先は `file` パラメータの enum（daily2 / dailyweek2）から解決する。任意 URL を
 *   受け取らないことで SSRF・オープンプロキシ化を構造的に防ぐ。
 * - CRON_SECRET による Bearer 認証必須（他の cron エンドポイントと同じ）。
 *
 * GET /api/proxy/nikkei225jp?file=daily2|dailyweek2
 * Headers: Authorization: Bearer <CRON_SECRET>
 * Response: 200 で上流のレスポンスボディをそのまま text/plain で返す。
 *           上流が非 2xx の場合は同じステータスコードで JSON エラーを返す。
 */

import { NextResponse } from 'next/server';
import { requireCronAuth } from '@/lib/cron/auth';
import { BROWSER_USER_AGENT } from '@/lib/market/yahoo-chart-client';
import { createLogger } from '@/lib/utils/logger';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
/**
 * 東京リージョン固定。これがこのルートの存在理由なので、プロジェクト既定リージョンの
 * 設定に依存させない（このリポジトリには vercel.json が無く既定は米国になりうる）。
 */
export const preferredRegion = 'hnd1';
export const maxDuration = 15;

const logger = createLogger({ module: 'route/proxy-nikkei225jp' });

/** 取得を許可する上流 URL。ここに無いものは一切取得しない。 */
const ALLOWED_FILES = {
  daily2: 'https://nikkei225jp.com/_data/_nfsDATA/DAY/daily2.json',
  dailyweek2: 'https://nikkei225jp.com/_data/_nfsDATA/DAY/dailyweek2.json',
} as const;

type AllowedFile = keyof typeof ALLOWED_FILES;

/** dailyweek2.json はホットリンク保護があり Referer 必須（無いと404） */
const REFERER = 'https://nikkei225jp.com/data/sinyou.php';
const UPSTREAM_TIMEOUT_MS = 10_000;

function isAllowedFile(value: string | null): value is AllowedFile {
  return value !== null && Object.prototype.hasOwnProperty.call(ALLOWED_FILES, value);
}

export async function GET(request: Request): Promise<Response> {
  const authError = requireCronAuth(request);
  if (authError) {
    return authError;
  }

  const file = new URL(request.url).searchParams.get('file');
  if (!isAllowedFile(file)) {
    return NextResponse.json(
      { error: `file must be one of: ${Object.keys(ALLOWED_FILES).join(', ')}` },
      { status: 400 }
    );
  }

  const upstreamUrl = ALLOWED_FILES[file];

  let res: Response;
  try {
    res = await fetch(upstreamUrl, {
      headers: {
        'User-Agent': BROWSER_USER_AGENT,
        Accept: '*/*',
        Referer: REFERER,
      },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
      cache: 'no-store',
    });
  } catch (error) {
    logger.error('Upstream fetch failed', { file, error });
    return NextResponse.json({ error: 'Upstream fetch failed', file }, { status: 502 });
  }

  if (!res.ok) {
    // 上流のステータスをそのまま返す。呼び出し側(fetchWithRetry)のリトライ判定を
    // 直接取得時と同じにするため、ここで 200 に丸めない。
    logger.warn('Upstream returned non-OK', { file, status: res.status });
    return NextResponse.json(
      { error: 'Upstream returned non-OK', file, status: res.status },
      { status: res.status }
    );
  }

  const body = await res.text();
  logger.info('Proxied nikkei225jp file', { file, bytes: body.length });

  return new Response(body, {
    status: 200,
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
      'Cache-Control': 'no-store',
    },
  });
}
