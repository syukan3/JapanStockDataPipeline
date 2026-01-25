/**
 * Cron 認証
 *
 * @description CRON_SECRET を使用した認証
 */

import { timingSafeEqual } from 'crypto';
import { createLogger } from '../utils/logger';

/**
 * タイミングセーフな文字列比較
 *
 * @description timing attack を防ぐため、一定時間で比較を完了する
 */
function secureCompare(a: string, b: string): boolean {
  if (a.length !== b.length) {
    return false;
  }
  return timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

const logger = createLogger({ module: 'cron-auth' });

export interface AuthResult {
  /** 認証成功 */
  success: boolean;
  /** エラーメッセージ */
  error?: string;
}

/**
 * Cron リクエストの認証を検証
 *
 * @param request Request オブジェクト
 * @returns 認証結果
 *
 * @example
 * ```typescript
 * const auth = verifyCronAuth(request);
 * if (!auth.success) {
 *   return Response.json({ error: auth.error }, { status: 401 });
 * }
 * ```
 */
export function verifyCronAuth(request: Request): AuthResult {
  const cronSecret = process.env.CRON_SECRET;

  if (!cronSecret) {
    logger.error('CRON_SECRET environment variable is not set');
    return {
      success: false,
      error: 'Server configuration error',
    };
  }

  const authHeader = request.headers.get('authorization');

  if (!authHeader) {
    logger.warn('Missing Authorization header', {
      path: new URL(request.url).pathname,
    });
    return {
      success: false,
      error: 'Missing Authorization header',
    };
  }

  // Bearer トークン形式をチェック
  if (!authHeader.startsWith('Bearer ')) {
    logger.warn('Invalid Authorization header format', {
      path: new URL(request.url).pathname,
    });
    return {
      success: false,
      error: 'Invalid Authorization header format',
    };
  }

  const token = authHeader.slice(7); // "Bearer " を除去

  if (!secureCompare(token, cronSecret)) {
    logger.warn('Invalid CRON_SECRET', {
      path: new URL(request.url).pathname,
    });
    return {
      success: false,
      error: 'Unauthorized',
    };
  }

  logger.debug('Cron authentication successful', {
    path: new URL(request.url).pathname,
  });

  return { success: true };
}

/**
 * 認証エラーレスポンスを作成
 */
export function createUnauthorizedResponse(error: string = 'Unauthorized'): Response {
  return Response.json(
    { error },
    {
      status: 401,
      headers: {
        'WWW-Authenticate': 'Bearer',
      },
    }
  );
}

/**
 * Cron リクエストを認証し、失敗時はレスポンスを返す
 *
 * @param request Request オブジェクト
 * @returns 認証失敗時は Response、成功時は null
 *
 * @example
 * ```typescript
 * export async function POST(request: Request) {
 *   const authError = requireCronAuth(request);
 *   if (authError) return authError;
 *   // 処理を続行...
 * }
 * ```
 */
export function requireCronAuth(request: Request): Response | null {
  const auth = verifyCronAuth(request);

  if (!auth.success) {
    return createUnauthorizedResponse(auth.error);
  }

  return null;
}
