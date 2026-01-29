/**
 * Cron A Chunk API Route: equity_bars 分割処理
 *
 * @description equity_bars の1ページ分を処理するエンドポイント。
 * 一括処理が Vercel Hobby の10秒制限で失敗した場合のフォールバック。
 *
 * NOTE: このルートはロック/ジョブ管理を行わない。
 * GitHub Actions の cron-a.yml から、親ルート（/api/cron/jquants/a）の
 * equity_bars 一括処理が失敗した場合のみ呼ばれる設計。
 * 親ルート側でロック取得・ジョブ記録を行うため、ここでは認証のみ実施。
 *
 * POST /api/cron/jquants/a/chunk
 * Body: { "pagination_key"?: string, "date"?: string }
 * Headers: Authorization: Bearer <CRON_SECRET>
 */

import { NextResponse } from 'next/server';
import { requireCronAuth } from '@/lib/cron/auth';
import { handleCronAChunk, CronAChunkRequestSchema } from '@/lib/cron/handlers';
import { createLogger } from '@/lib/utils/logger';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 10; // Vercel Hobby 制限

const logger = createLogger({ module: 'route/cron-a-chunk' });

export async function POST(request: Request): Promise<Response> {
  // 1. CRON_SECRET 認証
  const authError = requireCronAuth(request);
  if (authError) {
    return authError;
  }

  // 2. リクエストボディのパースとバリデーション
  let body: unknown;
  try {
    const rawText = await request.text();
    body = rawText.trim() === '' ? {} : JSON.parse(rawText);
  } catch {
    return NextResponse.json(
      { error: 'Invalid JSON in request body' },
      { status: 400 }
    );
  }

  const parsed = CronAChunkRequestSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json(
      { error: 'Invalid request body', details: parsed.error.flatten() },
      { status: 400 }
    );
  }

  try {
    logger.info('Executing chunk handler', {
      hasPaginationKey: !!parsed.data.pagination_key,
      date: parsed.data.date,
    });

    const result = await handleCronAChunk(parsed.data);

    logger.info('Chunk completed', {
      success: result.success,
      targetDate: result.targetDate ?? undefined,
      fetched: result.fetched,
      inserted: result.inserted,
      done: result.done,
    });

    if (!result.success) {
      const isDev = process.env.NODE_ENV === 'development';
      return NextResponse.json(
        { ...result, error: isDev ? result.error : 'Processing failed' },
        { status: 500 }
      );
    }

    return NextResponse.json(result);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error('Chunk route failed', { error: errorMessage });

    const isDev = process.env.NODE_ENV === 'development';
    return NextResponse.json(
      {
        error: 'Internal server error',
        ...(isDev && { detail: errorMessage }),
      },
      { status: 500 }
    );
  }
}
