/**
 * Cron D API Route: マクロ経済データ同期
 *
 * @description GitHub Actions から呼び出される API エンドポイント
 *
 * POST /api/cron/macro
 * Body: { "source": "fred" | "estat" | "all", "backfill_days": 0 }
 * Headers: Authorization: Bearer <CRON_SECRET>
 */

import { NextResponse } from 'next/server';
import { requireCronAuth } from '@/lib/cron/auth';
import { createAdminClient } from '@/lib/supabase/admin';
import { createLogger } from '@/lib/utils/logger';
import { handleCronD, CronDRequestSchema } from '@/lib/cron/handlers/macro';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 300; // マクロデータは系列数が多いため長めに設定

const JOB_NAME = 'cron-d-macro' as const;

const logger = createLogger({ module: 'route/cron-d' });

export async function POST(request: Request): Promise<Response> {
  // 1. CRON_SECRET 認証
  const authError = requireCronAuth(request);
  if (authError) {
    return authError;
  }

  // 2. リクエストボディのパースとバリデーション
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: 'Invalid JSON body' },
      { status: 400 }
    );
  }

  const parsed = CronDRequestSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json(
      { error: 'Invalid request body', details: parsed.error.flatten() },
      { status: 400 }
    );
  }

  const { source, backfill_days } = parsed.data;
  const supabaseIngest = createAdminClient('jquants_ingest');

  // 3. ジョブ実行開始（job_runs INSERT）
  const runId = crypto.randomUUID();

  const { error: insertError } = await supabaseIngest
    .from('job_runs')
    .insert({
      run_id: runId,
      job_name: JOB_NAME,
      status: 'running',
      meta: { source, backfill_days },
    });

  if (insertError) {
    logger.error('Failed to start job run', { error: insertError.message });
    return NextResponse.json(
      { error: 'Failed to start job run', detail: insertError.message },
      { status: 500 }
    );
  }

  try {
    // 4. ハンドラー実行
    logger.info('Executing Cron D handler', { runId, source, backfill_days });
    const result = await handleCronD(source, runId, backfill_days);

    // 5. ジョブ完了
    const finalStatus = result.success ? 'success' : 'failed';
    await supabaseIngest
      .from('job_runs')
      .update({
        status: finalStatus,
        finished_at: new Date().toISOString(),
        error_message: result.errors.length > 0 ? result.errors.join('; ') : null,
        meta: {
          source,
          backfill_days,
          seriesProcessed: result.seriesProcessed,
          rowsUpserted: result.rowsUpserted,
          skippedValues: result.skippedValues,
        },
      })
      .eq('run_id', runId);

    logger.info('Cron D completed', {
      runId,
      source,
      success: result.success,
      seriesProcessed: result.seriesProcessed,
      rowsUpserted: result.rowsUpserted,
    });

    return NextResponse.json({
      success: result.success,
      runId,
      source,
      seriesProcessed: result.seriesProcessed,
      rowsUpserted: result.rowsUpserted,
      skippedValues: result.skippedValues,
      errors: result.errors.length > 0 ? result.errors : undefined,
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Cron D failed with exception', { runId, source, error: errorMessage });

    // ジョブ失敗として記録
    await supabaseIngest
      .from('job_runs')
      .update({
        status: 'failed',
        finished_at: new Date().toISOString(),
        error_message: errorMessage,
      })
      .eq('run_id', runId)
      .then(() => {}, () => {}); // クリーンアップ失敗は握りつぶす

    const isDev = process.env.NODE_ENV === 'development';
    return NextResponse.json(
      {
        error: 'Internal server error',
        ...(isDev && { detail: errorMessage }),
        runId,
      },
      { status: 500 }
    );
  }
}
