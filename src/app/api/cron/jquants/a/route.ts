/**
 * Cron A API Route: 日次確定データ同期
 *
 * @description GitHub Actions から呼び出される API エンドポイント
 *
 * POST /api/cron/jquants/a
 * Body: { "dataset": "calendar" | "equity_bars" | "topix" | "financial" | "equity_master" }
 * Headers: Authorization: Bearer <CRON_SECRET>
 */

import { NextResponse } from 'next/server';
import { requireCronAuth } from '@/lib/cron/auth';
import { jquantsGate } from '@/lib/cron/jquants-gate';
import { acquireLock, releaseLock } from '@/lib/cron/job-lock';
import { startJobRun, completeJobRun } from '@/lib/cron/job-run';
import { updateHeartbeat } from '@/lib/cron/heartbeat';
import { handleCronA, CronARequestSchema } from '@/lib/cron/handlers';
import { createAdminClient } from '@/lib/supabase/admin';
import { createLogger } from '@/lib/utils/logger';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 10; // Vercel Hobby 制限

const JOB_NAME = 'cron_a' as const;
const LOCK_TTL_SECONDS = 60; // 10秒制限のため余裕を持って60秒

const logger = createLogger({ module: 'route/cron-a' });

export async function POST(request: Request): Promise<Response> {
  // 1. CRON_SECRET 認証
  const authError = requireCronAuth(request);
  if (authError) {
    return authError;
  }

  // 1b. J-Quants OFF なら handler を作る前に止める（00132 のスイッチ。§3.4 最終防衛線）
  const gated = await jquantsGate('/api/cron/jquants/a');
  if (gated) {
    return gated;
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

  const parsed = CronARequestSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json(
      { error: 'Invalid request body', details: parsed.error.flatten() },
      { status: 400 }
    );
  }

  const { dataset } = parsed.data;
  const supabaseIngest = createAdminClient('jquants_ingest');

  // 3. ロック取得
  const lockResult = await acquireLock(supabaseIngest, JOB_NAME, LOCK_TTL_SECONDS);
  if (!lockResult.success) {
    logger.info('Lock not acquired', { jobName: JOB_NAME, dataset, reason: lockResult.error });
    return NextResponse.json(
      { error: 'Another job is running', detail: lockResult.error },
      { status: 409 }
    );
  }

  const lockToken = lockResult.token!;
  let runId = '';

  try {
    // 4. ジョブ実行開始（job_runs INSERT）
    const startResult = await startJobRun(supabaseIngest, {
      jobName: JOB_NAME,
      meta: { dataset },
    });

    if (startResult.error) {
      // 既に実行済みの場合など
      logger.info('Job run not started', { jobName: JOB_NAME, dataset, reason: startResult.error });
      return NextResponse.json(
        { error: 'Job already executed', detail: startResult.error },
        { status: 200 } // 冪等性を考慮して200を返す
      );
    }

    runId = startResult.runId;

    // 5. ハートビート更新（running）
    await updateHeartbeat(supabaseIngest, {
      jobName: JOB_NAME,
      status: 'running',
      runId,
      meta: { dataset },
    });

    // 6. ハンドラー実行
    logger.info('Executing Cron A handler', { runId, dataset });
    const result = await handleCronA(dataset, runId);

    // 7. ジョブ完了 & ハートビート更新（並列実行）
    const finalStatus = result.success ? 'success' : 'failed';
    await Promise.all([
      completeJobRun(supabaseIngest, runId, finalStatus, result.error),
      updateHeartbeat(supabaseIngest, {
        jobName: JOB_NAME,
        status: finalStatus,
        runId,
        targetDate: result.targetDate ?? undefined,
        error: result.error,
        meta: { dataset, fetched: result.fetched, inserted: result.inserted },
      }),
    ]);

    logger.info('Cron A completed', {
      runId,
      dataset,
      success: result.success,
      fetched: result.fetched,
      inserted: result.inserted,
    });

    // 9. レスポンス返却
    return NextResponse.json({
      success: result.success,
      runId,
      dataset,
      targetDate: result.targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
      pageCount: result.pageCount,
      error: result.error,
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Cron A failed with exception', { runId, dataset, error: errorMessage });

    // ジョブが開始されていた場合は失敗として記録（クリーンアップは並列実行、失敗しても握りつぶす）
    if (runId) {
      await Promise.allSettled([
        completeJobRun(supabaseIngest, runId, 'failed', errorMessage),
        updateHeartbeat(supabaseIngest, {
          jobName: JOB_NAME,
          status: 'failed',
          runId,
          error: errorMessage,
          meta: { dataset },
        }),
      ]);
    }

    // 本番環境ではエラー詳細を隠す
    const isDev = process.env.NODE_ENV === 'development';
    return NextResponse.json(
      {
        error: 'Internal server error',
        ...(isDev && { detail: errorMessage }),
        runId: runId || undefined,
      },
      { status: 500 }
    );
  } finally {
    // 10. ロック解放（失敗してもレスポンスには影響させない）
    try {
      await releaseLock(supabaseIngest, JOB_NAME, lockToken);
    } catch (releaseError) {
      logger.error('Failed to release lock', {
        jobName: JOB_NAME,
        lockToken,
        error: releaseError instanceof Error ? releaseError.message : String(releaseError),
      });
      // ロック解放失敗はログのみ。TTLで自動解放されるため、ここでは握りつぶす
    }
  }
}
