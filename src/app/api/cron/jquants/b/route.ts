/**
 * Cron B API Route: 決算発表予定同期
 *
 * @description GitHub Actions から呼び出される API エンドポイント
 *
 * POST /api/cron/jquants/b
 * Headers: Authorization: Bearer <CRON_SECRET>
 */

import { NextResponse } from 'next/server';
import { requireCronAuth } from '@/lib/cron/auth';
import { acquireLock, releaseLock } from '@/lib/cron/job-lock';
import { startJobRun, completeJobRun } from '@/lib/cron/job-run';
import { getNextBusinessDay } from '@/lib/cron/business-day';
import { handleCronB } from '@/lib/cron/handlers';
import { createAdminClient } from '@/lib/supabase/admin';
import { createLogger } from '@/lib/utils/logger';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 10; // Vercel Hobby 制限

const JOB_NAME = 'cron_b' as const;
const LOCK_TTL_SECONDS = 60;
const SUCCESS_REOBSERVE_SECONDS = 6 * 60 * 60;

const logger = createLogger({ module: 'route/cron-b' });

export async function POST(request: Request): Promise<Response> {
  // 1. CRON_SECRET 認証
  const authError = requireCronAuth(request);
  if (authError) {
    return authError;
  }

  // 2. coverage 対象日を trading_calendar から先に確定する。
  // 取得失敗と「次営業日なし」を区別できない getNextBusinessDay の契約上、
  // null はどちらでも fail closed とする。
  let targetDate: string;
  try {
    const supabaseCore = createAdminClient('jquants_core');
    const nextBusinessDay = await getNextBusinessDay(supabaseCore);
    if (!nextBusinessDay) {
      throw new Error('Next business day could not be determined');
    }
    targetDate = nextBusinessDay;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error('Failed to determine Cron B target date', { error: errorMessage });
    return NextResponse.json(
      { error: 'Failed to determine target date' },
      { status: 500 }
    );
  }

  const supabaseIngest = createAdminClient('jquants_ingest');

  // 3. ロック取得
  const lockResult = await acquireLock(supabaseIngest, JOB_NAME, LOCK_TTL_SECONDS);
  if (!lockResult.success) {
    logger.info('Lock not acquired', { jobName: JOB_NAME, reason: lockResult.error });
    return NextResponse.json(
      { error: 'Another job is running', detail: lockResult.error },
      { status: 409 }
    );
  }

  const lockToken = lockResult.token!;
  let runId = '';
  let attemptId = '';

  try {
    // 4. ジョブ実行開始（job_runs INSERT）
    const startResult = await startJobRun(supabaseIngest, {
      jobName: JOB_NAME,
      targetDate,
      reclaimStaleAfterSeconds: LOCK_TTL_SECONDS,
      reclaimSuccessAfterSeconds: SUCCESS_REOBSERVE_SECONDS,
      coverageDataset: 'earnings_calendar',
    });

    if (startResult.error) {
      logger.info('Job run not started', { jobName: JOB_NAME, reason: startResult.error });
      return NextResponse.json(
        {
          error: startResult.alreadyExecuted
            ? 'Job already executed'
            : 'Failed to start job run',
          detail: startResult.error,
        },
        { status: startResult.alreadyExecuted ? 200 : 500 }
      );
    }

    runId = startResult.runId;
    attemptId = startResult.attemptId ?? '';
    if (!attemptId) {
      throw new Error('Atomic Cron B claim did not return an attempt ID');
    }

    // 5. claim RPCがfailed coverage fenceとrunning heartbeatを同時確定済み。
    // 6. ハンドラー実行
    logger.info('Executing Cron B handler', { runId, targetDate });
    const result = await handleCronB(runId, targetDate, attemptId);

    // 7. 現在のattemptだけがジョブを完了できる。supersededされたworkerは
    // final heartbeatも更新せず、後続attemptの監視状態を上書きしない。
    const finalStatus = result.success ? 'success' : 'failed';
    const completion = await completeJobRun(
      supabaseIngest,
      runId,
      finalStatus,
      result.error,
      attemptId,
      { fetched: result.fetched, inserted: result.inserted }
    );
    if (!completion.completed) {
      if (completion.reason === 'db_error') {
        logger.error('Failed to persist Cron B completion', {
          runId,
          attemptId,
          targetDate,
          error: completion.error,
        });
        return NextResponse.json(
          { error: 'Failed to complete job run', detail: completion.error, runId },
          { status: 500 }
        );
      }
      logger.warn('Cron B attempt was superseded before job completion', {
        runId,
        attemptId,
        targetDate,
      });
      return NextResponse.json(
        { error: 'Job attempt superseded', runId },
        { status: 409 }
      );
    }
    logger.info('Cron B completed', {
      runId,
      targetDate,
      success: result.success,
      announcementDate: result.announcementDate,
      fetched: result.fetched,
      inserted: result.inserted,
    });

    // 8. レスポンス返却
    return NextResponse.json(
      {
        success: result.success,
        runId,
        announcementDate: result.announcementDate,
        fetched: result.fetched,
        inserted: result.inserted,
        error: result.error,
      },
      { status: result.success ? 200 : 500 }
    );
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Cron B failed with exception', { runId, error: errorMessage });

    // current attemptだけを失敗完了し、heartbeatも同じRPCで更新する。
    if (runId && attemptId) {
      const completion = await completeJobRun(
        supabaseIngest,
        runId,
        'failed',
        errorMessage,
        attemptId,
        {}
      );
      if (!completion.completed) {
        logger.warn('Cron B exception completion was not persisted', {
          runId,
          attemptId,
          targetDate,
          reason: completion.reason,
          error: completion.error,
        });
      }
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
    // 9. ロック解放（失敗してもレスポンスには影響させない）
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
