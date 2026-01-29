/**
 * Cron A チャンクハンドラー: equity_bars の1ページ分を処理
 *
 * @description Vercel Hobbyの10秒制限で一括処理が失敗した場合のフォールバック。
 * GitHub Actionsからpagination_keyを渡してループ呼び出しすることで全ページを処理。
 */

import { z } from 'zod';
import { createLogger, type LogContext } from '../../utils/logger';
import { syncEquityBarsDailySinglePage } from '../../jquants/endpoints';
import { determineTargetDates } from '../catch-up';
import { createAdminClient } from '../../supabase/admin';
import { startJobRun, completeJobRun } from '../job-run';
import { updateHeartbeat } from '../heartbeat';
import type { JobName } from '../job-run';

const logger = createLogger({ module: 'cron-a-chunk' });

const JOB_NAME: JobName = 'cron_a';

/** リクエストボディのスキーマ */
export const CronAChunkRequestSchema = z.object({
  /** pagination_key（初回は省略） */
  pagination_key: z.string().max(1024).optional(),
  /** 対象日付（省略時はキャッチアップで自動決定） */
  date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'date must be YYYY-MM-DD format').optional(),
}).refine(
  (data) => !data.pagination_key || data.date,
  { message: 'date is required when pagination_key is provided', path: ['date'] }
);

export type CronAChunkRequest = z.infer<typeof CronAChunkRequestSchema>;

/** チャンク処理の結果 */
export interface CronAChunkResult {
  success: boolean;
  targetDate: string | null;
  fetched: number;
  inserted: number;
  /** 次ページのpagination_key（なければ処理完了） */
  pagination_key?: string;
  /** 全ページ処理完了したかどうか */
  done: boolean;
  error?: string;
}

/**
 * equity_bars の1ページ分を処理するハンドラー
 */
export async function handleCronAChunk(
  request: CronAChunkRequest
): Promise<CronAChunkResult> {
  const logContext: LogContext = {
    jobName: JOB_NAME,
    dataset: 'equity_bars',
  };

  logger.info('Starting chunk handler', {
    hasPaginationKey: !!request.pagination_key,
    date: request.date,
  });

  try {
    // 対象日付を決定
    let targetDate = request.date;

    if (!targetDate) {
      const supabaseIngest = createAdminClient('jquants_ingest');
      const supabaseCore = createAdminClient('jquants_core');

      const targetDates = await determineTargetDates(
        supabaseIngest,
        supabaseCore,
        JOB_NAME
      );

      if (targetDates.length === 0) {
        logger.info('No target dates to process');
        return {
          success: true,
          targetDate: null,
          fetched: 0,
          inserted: 0,
          done: true,
        };
      }

      targetDate = targetDates[0];
    }

    logger.info('Processing chunk', { targetDate, paginationKey: request.pagination_key });

    const result = await syncEquityBarsDailySinglePage(
      { date: targetDate },
      {
        logContext,
        paginationKey: request.pagination_key,
      }
    );

    const done = !result.paginationKey;

    logger.info('Chunk processed', {
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
      done,
    });

    // 全ページ処理完了時にjob_runを記録し、catch-upが処理済みと認識できるようにする
    if (done) {
      try {
        const supabaseIngest = createAdminClient('jquants_ingest');
        const startResult = await startJobRun(supabaseIngest, {
          jobName: JOB_NAME,
          targetDate,
          meta: { dataset: 'equity_bars', source: 'chunk_fallback' },
        });

        if (startResult.runId) {
          await Promise.all([
            completeJobRun(supabaseIngest, startResult.runId, 'success'),
            updateHeartbeat(supabaseIngest, {
              jobName: JOB_NAME,
              status: 'success',
              runId: startResult.runId,
              targetDate,
              meta: { dataset: 'equity_bars', source: 'chunk_fallback' },
            }),
          ]);
          logger.info('Job run recorded for chunk fallback', {
            runId: startResult.runId,
            targetDate,
          });
        }
      } catch (recordError) {
        // job_run記録失敗はデータ同期自体の成功に影響させない
        logger.error('Failed to record job run for chunk fallback', {
          error: recordError instanceof Error ? recordError.message : String(recordError),
        });
      }
    }

    return {
      success: true,
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
      pagination_key: result.paginationKey,
      done,
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error('Chunk handler failed', { error: errorMessage });

    return {
      success: false,
      targetDate: request.date ?? null,
      fetched: 0,
      inserted: 0,
      done: false,
      error: errorMessage,
    };
  }
}
