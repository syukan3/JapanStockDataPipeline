/**
 * Cron B ハンドラー: 決算発表予定同期
 *
 * @description 翌営業日の決算発表予定を取得
 * - APIは常に「翌営業日」分を返す
 * - 土日祝も起動して問題なし（金曜失敗→土日リカバリ可能）
 */

import { createLogger, type LogContext } from '../../utils/logger';
import { sendJobFailureEmail } from '../../notification/email';
import { getSupabaseAdmin } from '../../supabase/admin';

// エンドポイント関数（バレルファイル経由を避け直接インポート）
import {
  EarningsCalendarSyncError,
  syncEarningsCalendar,
} from '../../jquants/endpoints/earnings-calendar';

// Cronユーティリティ
import type { JobName } from '../job-run';

const logger = createLogger({ module: 'cron-b' });

/** Cron B の結果 */
export interface CronBResult {
  /** 成功フラグ */
  success: boolean;
  /** 決算発表日（翌営業日） */
  announcementDate: string | null;
  /** 取得件数 */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** エラーメッセージ */
  error?: string;
}

/** ジョブ名 */
const JOB_NAME: JobName = 'cron_b';

async function failCoverageAttempt(options: {
  targetDate: string;
  rowCount: number;
  errorCount: number;
  sourceObservedAt: string | null;
  runId: string;
  attemptId: string;
}): Promise<boolean> {
  const supabase = getSupabaseAdmin();
  const { data, error } = await supabase.rpc('fail_earnings_coverage_attempt', {
    p_target_date: options.targetDate,
    p_run_id: options.runId,
    p_attempt_id: options.attemptId,
    p_row_count: options.rowCount,
    p_error_count: options.errorCount,
    p_source_observed_at: options.sourceObservedAt,
  });

  if (error) {
    throw new Error(`Failed to persist failed dataset coverage: ${error.message}`);
  }
  return data === true;
}

/**
 * Cron B メインハンドラー
 *
 * @param runId 実行ID（ログ用）
 * @param targetDate trading_calendar から決定した取得対象日
 * @param attemptId reclaimごとに更新されるfencing token
 */
export async function handleCronB(
  runId: string,
  targetDate: string,
  attemptId: string
): Promise<CronBResult> {
  const logContext: LogContext = {
    jobName: JOB_NAME,
    runId,
  };

  logger.info('Starting Cron B handler', { runId, targetDate });

  const timer = logger.startTimer('Sync earnings calendar');
  let fetched = 0;
  let inserted = 0;
  let sourceObservedAt: string | null = null;

  try {
    // claim RPCで前回successをfailedへfence済み。決算発表予定を同期する。
    const result = await syncEarningsCalendar({
      logContext,
      expectedAnnouncementDate: targetDate,
      runId,
      attemptId,
    });
    fetched = result.fetched;
    inserted = result.inserted;
    sourceObservedAt = result.sourceObservedAt;

    // API 0件でも「対象日を確認して0件だった」ことを job_runs / heartbeat で
    // 証明できるよう、常に期待した対象日を返す。
    const announcementDate = targetDate;

    timer.end({
      fetched: result.fetched,
      inserted: result.inserted,
      announcementDate,
    });

    logger.info('Cron B handler completed', {
      runId,
      announcementDate,
      fetched: result.fetched,
      inserted: result.inserted,
    });

    return {
      success: true,
      announcementDate,
      fetched: result.fetched,
      inserted: result.inserted,
    };
  } catch (error) {
    let errorMessage = error instanceof Error ? error.message : String(error);

    if (error instanceof EarningsCalendarSyncError) {
      fetched = error.fetched;
      inserted = error.inserted;
      sourceObservedAt = error.sourceObservedAt;
    }

    timer.endWithError(error as Error);

    logger.error('Cron B handler failed', {
      runId,
      error: errorMessage,
    });

    let attemptSuperseded = false;

    // API取得失敗も含め、対象日の coverage を必ず failed で残す。
    // success coverage の保存自体が失敗した場合もここで failed を再試行する。
    try {
      const persisted = await failCoverageAttempt({
        targetDate,
        rowCount: inserted,
        errorCount: 1,
        sourceObservedAt,
        runId,
        attemptId,
      });
      if (!persisted) {
        attemptSuperseded = true;
        logger.warn('Skipped failure coverage for superseded Cron B attempt', {
          runId,
          attemptId,
          targetDate,
        });
      }
    } catch (coverageError) {
      const coverageMessage =
        coverageError instanceof Error ? coverageError.message : String(coverageError);
      logger.error('Failed to persist Cron B failure coverage', {
        runId,
        targetDate,
        error: coverageMessage,
      });
      errorMessage = `${errorMessage}; ${coverageMessage}`;
    }

    // reclaim済みの旧worker由来エラーは、新attemptの状態と混同するため通知しない。
    if (!attemptSuperseded) {
      try {
        await sendJobFailureEmail({
          jobName: JOB_NAME,
          error: errorMessage,
          runId,
          timestamp: new Date(),
        });
      } catch (notifyError) {
        logger.error('Failed to send failure notification', {
          error: notifyError instanceof Error ? notifyError.message : String(notifyError),
        });
      }
    }

    return {
      success: false,
      announcementDate: null,
      fetched,
      inserted,
      error: errorMessage,
    };
  }
}
