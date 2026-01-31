/**
 * Cron B ハンドラー: 決算発表予定同期
 *
 * @description 翌営業日の決算発表予定を取得
 * - APIは常に「翌営業日」分を返す
 * - 土日祝も起動して問題なし（金曜失敗→土日リカバリ可能）
 */

import { createLogger, type LogContext } from '../../utils/logger';
import { sendJobFailureEmail } from '../../notification/email';

// エンドポイント関数（バレルファイル経由を避け直接インポート）
import { syncEarningsCalendar } from '../../jquants/endpoints/earnings-calendar';

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

/**
 * Cron B メインハンドラー
 *
 * @param runId 実行ID（ログ用）
 */
export async function handleCronB(runId: string): Promise<CronBResult> {
  const logContext: LogContext = {
    jobName: JOB_NAME,
    runId,
  };

  logger.info('Starting Cron B handler', { runId });

  const timer = logger.startTimer('Sync earnings calendar');

  try {
    // 決算発表予定を同期（APIは翌営業日分を返す）
    const result = await syncEarningsCalendar({ logContext });

    timer.end({
      fetched: result.fetched,
      inserted: result.inserted,
      announcementDate: result.announcementDate,
    });

    logger.info('Cron B handler completed', {
      runId,
      announcementDate: result.announcementDate,
      fetched: result.fetched,
      inserted: result.inserted,
    });

    return {
      success: true,
      announcementDate: result.announcementDate,
      fetched: result.fetched,
      inserted: result.inserted,
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    timer.endWithError(error as Error);

    logger.error('Cron B handler failed', {
      runId,
      error: errorMessage,
    });

    // 失敗通知を送信
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

    return {
      success: false,
      announcementDate: null,
      fetched: 0,
      inserted: 0,
      error: errorMessage,
    };
  }
}
