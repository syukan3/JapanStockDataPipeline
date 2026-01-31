/**
 * Cron A ハンドラー: 日次確定データ同期
 *
 * @description 前営業日の確定データをデータセット単位で取り込み
 * - calendar: 取引カレンダー（±370日）
 * - equity_bars: 株価日足
 * - topix: TOPIX
 * - financial: 財務サマリー
 * - equity_master: 銘柄マスタスナップショット
 *
 * Vercel Hobby の 10 秒制限に対応するため、1 回の呼び出しで 1 データセットのみ処理
 */

import { z } from 'zod';
import { createLogger, type LogContext } from '../../utils/logger';
import { getJSTDate } from '../../utils/date';
import { sendJobFailureEmail } from '../../notification/email';

// エンドポイント関数（バレルファイル経由を避け直接インポート）
import { syncTradingCalendarRange } from '../../jquants/endpoints/trading-calendar';
import { syncEquityBarsDailyForDate } from '../../jquants/endpoints/equity-bars-daily';
import { syncTopixBarsDailyForDate } from '../../jquants/endpoints/index-topix';
import { syncFinancialSummaryForDate } from '../../jquants/endpoints/fins-summary';
import { syncEquityMasterSCD } from '../../jquants/endpoints/equity-master';

// Cronユーティリティ
import { determineTargetDates } from '../catch-up';
import type { JobName } from '../job-run';

// Supabaseクライアント
import { createAdminClient } from '../../supabase/admin';

const logger = createLogger({ module: 'cron-a' });

/** Cron A で処理するデータセット */
export const CRON_A_DATASETS = [
  'calendar',
  'equity_bars',
  'topix',
  'financial',
  'equity_master',
] as const;

export type CronADataset = (typeof CRON_A_DATASETS)[number];

/** リクエストボディのスキーマ */
export const CronARequestSchema = z.object({
  dataset: z.enum(CRON_A_DATASETS),
});

export type CronARequest = z.infer<typeof CronARequestSchema>;

/** Cron A の結果 */
export interface CronAResult {
  /** 成功フラグ */
  success: boolean;
  /** 処理したデータセット */
  dataset: CronADataset;
  /** 対象日付 */
  targetDate: string | null;
  /** 取得件数 */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** ページ数（ページネーション対応の場合） */
  pageCount?: number;
  /** エラーメッセージ */
  error?: string;
}

/** ジョブ名 */
const JOB_NAME: JobName = 'cron_a';

/**
 * 取引カレンダーを同期（±370日）
 */
async function syncCalendar(
  logContext: LogContext
): Promise<CronAResult> {
  const timer = logger.startTimer('Sync calendar');

  try {
    const result = await syncTradingCalendarRange(new Date(), 370, { logContext });

    timer.end({
      fetched: result.fetched,
      inserted: result.inserted,
    });

    return {
      success: true,
      dataset: 'calendar',
      targetDate: getJSTDate(),
      fetched: result.fetched,
      inserted: result.inserted,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/**
 * 株価日足を同期（前営業日分）
 */
async function syncEquityBars(
  targetDate: string,
  logContext: LogContext
): Promise<CronAResult> {
  const timer = logger.startTimer('Sync equity bars');

  try {
    const result = await syncEquityBarsDailyForDate(targetDate, { logContext });

    timer.end({
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
      pageCount: result.pageCount,
    });

    return {
      success: true,
      dataset: 'equity_bars',
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
      pageCount: result.pageCount,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/**
 * TOPIXを同期（前営業日分）
 */
async function syncTopix(
  targetDate: string,
  logContext: LogContext
): Promise<CronAResult> {
  const timer = logger.startTimer('Sync TOPIX');

  try {
    const result = await syncTopixBarsDailyForDate(targetDate, { logContext });

    timer.end({
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
    });

    return {
      success: true,
      dataset: 'topix',
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/**
 * 財務サマリーを同期（前営業日分）
 */
async function syncFinancial(
  targetDate: string,
  logContext: LogContext
): Promise<CronAResult> {
  const timer = logger.startTimer('Sync financial');

  try {
    const result = await syncFinancialSummaryForDate(targetDate, { logContext });

    timer.end({
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
      pageCount: result.pageCount,
    });

    return {
      success: true,
      dataset: 'financial',
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
      pageCount: result.pageCount,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/**
 * 銘柄マスタを同期（SCD Type 2方式）
 */
async function syncEquityMaster(
  targetDate: string,
  logContext: LogContext
): Promise<CronAResult> {
  const timer = logger.startTimer('Sync equity master SCD');

  try {
    const result = await syncEquityMasterSCD(targetDate, { logContext });

    timer.end({
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
      updated: result.updated,
      delisted: result.delisted,
    });

    return {
      success: true,
      dataset: 'equity_master',
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted + result.updated,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/**
 * データセットがキャッチアップ対象かどうかを検証
 *
 * NOTE: 現在の設計では全データセットが同一のjob_name='cron_a'を共有し、
 * 日付単位でキャッチアップを管理している。これは、GitHub Actionsから
 * 各データセットを順次呼び出すことで、同一日のデータを完全に処理する設計。
 * 将来的にデータセット単位のキャッチアップが必要になった場合は、
 * job_nameをデータセット別に分離するか、job_run_itemsを活用する。
 */
function isValidCatchUpDataset(dataset: CronADataset): boolean {
  // calendarは特別処理のため対象外
  return dataset !== 'calendar';
}

/**
 * Cron A メインハンドラー
 *
 * @param dataset 処理するデータセット
 * @param runId 実行ID（ログ用）
 */
export async function handleCronA(
  dataset: CronADataset,
  runId: string
): Promise<CronAResult> {
  const logContext: LogContext = {
    jobName: JOB_NAME,
    runId,
    dataset,
  };

  logger.info('Starting Cron A handler', { dataset, runId });

  try {
    // カレンダーは特別処理（キャッチアップ不要）
    if (dataset === 'calendar') {
      return await syncCalendar(logContext);
    }

    // キャッチアップ対象かどうかを検証
    if (!isValidCatchUpDataset(dataset)) {
      throw new Error(`Dataset not eligible for catch-up: ${dataset}`);
    }

    // Supabaseクライアント取得
    const supabaseIngest = createAdminClient('jquants_ingest');
    const supabaseCore = createAdminClient('jquants_core');

    // 処理対象日を決定（キャッチアップ対応）
    const targetDates = await determineTargetDates(
      supabaseIngest,
      supabaseCore,
      JOB_NAME
    );

    if (targetDates.length === 0) {
      logger.info('No target dates to process', { dataset });
      return {
        success: true,
        dataset,
        targetDate: null,
        fetched: 0,
        inserted: 0,
      };
    }

    // 10秒制限のため、1日分のみ処理
    const targetDate = targetDates[0];
    logger.info('Processing target date', { dataset, targetDate });

    // データセットに応じた処理を実行
    let result: CronAResult;

    switch (dataset) {
      case 'equity_bars':
        result = await syncEquityBars(targetDate, logContext);
        break;
      case 'topix':
        result = await syncTopix(targetDate, logContext);
        break;
      case 'financial':
        result = await syncFinancial(targetDate, logContext);
        break;
      case 'equity_master':
        result = await syncEquityMaster(targetDate, logContext);
        break;
      default:
        throw new Error(`Unknown dataset: ${dataset}`);
    }

    logger.info('Cron A handler completed', {
      dataset,
      runId,
      targetDate,
      fetched: result.fetched,
      inserted: result.inserted,
    });

    return result;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Cron A handler failed', {
      dataset,
      runId,
      error: errorMessage,
    });

    // 失敗通知を送信
    try {
      await sendJobFailureEmail({
        jobName: JOB_NAME,
        error: errorMessage,
        runId,
        dataset,
        timestamp: new Date(),
      });
    } catch (notifyError) {
      logger.error('Failed to send failure notification', {
        error: notifyError instanceof Error ? notifyError.message : String(notifyError),
      });
    }

    return {
      success: false,
      dataset,
      targetDate: null,
      fetched: 0,
      inserted: 0,
      error: errorMessage,
    };
  }
}
