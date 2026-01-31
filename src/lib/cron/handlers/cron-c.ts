/**
 * Cron C ハンドラー: 投資部門別同期 + 整合性チェック
 *
 * @description 週次・不定期の投資部門別データ追随と整合性チェック
 * - スライディングウィンドウ方式で過去N日分を取得
 * - 訂正・再公表への対応（published_date を主キーに含む）
 * - 取引カレンダーの未来分が埋まっているかチェック
 */

import { createLogger, type LogContext } from '../../utils/logger';
import { sendJobFailureEmail } from '../../notification/email';
import { getJSTDate, addDays } from '../../utils/date';

// エンドポイント関数（バレルファイル経由を避け直接インポート）
import { syncInvestorTypesWithWindow } from '../../jquants/endpoints/investor-types';
import { getLatestEquityBarDateFromDB } from '../../jquants/endpoints/equity-bars-daily';
import { getLatestTopixBarDateFromDB } from '../../jquants/endpoints/index-topix';

// Cronユーティリティ
import type { JobName } from '../job-run';
import { checkCalendarCoverage } from '../business-day';

// Supabaseクライアント
import { createAdminClient } from '../../supabase/admin';

const logger = createLogger({ module: 'cron-c' });

/** 環境変数からスライディングウィンドウ日数を取得（デフォルト: 60日） */
function getInvestorTypesWindowDays(): number {
  const DEFAULT_WINDOW_DAYS = 60;
  const MAX_WINDOW_DAYS = 365;
  const MIN_WINDOW_DAYS = 1;

  const envValue = process.env.INVESTOR_TYPES_WINDOW_DAYS;
  if (!envValue) {
    return DEFAULT_WINDOW_DAYS;
  }

  const parsed = parseInt(envValue, 10);

  // NaN、負数、不正値の場合はデフォルト値を返す
  if (!Number.isFinite(parsed) || parsed < MIN_WINDOW_DAYS) {
    logger.warn('Invalid INVESTOR_TYPES_WINDOW_DAYS, using default', {
      envValue,
      default: DEFAULT_WINDOW_DAYS,
    });
    return DEFAULT_WINDOW_DAYS;
  }

  // 最大値を超える場合はクランプ
  if (parsed > MAX_WINDOW_DAYS) {
    logger.warn('INVESTOR_TYPES_WINDOW_DAYS exceeds maximum, clamping', {
      envValue,
      max: MAX_WINDOW_DAYS,
    });
    return MAX_WINDOW_DAYS;
  }

  return parsed;
}

/** 整合性チェック結果 */
export interface IntegrityCheckResult {
  /** カレンダーが十分な範囲をカバーしているか */
  calendarOk: boolean;
  /** カレンダーの最小日付 */
  calendarMinDate: string | null;
  /** カレンダーの最大日付 */
  calendarMaxDate: string | null;
  /** 株価の最新日付 */
  latestEquityBarDate: string | null;
  /** TOPIXの最新日付 */
  latestTopixDate: string | null;
  /** 警告メッセージ一覧 */
  warnings: string[];
}

/** Cron C の結果 */
export interface CronCResult {
  /** 成功フラグ */
  success: boolean;
  /** 投資部門別の取得件数 */
  fetched: number;
  /** 投資部門別の保存件数 */
  inserted: number;
  /** 整合性チェック結果 */
  integrityCheck: IntegrityCheckResult;
  /** エラーメッセージ */
  error?: string;
}

/** ジョブ名 */
const JOB_NAME: JobName = 'cron_c';

/**
 * 整合性チェックを実行
 *
 * @description
 * - 取引カレンダーが未来分まで埋まっているか（±370日）
 * - 株価・TOPIXの最新日付が想定範囲内か
 */
async function runIntegrityCheck(
  logContext: LogContext
): Promise<IntegrityCheckResult> {
  logger.info('Running integrity check', logContext);

  const supabaseCore = createAdminClient('jquants_core');

  const warnings: string[] = [];

  // 並列でデータ取得（ウォーターフォール回避）
  const [calendarCoverage, latestEquityBarDate, latestTopixDate] = await Promise.all([
    checkCalendarCoverage(supabaseCore, 370, 370),
    getLatestEquityBarDateFromDB(),
    getLatestTopixBarDateFromDB(),
  ]);

  // カレンダーカバレッジをチェック
  if (!calendarCoverage.ok) {
    const msg = `Calendar coverage insufficient: min=${calendarCoverage.minDate}, max=${calendarCoverage.maxDate}, required=[${calendarCoverage.requiredMinDate}, ${calendarCoverage.requiredMaxDate}]`;
    logger.warn(msg, logContext);
    warnings.push(msg);
  }

  // 株価の最新日付をチェック
  const today = getJSTDate();
  const threeDaysAgo = addDays(today, -3);

  if (latestEquityBarDate && latestEquityBarDate < threeDaysAgo) {
    const msg = `Equity bar data is stale: latest=${latestEquityBarDate}`;
    logger.warn(msg, logContext);
    warnings.push(msg);
  }

  // TOPIXの最新日付をチェック

  if (latestTopixDate && latestTopixDate < threeDaysAgo) {
    const msg = `TOPIX data is stale: latest=${latestTopixDate}`;
    logger.warn(msg, logContext);
    warnings.push(msg);
  }

  logger.info('Integrity check completed', {
    ...logContext,
    calendarOk: calendarCoverage.ok,
    latestEquityBarDate,
    latestTopixDate,
    warningCount: warnings.length,
  });

  return {
    calendarOk: calendarCoverage.ok,
    calendarMinDate: calendarCoverage.minDate,
    calendarMaxDate: calendarCoverage.maxDate,
    latestEquityBarDate,
    latestTopixDate,
    warnings,
  };
}

/**
 * Cron C メインハンドラー
 *
 * @param runId 実行ID（ログ用）
 */
export async function handleCronC(runId: string): Promise<CronCResult> {
  const logContext: LogContext = {
    jobName: JOB_NAME,
    runId,
  };

  logger.info('Starting Cron C handler', { runId });

  const timer = logger.startTimer('Cron C handler');

  // 初期状態の整合性チェック結果
  let integrityCheck: IntegrityCheckResult = {
    calendarOk: false,
    calendarMinDate: null,
    calendarMaxDate: null,
    latestEquityBarDate: null,
    latestTopixDate: null,
    warnings: [],
  };

  try {
    // 1. 投資部門別を同期（スライディングウィンドウ）
    const windowDays = getInvestorTypesWindowDays();
    logger.info('Syncing investor types', { windowDays, runId });

    // 投資部門別同期と整合性チェックは独立しているため並列実行
    const [syncResult, integrityCheckResult] = await Promise.all([
      syncInvestorTypesWithWindow(windowDays, { logContext }),
      runIntegrityCheck(logContext),
    ]);
    integrityCheck = integrityCheckResult;

    logger.info('Investor types sync completed', {
      runId,
      fetched: syncResult.fetched,
      inserted: syncResult.inserted,
    });

    timer.end({
      fetched: syncResult.fetched,
      inserted: syncResult.inserted,
      integrityWarnings: integrityCheck.warnings.length,
    });

    // 警告がある場合でも成功として扱うが、ログに記録
    if (integrityCheck.warnings.length > 0) {
      logger.warn('Integrity check has warnings', {
        runId,
        warnings: integrityCheck.warnings,
      });
    }

    logger.info('Cron C handler completed', {
      runId,
      fetched: syncResult.fetched,
      inserted: syncResult.inserted,
    });

    return {
      success: true,
      fetched: syncResult.fetched,
      inserted: syncResult.inserted,
      integrityCheck,
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    timer.endWithError(error as Error);

    logger.error('Cron C handler failed', {
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
      fetched: 0,
      inserted: 0,
      integrityCheck,
      error: errorMessage,
    };
  }
}
