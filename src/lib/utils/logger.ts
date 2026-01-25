/**
 * 構造化ロギングユーティリティ
 *
 * @description Vercel Logs で解析しやすい JSON 形式のログ出力
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface LogContext {
  /** ジョブ名 (cron_a, cron_b, cron_c) */
  jobName?: string;
  /** 実行ID (UUID) */
  runId?: string;
  /** データセット名 */
  dataset?: string;
  /** 対象日付 */
  targetDate?: string;
  /** 処理行数 */
  rowCount?: number;
  /** ページ数 */
  pageCount?: number;
  /** 処理時間（ミリ秒） */
  durationMs?: number;
  /** エラーコード */
  errorCode?: string;
  /** その他のコンテキスト */
  [key: string]: unknown;
}

interface LogPayload extends LogContext {
  timestamp: string;
  level: LogLevel;
  message: string;
}

/**
 * ログレベルの優先度
 */
const LOG_LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

/**
 * 最小ログレベル（モジュールロード時に一度だけ評価）
 */
const MIN_LOG_LEVEL: LogLevel = (() => {
  const level = process.env.LOG_LEVEL?.toLowerCase() as LogLevel | undefined;
  if (level && level in LOG_LEVEL_PRIORITY) {
    return level;
  }
  return process.env.NODE_ENV === 'production' ? 'info' : 'debug';
})();

/**
 * ログを出力すべきかどうかを判定
 */
function shouldLog(level: LogLevel): boolean {
  return LOG_LEVEL_PRIORITY[level] >= LOG_LEVEL_PRIORITY[MIN_LOG_LEVEL];
}

/**
 * エラーオブジェクトをシリアライズ可能な形式に変換
 */
function serializeError(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack?.split('\n').slice(0, 5).join('\n'), // スタックトレースを5行に制限
      ...(error.cause ? { cause: serializeError(error.cause) } : {}),
    };
  }
  return { value: String(error) };
}

/**
 * ロガーを作成
 *
 * @param defaultContext 全ログに付与するデフォルトコンテキスト
 *
 * @example
 * ```typescript
 * const logger = createLogger({ jobName: 'cron_a', runId: 'xxx-xxx' });
 * logger.info('Processing started', { dataset: 'equity_bars' });
 * logger.error('Failed to fetch data', { error: err, statusCode: 500 });
 * ```
 */
export function createLogger(defaultContext: LogContext = {}) {
  const log = (level: LogLevel, message: string, context: LogContext = {}) => {
    if (!shouldLog(level)) {
      return;
    }

    // エラーオブジェクトがあればシリアライズ
    const processedContext = { ...context };
    if (processedContext.error) {
      processedContext.error = serializeError(processedContext.error);
    }

    const payload: LogPayload = {
      timestamp: new Date().toISOString(),
      level,
      message,
      ...defaultContext,
      ...processedContext,
    };

    // JSON形式で出力（Vercel Logsが解析可能）
    const jsonStr = JSON.stringify(payload);

    switch (level) {
      case 'error':
        console.error(jsonStr);
        break;
      case 'warn':
        console.warn(jsonStr);
        break;
      default:
        console.log(jsonStr);
    }
  };

  return {
    debug: (message: string, context?: LogContext) => log('debug', message, context),
    info: (message: string, context?: LogContext) => log('info', message, context),
    warn: (message: string, context?: LogContext) => log('warn', message, context),
    error: (message: string, context?: LogContext) => log('error', message, context),

    /**
     * 子ロガーを作成（コンテキストを追加）
     */
    child: (additionalContext: LogContext) =>
      createLogger({ ...defaultContext, ...additionalContext }),

    /**
     * 処理時間を計測するタイマーを開始
     */
    startTimer: (label: string) => {
      const startTime = Date.now();
      return {
        end: (context?: LogContext) => {
          const durationMs = Date.now() - startTime;
          log('info', `${label} completed`, { ...context, durationMs });
          return durationMs;
        },
        endWithError: (error: Error, context?: LogContext) => {
          const durationMs = Date.now() - startTime;
          log('error', `${label} failed`, { ...context, durationMs, error });
          return durationMs;
        },
      };
    },
  };
}

/**
 * デフォルトロガー
 */
export const logger = createLogger();

/**
 * ジョブ用ロガーを作成するファクトリ関数
 */
export function createJobLogger(jobName: string, runId: string, targetDate?: string) {
  return createLogger({
    jobName,
    runId,
    targetDate,
  });
}
