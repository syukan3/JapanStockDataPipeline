/**
 * logger.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createLogger, createJobLogger } from '@/lib/utils/logger';

describe('logger.ts', () => {
  let consoleLogSpy: ReturnType<typeof vi.spyOn>;
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('createLogger', () => {
    it('デフォルトコンテキストなしでロガーを作成できる', () => {
      const logger = createLogger();
      expect(logger).toBeDefined();
      expect(logger.debug).toBeTypeOf('function');
      expect(logger.info).toBeTypeOf('function');
      expect(logger.warn).toBeTypeOf('function');
      expect(logger.error).toBeTypeOf('function');
      expect(logger.child).toBeTypeOf('function');
      expect(logger.startTimer).toBeTypeOf('function');
    });

    it('デフォルトコンテキストを設定できる', () => {
      const logger = createLogger({ jobName: 'cron_a', runId: 'test-run-id' });
      logger.info('test message');

      expect(consoleLogSpy).toHaveBeenCalledTimes(1);
      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.jobName).toBe('cron_a');
      expect(loggedJson.runId).toBe('test-run-id');
      expect(loggedJson.message).toBe('test message');
    });

    it('JSON形式でログ出力する', () => {
      const logger = createLogger();
      logger.info('test message', { dataset: 'equity_bars' });

      expect(consoleLogSpy).toHaveBeenCalledTimes(1);
      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.timestamp).toBeDefined();
      expect(loggedJson.level).toBe('info');
      expect(loggedJson.message).toBe('test message');
      expect(loggedJson.dataset).toBe('equity_bars');
    });
  });

  describe('ログレベル別出力', () => {
    it('debug は console.log を使用する', () => {
      const logger = createLogger();
      logger.debug('debug message');

      // Note: setup.ts で NODE_ENV=test のため、debug レベルでもログ出力される
      expect(consoleLogSpy).toHaveBeenCalled();
    });

    it('info は console.log を使用する', () => {
      const logger = createLogger();
      logger.info('info message');

      expect(consoleLogSpy).toHaveBeenCalledTimes(1);
      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.level).toBe('info');
    });

    it('warn は console.warn を使用する', () => {
      const logger = createLogger();
      logger.warn('warn message');

      expect(consoleWarnSpy).toHaveBeenCalledTimes(1);
      const loggedJson = JSON.parse(consoleWarnSpy.mock.calls[0][0]);
      expect(loggedJson.level).toBe('warn');
    });

    it('error は console.error を使用する', () => {
      const logger = createLogger();
      logger.error('error message');

      expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
      const loggedJson = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(loggedJson.level).toBe('error');
    });
  });

  describe('エラーシリアライズ', () => {
    it('Errorオブジェクトをシリアライズする', () => {
      const logger = createLogger();
      const testError = new Error('Test error message');
      logger.error('error occurred', { error: testError });

      expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
      const loggedJson = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(loggedJson.error.name).toBe('Error');
      expect(loggedJson.error.message).toBe('Test error message');
      expect(loggedJson.error.stack).toBeDefined();
    });

    it('スタックトレースを5行に制限する', () => {
      const logger = createLogger();
      const testError = new Error('Test error');
      logger.error('error occurred', { error: testError });

      const loggedJson = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      const stackLines = loggedJson.error.stack.split('\n');
      expect(stackLines.length).toBeLessThanOrEqual(5);
    });

    it('Error.causeをネストしてシリアライズする', () => {
      const logger = createLogger();
      const causeError = new Error('Cause error');
      const testError = new Error('Main error', { cause: causeError });
      logger.error('error occurred', { error: testError });

      const loggedJson = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(loggedJson.error.cause).toBeDefined();
      expect(loggedJson.error.cause.message).toBe('Cause error');
    });

    it('非Errorオブジェクトをシリアライズする', () => {
      const logger = createLogger();
      logger.error('error occurred', { error: 'string error' as unknown });

      const loggedJson = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(loggedJson.error.value).toBe('string error');
    });

    it('プレーンオブジェクトのエラーは中身を保持する', () => {
      const logger = createLogger();
      // Resend SDK 等は Error ではないオブジェクトを返す
      logger.error('error occurred', {
        error: { statusCode: 429, name: 'rate_limit_exceeded', message: 'Too many requests' },
      });

      const loggedJson = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(loggedJson.error.name).toBe('rate_limit_exceeded');
      expect(loggedJson.error.message).toBe('Too many requests');
      expect(loggedJson.error.statusCode).toBe(429);
    });

    it('allowlist外のフィールド（認証情報等）はログに出さない', () => {
      const logger = createLogger();
      logger.error('error occurred', {
        error: {
          message: 'Unauthorized',
          statusCode: 401,
          headers: { authorization: 'Bearer super-secret-token' },
          request: { url: 'https://api.example.com?apikey=secret' },
        },
      });

      const raw = consoleErrorSpy.mock.calls[0][0] as string;
      expect(raw).not.toContain('super-secret-token');
      expect(raw).not.toContain('apikey=secret');

      const loggedJson = JSON.parse(raw);
      expect(loggedJson.error.message).toBe('Unauthorized');
      expect(loggedJson.error.statusCode).toBe(401);
      expect(loggedJson.error.headers).toBeUndefined();
      expect(loggedJson.error.request).toBeUndefined();
    });

    it('長すぎる文字列フィールドは500文字に切り詰める', () => {
      const logger = createLogger();
      logger.error('error occurred', { error: { message: 'a'.repeat(501) } });

      const loggedJson = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(loggedJson.error.message).toHaveLength(500);
    });

    it('既知フィールドが無いオブジェクトはキー名のみ残す', () => {
      const logger = createLogger();
      logger.error('error occurred', { error: { secretValue: 'do-not-log-me' } });

      const raw = consoleErrorSpy.mock.calls[0][0] as string;
      expect(raw).not.toContain('do-not-log-me');
      expect(JSON.parse(raw).error.keys).toEqual(['secretValue']);
    });

    it('循環参照を含むオブジェクトでも落ちない', () => {
      const logger = createLogger();
      const circular: Record<string, unknown> = { message: 'weird' };
      circular.self = circular;

      expect(() => logger.error('error occurred', { error: circular })).not.toThrow();

      const loggedJson = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(loggedJson.error.message).toBe('weird');
      expect(loggedJson.error.self).toBeUndefined();
    });
  });

  describe('child ロガー', () => {
    it('子ロガーを作成できる', () => {
      const parentLogger = createLogger({ jobName: 'cron_a' });
      const childLogger = parentLogger.child({ dataset: 'equity_bars' });

      childLogger.info('child message');

      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.jobName).toBe('cron_a');
      expect(loggedJson.dataset).toBe('equity_bars');
    });

    it('子ロガーは親のコンテキストを継承する', () => {
      const parentLogger = createLogger({ jobName: 'cron_a', runId: 'run-123' });
      const childLogger = parentLogger.child({ dataset: 'equity_bars' });

      childLogger.info('test');

      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.jobName).toBe('cron_a');
      expect(loggedJson.runId).toBe('run-123');
      expect(loggedJson.dataset).toBe('equity_bars');
    });

    it('子ロガーは親のコンテキストを上書きできる', () => {
      const parentLogger = createLogger({ jobName: 'cron_a' });
      const childLogger = parentLogger.child({ jobName: 'cron_b' });

      childLogger.info('test');

      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.jobName).toBe('cron_b');
    });
  });

  describe('startTimer', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('処理時間を計測できる', () => {
      const logger = createLogger();
      const timer = logger.startTimer('Processing');

      vi.advanceTimersByTime(1000);

      const duration = timer.end();

      expect(duration).toBe(1000);
      expect(consoleLogSpy).toHaveBeenCalledTimes(1);
      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.message).toBe('Processing completed');
      expect(loggedJson.durationMs).toBe(1000);
    });

    it('エラー終了時に処理時間を計測できる', () => {
      const logger = createLogger();
      const timer = logger.startTimer('Processing');

      vi.advanceTimersByTime(500);

      const testError = new Error('Processing failed');
      const duration = timer.endWithError(testError);

      expect(duration).toBe(500);
      expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
      const loggedJson = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(loggedJson.message).toBe('Processing failed');
      expect(loggedJson.durationMs).toBe(500);
      expect(loggedJson.error.message).toBe('Processing failed');
    });

    it('end()にコンテキストを渡せる', () => {
      const logger = createLogger();
      const timer = logger.startTimer('Processing');

      vi.advanceTimersByTime(100);

      timer.end({ rowCount: 100 });

      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.rowCount).toBe(100);
      expect(loggedJson.durationMs).toBe(100);
    });
  });

  describe('createJobLogger', () => {
    it('ジョブ用ロガーを作成できる', () => {
      const logger = createJobLogger('cron_a', 'run-123', '2024-01-15');
      logger.info('job started');

      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.jobName).toBe('cron_a');
      expect(loggedJson.runId).toBe('run-123');
      expect(loggedJson.targetDate).toBe('2024-01-15');
    });

    it('targetDateは省略可能', () => {
      const logger = createJobLogger('cron_b', 'run-456');
      logger.info('job started');

      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.jobName).toBe('cron_b');
      expect(loggedJson.runId).toBe('run-456');
      expect(loggedJson.targetDate).toBeUndefined();
    });
  });

  describe('timestamp', () => {
    it('ISO形式のタイムスタンプを含む', () => {
      const logger = createLogger();
      logger.info('test');

      const loggedJson = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(loggedJson.timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
    });
  });
});
