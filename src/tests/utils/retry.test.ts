import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  RetryableError,
  NonRetryableError,
  withRetry,
  fetchWithRetry,
} from '@/lib/utils/retry';

describe('retry.ts', () => {
  describe('RetryableError', () => {
    it('プロパティが正しく設定される', () => {
      const error = new RetryableError('Test error', 429);
      expect(error.name).toBe('RetryableError');
      expect(error.message).toBe('Test error');
      expect(error.statusCode).toBe(429);
    });

    it('cause を設定できる', () => {
      const cause = new Error('Original error');
      const error = new RetryableError('Wrapped error', 500, cause);
      expect(error.cause).toBe(cause);
    });
  });

  describe('NonRetryableError', () => {
    it('プロパティが正しく設定される', () => {
      const error = new NonRetryableError('Bad request', 400);
      expect(error.name).toBe('NonRetryableError');
      expect(error.message).toBe('Bad request');
      expect(error.statusCode).toBe(400);
    });
  });

  describe('withRetry', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('成功時は即時完了する', async () => {
      const fn = vi.fn().mockResolvedValue('success');

      const resultPromise = withRetry(fn, { maxRetries: 3 });
      const result = await resultPromise;

      expect(result).toBe('success');
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('RetryableErrorでリトライして成功する', async () => {
      const fn = vi
        .fn()
        .mockRejectedValueOnce(new RetryableError('Temporary failure', 503))
        .mockRejectedValueOnce(new RetryableError('Temporary failure', 503))
        .mockResolvedValue('success');

      const resultPromise = withRetry(fn, {
        maxRetries: 3,
        baseDelayMs: 100,
        jitterMs: 0,
      });

      // 1回目失敗 → 100ms待機
      await vi.advanceTimersByTimeAsync(100);
      // 2回目失敗 → 200ms待機
      await vi.advanceTimersByTimeAsync(200);

      const result = await resultPromise;
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalledTimes(3);
    });

    it('NonRetryableErrorで即座に失敗する', async () => {
      const fn = vi
        .fn()
        .mockRejectedValue(new NonRetryableError('Bad request', 400));

      await expect(withRetry(fn, { maxRetries: 3 })).rejects.toThrow(
        NonRetryableError
      );
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('最大リトライ回数を超過すると失敗する', async () => {
      const fn = vi
        .fn()
        .mockRejectedValue(new RetryableError('Always fails', 503));

      // Promise を作成し、すぐに catch ハンドラをつけておく
      const resultPromise = withRetry(fn, {
        maxRetries: 2,
        baseDelayMs: 100,
        jitterMs: 0,
      }).catch((e) => e);

      // 全てのタイマーを進めてPromiseを解決させる
      await vi.runAllTimersAsync();

      const result = await resultPromise;
      expect(result).toBeInstanceOf(RetryableError);
      // 初回 + リトライ2回 = 3回
      expect(fn).toHaveBeenCalledTimes(3);
    });

    it('onRetryコールバックが呼ばれる', async () => {
      const onRetry = vi.fn();
      const fn = vi
        .fn()
        .mockRejectedValueOnce(new RetryableError('Fail 1', 503))
        .mockRejectedValueOnce(new RetryableError('Fail 2', 503))
        .mockResolvedValue('success');

      const resultPromise = withRetry(fn, {
        maxRetries: 3,
        baseDelayMs: 100,
        jitterMs: 0,
        onRetry,
      });

      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(200);

      await resultPromise;

      expect(onRetry).toHaveBeenCalledTimes(2);
      // attempt = 1 (attempt + 1 で渡される)
      expect(onRetry).toHaveBeenNthCalledWith(1, 1, expect.any(Error), 100);
      // attempt = 2
      expect(onRetry).toHaveBeenNthCalledWith(2, 2, expect.any(Error), 200);
    });

    it('指数バックオフが正しく計算される（jitterMs=0）', async () => {
      const onRetry = vi.fn();
      const fn = vi
        .fn()
        .mockRejectedValueOnce(new RetryableError('Fail', 503))
        .mockRejectedValueOnce(new RetryableError('Fail', 503))
        .mockRejectedValueOnce(new RetryableError('Fail', 503))
        .mockResolvedValue('success');

      const resultPromise = withRetry(fn, {
        maxRetries: 3,
        baseDelayMs: 100,
        maxDelayMs: 1000,
        jitterMs: 0,
        onRetry,
      });

      // delay = 100 * 2^0 = 100
      await vi.advanceTimersByTimeAsync(100);
      // delay = 100 * 2^1 = 200
      await vi.advanceTimersByTimeAsync(200);
      // delay = 100 * 2^2 = 400
      await vi.advanceTimersByTimeAsync(400);

      await resultPromise;

      expect(onRetry).toHaveBeenNthCalledWith(1, 1, expect.any(Error), 100);
      expect(onRetry).toHaveBeenNthCalledWith(2, 2, expect.any(Error), 200);
      expect(onRetry).toHaveBeenNthCalledWith(3, 3, expect.any(Error), 400);
    });

    it('最大遅延でキャップされる', async () => {
      const onRetry = vi.fn();
      const fn = vi
        .fn()
        .mockRejectedValueOnce(new RetryableError('Fail', 503))
        .mockRejectedValueOnce(new RetryableError('Fail', 503))
        .mockRejectedValueOnce(new RetryableError('Fail', 503))
        .mockResolvedValue('success');

      const resultPromise = withRetry(fn, {
        maxRetries: 3,
        baseDelayMs: 100,
        maxDelayMs: 150, // 100 * 2^1 = 200 を超える
        jitterMs: 0,
        onRetry,
      });

      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(150); // キャップ
      await vi.advanceTimersByTimeAsync(150); // キャップ

      await resultPromise;

      expect(onRetry).toHaveBeenNthCalledWith(1, 1, expect.any(Error), 100);
      expect(onRetry).toHaveBeenNthCalledWith(2, 2, expect.any(Error), 150);
      expect(onRetry).toHaveBeenNthCalledWith(3, 3, expect.any(Error), 150);
    });

    it('statusCodeがretryStatusCodesに含まれる場合リトライする', async () => {
      const errorWithStatusCode = Object.assign(new Error('Custom error'), {
        statusCode: 429,
      });
      const fn = vi
        .fn()
        .mockRejectedValueOnce(errorWithStatusCode)
        .mockResolvedValue('success');

      const resultPromise = withRetry(fn, {
        maxRetries: 1,
        baseDelayMs: 100,
        jitterMs: 0,
        retryStatusCodes: [429],
      });

      await vi.advanceTimersByTimeAsync(100);

      const result = await resultPromise;
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it('ネットワークエラーでリトライする', async () => {
      const networkError = new Error('fetch failed');
      const fn = vi
        .fn()
        .mockRejectedValueOnce(networkError)
        .mockResolvedValue('success');

      const resultPromise = withRetry(fn, {
        maxRetries: 1,
        baseDelayMs: 100,
        jitterMs: 0,
      });

      await vi.advanceTimersByTimeAsync(100);

      const result = await resultPromise;
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalledTimes(2);
    });
  });

  describe('fetchWithRetry', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
      vi.unstubAllGlobals();
    });

    it('成功時はResponseを返す', async () => {
      const mockResponse = new Response('{"data": "test"}', {
        status: 200,
        statusText: 'OK',
      });
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(mockResponse));

      const response = await fetchWithRetry('https://api.example.com/data');

      expect(response.ok).toBe(true);
      expect(fetch).toHaveBeenCalledTimes(1);
    });

    it('429でリトライして成功する', async () => {
      const rateLimitResponse = new Response('Too Many Requests', {
        status: 429,
        statusText: 'Too Many Requests',
      });
      const successResponse = new Response('{"data": "test"}', {
        status: 200,
        statusText: 'OK',
      });

      vi.stubGlobal(
        'fetch',
        vi.fn()
          .mockResolvedValueOnce(rateLimitResponse)
          .mockResolvedValueOnce(successResponse)
      );

      const resultPromise = fetchWithRetry(
        'https://api.example.com/data',
        undefined,
        {
          maxRetries: 1,
          baseDelayMs: 100,
          jitterMs: 0,
        }
      );

      await vi.advanceTimersByTimeAsync(100);

      const response = await resultPromise;
      expect(response.ok).toBe(true);
      expect(fetch).toHaveBeenCalledTimes(2);
    });

    it('400で即座に失敗する', async () => {
      const badRequestResponse = new Response('Bad Request', {
        status: 400,
        statusText: 'Bad Request',
      });
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(badRequestResponse));

      await expect(
        fetchWithRetry('https://api.example.com/data', undefined, {
          maxRetries: 3,
        })
      ).rejects.toThrow(NonRetryableError);

      expect(fetch).toHaveBeenCalledTimes(1);
    });

    it('500系エラーでリトライする', async () => {
      const errorCodes = [502, 503, 504];

      for (const statusCode of errorCodes) {
        vi.clearAllMocks();

        const errorResponse = new Response('Server Error', {
          status: statusCode,
          statusText: 'Server Error',
        });
        const successResponse = new Response('OK', {
          status: 200,
          statusText: 'OK',
        });

        vi.stubGlobal(
          'fetch',
          vi.fn()
            .mockResolvedValueOnce(errorResponse)
            .mockResolvedValueOnce(successResponse)
        );

        const resultPromise = fetchWithRetry(
          'https://api.example.com/data',
          undefined,
          {
            maxRetries: 1,
            baseDelayMs: 100,
            jitterMs: 0,
          }
        );

        await vi.advanceTimersByTimeAsync(100);

        const response = await resultPromise;
        expect(response.ok).toBe(true);
        expect(fetch).toHaveBeenCalledTimes(2);
      }
    });
  });
});
