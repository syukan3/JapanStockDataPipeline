import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  RetryableError,
  NonRetryableError,
  withRetry,
  fetchWithRetry,
  withPostgrestRetry,
  createRetryingFetch,
  isTransientPostgrestStatus,
  TRANSIENT_POSTGREST_STATUS_CODES,
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
  describe('withPostgrestRetry', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('成功応答はそのまま返す', async () => {
      const run = vi.fn().mockResolvedValue({ data: [1], error: null, status: 200 });

      const result = await withPostgrestRetry(run);

      expect(result.data).toEqual([1]);
      expect(run).toHaveBeenCalledTimes(1);
    });

    it('504 Gateway Timeout はリトライして成功する', async () => {
      const run = vi
        .fn()
        .mockResolvedValueOnce({ data: null, error: { message: 'Gateway Timeout' }, status: 504 })
        .mockResolvedValue({ data: ['ok'], error: null, status: 200 });
      const onRetry = vi.fn();

      const resultPromise = withPostgrestRetry(run, {
        maxRetries: 3,
        baseDelayMs: 100,
        jitterMs: 0,
        onRetry,
      });

      await vi.advanceTimersByTimeAsync(100);
      const result = await resultPromise;

      expect(result.error).toBeNull();
      expect(result.data).toEqual(['ok']);
      expect(run).toHaveBeenCalledTimes(2);
      expect(onRetry).toHaveBeenCalledTimes(1);
    });

    it('ネットワーク障害（status=0）もリトライ対象', async () => {
      const run = vi
        .fn()
        .mockResolvedValueOnce({ data: null, error: { message: 'FetchError: fetch failed' }, status: 0 })
        .mockResolvedValue({ data: [], error: null, status: 200 });

      const resultPromise = withPostgrestRetry(run, {
        maxRetries: 2,
        baseDelayMs: 100,
        jitterMs: 0,
      });

      await vi.advanceTimersByTimeAsync(100);
      const result = await resultPromise;

      expect(result.error).toBeNull();
      expect(run).toHaveBeenCalledTimes(2);
    });

    it('429 Too Many Requests はリトライして成功する', async () => {
      const run = vi
        .fn()
        .mockResolvedValueOnce({ data: null, error: { message: 'Too Many Requests' }, status: 429 })
        .mockResolvedValue({ data: ['ok'], error: null, status: 200 });

      const resultPromise = withPostgrestRetry(run, {
        maxRetries: 2,
        baseDelayMs: 100,
        jitterMs: 0,
      });

      await vi.advanceTimersByTimeAsync(100);
      const result = await resultPromise;

      expect(result.error).toBeNull();
      expect(run).toHaveBeenCalledTimes(2);
    });

    it('408 Request Timeout はリトライして成功する', async () => {
      const run = vi
        .fn()
        .mockResolvedValueOnce({ data: null, error: { message: 'Request Timeout' }, status: 408 })
        .mockResolvedValue({ data: ['ok'], error: null, status: 200 });

      const resultPromise = withPostgrestRetry(run, {
        maxRetries: 2,
        baseDelayMs: 100,
        jitterMs: 0,
      });

      await vi.advanceTimersByTimeAsync(100);
      const result = await resultPromise;

      expect(result.error).toBeNull();
      expect(run).toHaveBeenCalledTimes(2);
    });

    it('4xx（業務エラー）はリトライしない', async () => {
      const run = vi
        .fn()
        .mockResolvedValue({ data: null, error: { message: 'column does not exist' }, status: 400 });

      const result = await withPostgrestRetry(run, { maxRetries: 3, baseDelayMs: 100, jitterMs: 0 });

      expect(result.error?.message).toBe('column does not exist');
      expect(run).toHaveBeenCalledTimes(1);
    });

    it('status を持たないエラー応答はリトライしない', async () => {
      const run = vi.fn().mockResolvedValue({ error: { message: 'upsert failed' } });

      const result = await withPostgrestRetry(run, { maxRetries: 3, baseDelayMs: 100, jitterMs: 0 });

      expect(result.error?.message).toBe('upsert failed');
      expect(run).toHaveBeenCalledTimes(1);
    });

    it('リトライ上限に達したらエラーを含んだまま返す', async () => {
      const run = vi
        .fn()
        .mockResolvedValue({ data: null, error: { message: 'Gateway Timeout' }, status: 504 });

      const resultPromise = withPostgrestRetry(run, {
        maxRetries: 2,
        baseDelayMs: 100,
        jitterMs: 0,
      });

      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(200);
      const result = await resultPromise;

      expect(result.error?.message).toBe('Gateway Timeout');
      expect(run).toHaveBeenCalledTimes(3); // 初回 + リトライ2回
    });

    it('500（SQL例外）はリトライしない', async () => {
      const run = vi
        .fn()
        .mockResolvedValue({ data: null, error: { message: 'statement timeout' }, status: 500 });

      const result = await withPostgrestRetry(run, { maxRetries: 2, baseDelayMs: 100, jitterMs: 0 });

      expect(result.error?.message).toBe('statement timeout');
      expect(run).toHaveBeenCalledTimes(1);
    });

    it('プロキシ由来の 5xx（520等）もリトライする', async () => {
      const run = vi
        .fn()
        .mockResolvedValueOnce({ data: null, error: { message: 'Unknown Error' }, status: 520 })
        .mockResolvedValue({ data: ['ok'], error: null, status: 200 });

      const resultPromise = withPostgrestRetry(run, {
        maxRetries: 2,
        baseDelayMs: 100,
        jitterMs: 0,
      });

      await vi.advanceTimersByTimeAsync(100);
      const result = await resultPromise;

      expect(result.error).toBeNull();
      expect(run).toHaveBeenCalledTimes(2);
    });

    it('retryStatusCodes を明示するとそのリストのみ対象になる', async () => {
      const run = vi
        .fn()
        .mockResolvedValue({ data: null, error: { message: 'Gateway Timeout' }, status: 504 });

      const result = await withPostgrestRetry(run, {
        maxRetries: 2,
        baseDelayMs: 100,
        jitterMs: 0,
        retryStatusCodes: [429],
      });

      expect(result.error?.message).toBe('Gateway Timeout');
      expect(run).toHaveBeenCalledTimes(1);
    });

    it('isTransientPostgrestStatus はゲートウェイ由来だけを一時的とみなす', () => {
      for (const status of [0, 408, 429, 502, 503, 504, 520, 524]) {
        expect(isTransientPostgrestStatus(status)).toBe(true);
      }
      // 500 は SQL 例外（statement timeout 等）でも返るので投げ直さない
      for (const status of [200, 400, 401, 404, 409, 422, 500]) {
        expect(isTransientPostgrestStatus(status)).toBe(false);
      }
      expect(TRANSIENT_POSTGREST_STATUS_CODES).toContain(504);
    });
  });
  describe('createRetryingFetch', () => {
    function response(status: number): Response {
      return new Response(status === 204 ? null : 'body', { status });
    }

    it('GET の 504 は投げ直す', async () => {
      const fetchImpl = vi.fn().mockResolvedValueOnce(response(504)).mockResolvedValue(response(200));
      const retryingFetch = createRetryingFetch({ fetchImpl, baseDelayMs: 0 });

      const res = await retryingFetch('https://example.invalid/rest/v1/x');
      expect(res.status).toBe(200);
      expect(fetchImpl).toHaveBeenCalledTimes(2);
    });

    it('upsert（Prefer: resolution=merge-duplicates）は投げ直す', async () => {
      const fetchImpl = vi.fn().mockResolvedValueOnce(response(503)).mockResolvedValue(response(201));
      const retryingFetch = createRetryingFetch({
        fetchImpl,
        baseDelayMs: 0,
        retryIdempotentWrites: true,
      });

      const res = await retryingFetch('https://example.invalid/rest/v1/macro_indicator_daily', {
        method: 'POST',
        headers: { Prefer: 'return=minimal,resolution=merge-duplicates' },
        body: '[{"a":1}]',
      });
      expect(res.status).toBe(201);
      expect(fetchImpl).toHaveBeenCalledTimes(2);
    });

    it('PATCH（update）は投げ直す', async () => {
      const fetchImpl = vi.fn().mockResolvedValueOnce(response(504)).mockResolvedValue(response(204));
      const retryingFetch = createRetryingFetch({
        fetchImpl,
        baseDelayMs: 0,
        retryIdempotentWrites: true,
      });

      const res = await retryingFetch('https://example.invalid/rest/v1/job_runs?run_id=eq.1', {
        method: 'PATCH',
        body: '{"status":"success"}',
      });
      expect(res.status).toBe(204);
      expect(fetchImpl).toHaveBeenCalledTimes(2);
    });

    it('既定では upsert / update も投げ直さない（読み取り専用）', async () => {
      const fetchImpl = vi.fn().mockResolvedValue(response(504));
      const retryingFetch = createRetryingFetch({ fetchImpl, baseDelayMs: 0 });

      const upsert = await retryingFetch('https://example.invalid/rest/v1/x', {
        method: 'POST',
        headers: { Prefer: 'resolution=merge-duplicates' },
        body: '[{"a":1}]',
      });
      const patch = await retryingFetch('https://example.invalid/rest/v1/x?id=eq.1', {
        method: 'PATCH',
        body: '{"a":1}',
      });

      expect(upsert.status).toBe(504);
      expect(patch.status).toBe(504);
      expect(fetchImpl).toHaveBeenCalledTimes(2);
    });

    it('素の POST（insert・RPC）は投げ直さない', async () => {
      const fetchImpl = vi.fn().mockResolvedValue(response(504));
      const retryingFetch = createRetryingFetch({ fetchImpl, baseDelayMs: 0 });

      const res = await retryingFetch('https://example.invalid/rest/v1/rpc/claim_job_run', {
        method: 'POST',
        headers: { Prefer: 'return=representation' },
        body: '{}',
      });
      expect(res.status).toBe(504);
      expect(fetchImpl).toHaveBeenCalledTimes(1);
    });

    it('GET の 500 は投げ直さない（SQL例外を隠さない）', async () => {
      const fetchImpl = vi.fn().mockResolvedValue(response(500));
      const retryingFetch = createRetryingFetch({ fetchImpl, baseDelayMs: 0 });

      const res = await retryingFetch('https://example.invalid/rest/v1/x');
      expect(res.status).toBe(500);
      expect(fetchImpl).toHaveBeenCalledTimes(1);
    });

    it('GET の経路例外は投げ直し、使い切ったら rethrow する', async () => {
      const fetchImpl = vi.fn().mockRejectedValue(new Error('fetch failed'));
      const retryingFetch = createRetryingFetch({ fetchImpl, baseDelayMs: 0, maxAttempts: 3 });

      await expect(retryingFetch('https://example.invalid/rest/v1/x')).rejects.toThrow('fetch failed');
      expect(fetchImpl).toHaveBeenCalledTimes(3);
    });

    it('本文つき Request は投げ直さない（1回目で消費済みのため）', async () => {
      const fetchImpl = vi.fn().mockResolvedValue(response(503));
      const retryingFetch = createRetryingFetch({
        fetchImpl,
        baseDelayMs: 0,
        retryIdempotentWrites: true,
      });

      const request = new Request('https://example.invalid/rest/v1/job_runs', {
        method: 'PATCH',
        body: '{"status":"success"}',
      });
      const res = await retryingFetch(request);

      expect(res.status).toBe(503);
      expect(fetchImpl).toHaveBeenCalledTimes(1);
    });

    it('AbortError は投げ直さない', async () => {
      const abortError = new Error('This operation was aborted');
      abortError.name = 'AbortError';
      const fetchImpl = vi.fn().mockRejectedValue(abortError);
      const retryingFetch = createRetryingFetch({ fetchImpl, baseDelayMs: 0 });

      await expect(retryingFetch('https://example.invalid/rest/v1/x')).rejects.toThrow(
        'This operation was aborted'
      );
      expect(fetchImpl).toHaveBeenCalledTimes(1);
    });

    it('経路由来でない例外は投げ直さない', async () => {
      const fetchImpl = vi.fn().mockRejectedValue(new TypeError('Invalid URL'));
      const retryingFetch = createRetryingFetch({ fetchImpl, baseDelayMs: 0 });

      await expect(retryingFetch('https://example.invalid/rest/v1/x')).rejects.toThrow('Invalid URL');
      expect(fetchImpl).toHaveBeenCalledTimes(1);
    });

    it('本文がストリームなら投げ直さない（再送できないため）', async () => {
      const fetchImpl = vi.fn().mockResolvedValue(response(504));
      const retryingFetch = createRetryingFetch({
        fetchImpl,
        baseDelayMs: 0,
        retryIdempotentWrites: true,
      });

      const res = await retryingFetch('https://example.invalid/rest/v1/x', {
        method: 'POST',
        headers: { Prefer: 'resolution=merge-duplicates' },
        body: new ReadableStream(),
      });
      expect(res.status).toBe(504);
      expect(fetchImpl).toHaveBeenCalledTimes(1);
    });
  });
});
