/**
 * fred/client.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// mockReset: true に対応: vi.hoisted() 内で参照を保持
// RateLimiterコンストラクタは安定したオブジェクトを返す（リセットされない）
const { stableAcquire, mockFetchWithRetry } = vi.hoisted(() => {
  const acquire = vi.fn().mockResolvedValue(undefined);
  return {
    stableAcquire: acquire,
    mockFetchWithRetry: vi.fn(),
  };
});

vi.mock('@/lib/jquants/rate-limiter', () => ({
  RateLimiter: class {
    acquire = stableAcquire;
  },
}));

vi.mock('@/lib/utils/retry', () => ({
  fetchWithRetry: mockFetchWithRetry,
  NonRetryableError: class NonRetryableError extends Error {
    statusCode: number;
    constructor(message: string, statusCode: number) {
      super(message);
      this.name = 'NonRetryableError';
      this.statusCode = statusCode;
    }
  },
}));

vi.mock('@/lib/utils/logger', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    startTimer: vi.fn(() => ({ end: vi.fn(), endWithError: vi.fn() })),
  })),
}));

import { FredClient, createFredClient, getFredRateLimiter } from '@/lib/fred/client';

describe('fred/client.ts', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
    process.env.FRED_API_KEY = 'test-api-key';
    // mockReset: true でモック実装がクリアされるため、beforeEach で再設定
    stableAcquire.mockResolvedValue(undefined);
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('コンストラクタ', () => {
    it('env変数からAPIキーを取得する', () => {
      const client = new FredClient();
      expect(client).toBeDefined();
    });

    it('オプションでAPIキーを渡せる', () => {
      delete process.env.FRED_API_KEY;
      const client = new FredClient({ apiKey: 'custom-key' });
      expect(client).toBeDefined();
    });

    it('APIキーなしでエラーを投げる', () => {
      delete process.env.FRED_API_KEY;

      expect(() => new FredClient()).toThrow('FRED API key is required');
    });

    it('タイムアウトをカスタマイズできる', () => {
      const client = new FredClient({ timeoutMs: 5000 });
      expect(client).toBeDefined();
    });
  });

  describe('getSeriesObservations', () => {
    it('正常にパースしてobservationsを返す', async () => {
      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue({
          observations: [
            { date: '2024-01-10', value: '13.5', realtime_start: '2024-01-11' },
            { date: '2024-01-11', value: '14.2', realtime_start: '2024-01-12' },
          ],
        }),
      });

      const client = new FredClient();
      const { observations, skippedCount } = await client.getSeriesObservations('VIXCLS');

      expect(observations).toHaveLength(2);
      expect(observations[0]).toEqual({
        date: '2024-01-10',
        value: 13.5,
        releasedAt: '2024-01-11T00:00:00Z',
      });
      expect(skippedCount).toBe(0);
    });

    it('欠損値 "." をスキップする', async () => {
      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue({
          observations: [
            { date: '2024-01-10', value: '.', realtime_start: '2024-01-11' },
            { date: '2024-01-11', value: '14.2', realtime_start: '2024-01-12' },
          ],
        }),
      });

      const client = new FredClient();
      const { observations, skippedCount } = await client.getSeriesObservations('VIXCLS');

      expect(observations).toHaveLength(1);
      expect(skippedCount).toBe(1);
    });

    it('非数値をスキップする', async () => {
      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue({
          observations: [
            { date: '2024-01-10', value: 'N/A', realtime_start: '2024-01-11' },
          ],
        }),
      });

      const client = new FredClient();
      const { observations, skippedCount } = await client.getSeriesObservations('VIXCLS');

      expect(observations).toHaveLength(0);
      expect(skippedCount).toBe(1);
    });

    it('releasedAtを正しく構築する', async () => {
      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue({
          observations: [
            { date: '2024-06-15', value: '3.25', realtime_start: '2024-06-16' },
          ],
        }),
      });

      const client = new FredClient();
      const { observations } = await client.getSeriesObservations('FEDFUNDS');

      expect(observations[0].releasedAt).toBe('2024-06-16T00:00:00Z');
    });

    it('空データを正常に処理する', async () => {
      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue({
          observations: [],
        }),
      });

      const client = new FredClient();
      const { observations, skippedCount } = await client.getSeriesObservations('VIXCLS');

      expect(observations).toHaveLength(0);
      expect(skippedCount).toBe(0);
    });

    it('observationStartとobservationEndを渡せる', async () => {
      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue({ observations: [] }),
      });

      const client = new FredClient();
      await client.getSeriesObservations('VIXCLS', '2024-01-01', '2024-01-31');

      // fetchWithRetryが呼ばれたURLにパラメータが含まれることを確認
      const calledUrl = mockFetchWithRetry.mock.calls[0][0];
      expect(calledUrl).toContain('observation_start=2024-01-01');
      expect(calledUrl).toContain('observation_end=2024-01-31');
    });

    it('エラーが伝播する', async () => {
      mockFetchWithRetry.mockRejectedValue(new Error('Network error'));

      const client = new FredClient();

      await expect(
        client.getSeriesObservations('VIXCLS')
      ).rejects.toThrow('Network error');
    });
  });

  describe('シングルトン', () => {
    it('createFredClientで新しいインスタンスを生成する', () => {
      const client = createFredClient();
      expect(client).toBeInstanceOf(FredClient);
    });

    it('getFredRateLimiterがレートリミッターを返す', () => {
      const limiter = getFredRateLimiter();
      expect(limiter).toBeDefined();
    });
  });
});
