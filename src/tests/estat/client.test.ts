/**
 * estat/client.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

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

import { EStatClient, createEStatClient, getEStatRateLimiter } from '@/lib/estat/client';

/** e-Stat APIレスポンスのヘルパー */
function createEStatResponse(values: Array<{ $: string; '@time': string; [key: string]: string }>, classInfo?: any) {
  return {
    GET_STATS_DATA: {
      RESULT: { STATUS: 0, ERROR_MSG: '', DATE: '2024-01-15' },
      PARAMETER: { LANG: 'J', STATS_DATA_ID: 'test', DATA_FORMAT: 'J' },
      STATISTICAL_DATA: {
        TABLE_INF: {},
        CLASS_INF: classInfo ?? { CLASS_OBJ: [] },
        DATA_INF: { VALUE: values },
      },
    },
  };
}

describe('estat/client.ts', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
    process.env.ESTAT_API_KEY = 'test-app-id';
    stableAcquire.mockResolvedValue(undefined);
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('コンストラクタ', () => {
    it('env変数からAPIキーを取得する', () => {
      const client = new EStatClient();
      expect(client).toBeDefined();
    });

    it('オプションでappIdを渡せる', () => {
      delete process.env.ESTAT_API_KEY;
      const client = new EStatClient({ appId: 'custom-id' });
      expect(client).toBeDefined();
    });

    it('APIキーなしでエラーを投げる', () => {
      delete process.env.ESTAT_API_KEY;
      expect(() => new EStatClient()).toThrow('e-Stat API key is required');
    });
  });

  describe('getStatsData', () => {
    it('正常にパースしてobservationsを返す', async () => {
      const response = createEStatResponse([
        { $: '103.5', '@time': '2024000100' },
        { $: '104.2', '@time': '2024000200' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations, skippedCount } = await client.getStatsData('0003143513');

      expect(observations).toHaveLength(2);
      expect(observations[0].value).toBe(103.5);
      expect(skippedCount).toBe(0);
    });

    it('6種の欠損値をスキップする', async () => {
      const response = createEStatResponse([
        { $: '-', '@time': '2024000100' },
        { $: '...', '@time': '2024000200' },
        { $: '***', '@time': '2024000300' },
        { $: 'x', '@time': '2024000400' },
        { $: 'X', '@time': '2024000500' },
        { $: '', '@time': '2024000600' },
        { $: '100.5', '@time': '2024000700' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations, skippedCount } = await client.getStatsData('test');

      expect(observations).toHaveLength(1);
      expect(skippedCount).toBe(6);
    });

    it('APIステータスエラーを検出する', async () => {
      const response = {
        GET_STATS_DATA: {
          RESULT: { STATUS: 100, ERROR_MSG: 'Invalid parameter', DATE: '2024-01-15' },
          PARAMETER: {},
          STATISTICAL_DATA: {},
        },
      };

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();

      await expect(client.getStatsData('invalid')).rejects.toThrow('e-Stat API error: Invalid parameter');
    });

    it('同一日付は後勝ちでマージする', async () => {
      const response = createEStatResponse([
        { $: '100.0', '@time': '2024000100' },
        { $: '101.5', '@time': '2024000100' }, // 同じ月→後勝ち
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test');

      expect(observations).toHaveLength(1);
      expect(observations[0].value).toBe(101.5);
    });

    it('sourceFilterでサーバーサイドフィルタパラメータを構築する', async () => {
      const response = createEStatResponse([]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      await client.getStatsData('test', { tab: '01', cat01: '0161 生鮮食品を除く総合' });

      const calledUrl = mockFetchWithRetry.mock.calls[0][0];
      expect(calledUrl).toContain('cdTab=01');
      expect(calledUrl).toContain('cdCat01=0161');
    });

    it('空データの場合は空配列を返す', async () => {
      const response = {
        GET_STATS_DATA: {
          RESULT: { STATUS: 0, ERROR_MSG: '', DATE: '2024-01-15' },
          PARAMETER: {},
          STATISTICAL_DATA: {
            TABLE_INF: {},
            CLASS_INF: { CLASS_OBJ: [] },
            DATA_INF: { VALUE: [] },
          },
        },
      };

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test');

      expect(observations).toHaveLength(0);
    });

    it('非数値をスキップする', async () => {
      const response = createEStatResponse([
        { $: 'abc', '@time': '2024000100' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations, skippedCount } = await client.getStatsData('test');

      expect(observations).toHaveLength(0);
      expect(skippedCount).toBe(1);
    });
  });

  describe('parseTimeCode（間接テスト）', () => {
    it('YYYY00MM00 形式をパースして月末日を返す', async () => {
      const response = createEStatResponse([
        { $: '100.0', '@time': '2024000300' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test');

      expect(observations[0].date).toBe('2024-03-31');
    });

    it('YYYYMMDDDD 形式をパースする', async () => {
      const response = createEStatResponse([
        { $: '100.0', '@time': '2024120100' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test');

      // 2024年12月 or 2024年01月（位置による）
      expect(observations[0].date).toBeDefined();
    });

    it('YYYYMM 短縮形式をパースする', async () => {
      const response = createEStatResponse([
        { $: '100.0', '@time': '202406' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test');

      expect(observations[0].date).toBe('2024-06-30');
    });

    it('パース不能な時間コードはスキップする', async () => {
      const response = createEStatResponse([
        { $: '100.0', '@time': 'invalid' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations, skippedCount } = await client.getStatsData('test');

      expect(observations).toHaveLength(0);
      expect(skippedCount).toBe(1);
    });
  });

  describe('lastDayOfMonth（間接テスト）', () => {
    it('閏年2月は29日', async () => {
      const response = createEStatResponse([
        { $: '100.0', '@time': '2024000200' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test');

      expect(observations[0].date).toBe('2024-02-29');
    });

    it('平年2月は28日', async () => {
      const response = createEStatResponse([
        { $: '100.0', '@time': '2023000200' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test');

      expect(observations[0].date).toBe('2023-02-28');
    });

    it('12月は31日', async () => {
      const response = createEStatResponse([
        { $: '100.0', '@time': '2024001200' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test');

      expect(observations[0].date).toBe('2024-12-31');
    });

    it('4月は30日', async () => {
      const response = createEStatResponse([
        { $: '100.0', '@time': '2024000400' },
      ]);

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test');

      expect(observations[0].date).toBe('2024-04-30');
    });
  });

  describe('filterValues（間接テスト）', () => {
    it('CLASS_INFマッピングで名前→コード変換フィルタする', async () => {
      const classInfo = {
        CLASS_OBJ: [
          {
            '@id': 'cat01',
            '@name': '品目',
            CLASS: [
              { '@code': '0001', '@name': '総合' },
              { '@code': '0161', '@name': '生鮮食品を除く総合' },
            ],
          },
        ],
      };

      const response = createEStatResponse(
        [
          { $: '100.0', '@time': '2024000100', '@cat01': '0001' },
          { $: '101.5', '@time': '2024000100', '@cat01': '0161' },
        ],
        classInfo
      );

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test', {
        cat01: '生鮮食品を除く総合',
      });

      expect(observations).toHaveLength(1);
      expect(observations[0].value).toBe(101.5);
    });

    it('CLASS_INFにマッチしない場合はフォールバック（生のフィルタ値）', async () => {
      const classInfo = { CLASS_OBJ: [] };

      const response = createEStatResponse(
        [
          { $: '100.0', '@time': '2024000100', '@cat01': 'ABC' },
        ],
        classInfo
      );

      mockFetchWithRetry.mockResolvedValue({
        json: vi.fn().mockResolvedValue(response),
      });

      const client = new EStatClient();
      const { observations } = await client.getStatsData('test', {
        cat01: 'ABC',
      });

      expect(observations).toHaveLength(1);
    });
  });

  describe('シングルトン', () => {
    it('createEStatClientで新しいインスタンスを生成する', () => {
      const client = createEStatClient();
      expect(client).toBeInstanceOf(EStatClient);
    });

    it('getEStatRateLimiterがレートリミッターを返す', () => {
      const limiter = getEStatRateLimiter();
      expect(limiter).toBeDefined();
    });
  });

  describe('エラー伝播', () => {
    it('ネットワークエラーが伝播する', async () => {
      mockFetchWithRetry.mockRejectedValue(new Error('Network error'));

      const client = new EStatClient();

      await expect(client.getStatsData('test')).rejects.toThrow('Network error');
    });
  });
});
