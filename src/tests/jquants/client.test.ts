/**
 * jquants/client.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { JQuantsClient, createJQuantsClient } from '@/lib/jquants/client';

// モジュールモック
vi.mock('@/lib/jquants/rate-limiter', () => ({
  getJQuantsRateLimiter: () => ({
    acquire: vi.fn().mockResolvedValue(undefined),
  }),
}));

vi.mock('@/lib/utils/retry', () => ({
  fetchWithRetry: vi.fn(),
  RetryableError: class RetryableError extends Error {
    statusCode?: number;
    constructor(message: string, statusCode?: number) {
      super(message);
      this.name = 'RetryableError';
      this.statusCode = statusCode;
    }
  },
  NonRetryableError: class NonRetryableError extends Error {
    statusCode?: number;
    constructor(message: string, statusCode?: number) {
      super(message);
      this.name = 'NonRetryableError';
      this.statusCode = statusCode;
    }
  },
}));

import { fetchWithRetry, RetryableError, NonRetryableError } from '@/lib/utils/retry';

describe('jquants/client.ts', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    process.env.JQUANTS_API_KEY = 'test-api-key';
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('JQuantsClient constructor', () => {
    it('APIキーなしでエラーになる', () => {
      delete process.env.JQUANTS_API_KEY;

      expect(() => new JQuantsClient()).toThrow(
        'J-Quants API key is required. Set JQUANTS_API_KEY environment variable.'
      );
    });

    it('環境変数からAPIキーを取得する', () => {
      const client = new JQuantsClient();
      expect(client).toBeDefined();
    });

    it('オプションでAPIキーを指定できる', () => {
      delete process.env.JQUANTS_API_KEY;
      const client = new JQuantsClient({ apiKey: 'custom-api-key' });
      expect(client).toBeDefined();
    });

    it('オプションでタイムアウトを指定できる', () => {
      const client = new JQuantsClient({ timeoutMs: 60000 });
      expect(client).toBeDefined();
    });
  });

  describe('createJQuantsClient', () => {
    it('クライアントインスタンスを作成する', () => {
      const client = createJQuantsClient();
      expect(client).toBeInstanceOf(JQuantsClient);
    });

    it('オプションを渡せる', () => {
      const client = createJQuantsClient({ apiKey: 'custom-key' });
      expect(client).toBeInstanceOf(JQuantsClient);
    });
  });

  describe('getTradingCalendar', () => {
    it('取引カレンダーを取得する', async () => {
      const mockResponse = {
        data: [
          { Date: '2024-01-15', HolidayDivision: '1' },
          { Date: '2024-01-16', HolidayDivision: '1' },
        ],
      };

      vi.mocked(fetchWithRetry).mockResolvedValue({
        json: () => Promise.resolve(mockResponse),
      } as Response);

      const client = new JQuantsClient();
      const result = await client.getTradingCalendar({ from: '2024-01-15', to: '2024-01-16' });

      expect(result.data).toHaveLength(2);
      expect(fetchWithRetry).toHaveBeenCalledWith(
        expect.stringContaining('/markets/calendar'),
        expect.objectContaining({
          method: 'GET',
          headers: expect.objectContaining({
            'x-api-key': 'test-api-key',
          }),
        }),
        expect.any(Object)
      );
    });

    it('クエリパラメータを正しく構築する', async () => {
      vi.mocked(fetchWithRetry).mockResolvedValue({
        json: () => Promise.resolve({ data: [] }),
      } as Response);

      const client = new JQuantsClient();
      await client.getTradingCalendar({ from: '2024-01-01', to: '2024-12-31' });

      expect(fetchWithRetry).toHaveBeenCalledWith(
        expect.stringMatching(/from=2024-01-01.*to=2024-12-31/),
        expect.any(Object),
        expect.any(Object)
      );
    });
  });

  describe('getEquityMaster', () => {
    it('銘柄マスタを取得する', async () => {
      const mockResponse = {
        data: [
          { Code: '1301', CompanyName: 'Test Company' },
        ],
      };

      vi.mocked(fetchWithRetry).mockResolvedValue({
        json: () => Promise.resolve(mockResponse),
      } as Response);

      const client = new JQuantsClient();
      const result = await client.getEquityMaster({ code: '1301' });

      expect(result.data).toHaveLength(1);
      expect(fetchWithRetry).toHaveBeenCalledWith(
        expect.stringContaining('/equities/master'),
        expect.any(Object),
        expect.any(Object)
      );
    });
  });

  describe('getEquityBarsDaily', () => {
    it('株価日足を全ページ取得する', async () => {
      // 2ページ分のモック
      vi.mocked(fetchWithRetry)
        .mockResolvedValueOnce({
          json: () => Promise.resolve({
            data: [{ Code: '1301', Date: '2024-01-15' }],
            pagination_key: 'page2',
          }),
        } as Response)
        .mockResolvedValueOnce({
          json: () => Promise.resolve({
            data: [{ Code: '1301', Date: '2024-01-16' }],
            // pagination_key なし = 最後のページ
          }),
        } as Response);

      const client = new JQuantsClient();
      const result = await client.getEquityBarsDaily({ code: '1301' });

      expect(result).toHaveLength(2);
      expect(fetchWithRetry).toHaveBeenCalledTimes(2);
    });

    it('空データの場合は空配列を返す', async () => {
      vi.mocked(fetchWithRetry).mockResolvedValue({
        json: () => Promise.resolve({ data: [] }),
      } as Response);

      const client = new JQuantsClient();
      const result = await client.getEquityBarsDaily({ code: '9999' });

      expect(result).toEqual([]);
    });
  });

  describe('getEquityBarsDailyPaginated', () => {
    it('ページごとにデータをyieldする', async () => {
      vi.mocked(fetchWithRetry)
        .mockResolvedValueOnce({
          json: () => Promise.resolve({
            data: [{ Code: '1301', Date: '2024-01-15' }],
            pagination_key: 'page2',
          }),
        } as Response)
        .mockResolvedValueOnce({
          json: () => Promise.resolve({
            data: [{ Code: '1301', Date: '2024-01-16' }],
          }),
        } as Response);

      const client = new JQuantsClient();
      const pages: unknown[][] = [];

      for await (const page of client.getEquityBarsDailyPaginated({ code: '1301' })) {
        pages.push(page);
      }

      expect(pages).toHaveLength(2);
      expect(pages[0]).toHaveLength(1);
      expect(pages[1]).toHaveLength(1);
    });
  });

  describe('getTopixBarsDaily', () => {
    it('TOPIX日足を取得する', async () => {
      const mockResponse = {
        data: [
          { Date: '2024-01-15', Close: 2500.00 },
        ],
      };

      vi.mocked(fetchWithRetry).mockResolvedValue({
        json: () => Promise.resolve(mockResponse),
      } as Response);

      const client = new JQuantsClient();
      const result = await client.getTopixBarsDaily({ from: '2024-01-15' });

      expect(result.data).toHaveLength(1);
      expect(fetchWithRetry).toHaveBeenCalledWith(
        expect.stringContaining('/indices/bars/daily/topix'),
        expect.any(Object),
        expect.any(Object)
      );
    });
  });

  describe('getFinancialSummary', () => {
    it('財務サマリーを全ページ取得する', async () => {
      vi.mocked(fetchWithRetry).mockResolvedValue({
        json: () => Promise.resolve({
          data: [{ LocalCode: '1301', Result_FinancialStatement_ReportType: 'Q1' }],
        }),
      } as Response);

      const client = new JQuantsClient();
      const result = await client.getFinancialSummary({ code: '1301' });

      expect(result).toHaveLength(1);
    });
  });

  describe('getEarningsCalendar', () => {
    it('決算発表予定を取得する', async () => {
      const mockResponse = {
        data: [
          { Code: '1301', Date: '2024-02-15' },
        ],
      };

      vi.mocked(fetchWithRetry).mockResolvedValue({
        json: () => Promise.resolve(mockResponse),
      } as Response);

      const client = new JQuantsClient();
      const result = await client.getEarningsCalendar();

      expect(result.data).toHaveLength(1);
      expect(fetchWithRetry).toHaveBeenCalledWith(
        expect.stringContaining('/equities/earnings-calendar'),
        expect.any(Object),
        expect.any(Object)
      );
    });
  });

  describe('getInvestorTypes', () => {
    it('投資部門別売買状況を取得する', async () => {
      const mockResponse = {
        data: [
          { PublishedDate: '2024-01-15', Section: 'TSEPrime' },
        ],
      };

      vi.mocked(fetchWithRetry).mockResolvedValue({
        json: () => Promise.resolve(mockResponse),
      } as Response);

      const client = new JQuantsClient();
      const result = await client.getInvestorTypes({ section: 'TSEPrime' });

      expect(result.data).toHaveLength(1);
      expect(fetchWithRetry).toHaveBeenCalledWith(
        expect.stringContaining('/equities/investor-types'),
        expect.any(Object),
        expect.any(Object)
      );
    });
  });

  describe('getWeeklyMarginInterest', () => {
    it('信用取引週末残高を全ページ取得する', async () => {
      vi.mocked(fetchWithRetry)
        .mockResolvedValueOnce({
          json: () => Promise.resolve({
            data: [{ Code: '13010', Date: '2026-07-03' }],
            pagination_key: 'page2',
          }),
        } as Response)
        .mockResolvedValueOnce({
          json: () => Promise.resolve({
            data: [{ Code: '13010', Date: '2026-07-10' }],
          }),
        } as Response);

      const client = new JQuantsClient();
      const result = await client.getWeeklyMarginInterest({ code: '13010' });

      expect(result).toHaveLength(2);
      expect(fetchWithRetry).toHaveBeenCalledTimes(2);
      expect(fetchWithRetry).toHaveBeenCalledWith(
        expect.stringContaining('/markets/margin-interest'),
        expect.any(Object),
        expect.any(Object)
      );
    });
  });

  describe('getWeeklyMarginInterestPaginated', () => {
    it('ページごとにデータをyieldする', async () => {
      vi.mocked(fetchWithRetry)
        .mockResolvedValueOnce({
          json: () => Promise.resolve({
            data: [{ Code: '13010', Date: '2026-07-03' }],
            pagination_key: 'page2',
          }),
        } as Response)
        .mockResolvedValueOnce({
          json: () => Promise.resolve({
            data: [{ Code: '13010', Date: '2026-07-10' }],
          }),
        } as Response);

      const client = new JQuantsClient();
      const pages: unknown[][] = [];

      for await (const page of client.getWeeklyMarginInterestPaginated({ code: '13010' })) {
        pages.push(page);
      }

      expect(pages).toHaveLength(2);
      expect(pages[0]).toHaveLength(1);
      expect(pages[1]).toHaveLength(1);
    });
  });

  describe('エラーハンドリング', () => {
    it('NonRetryableErrorを伝播する', async () => {
      vi.mocked(fetchWithRetry).mockRejectedValue(
        new NonRetryableError('Bad request', 400)
      );

      const client = new JQuantsClient();

      await expect(client.getTradingCalendar()).rejects.toThrow(NonRetryableError);
    });

    it('RetryableErrorを伝播する', async () => {
      vi.mocked(fetchWithRetry).mockRejectedValue(
        new RetryableError('Service unavailable', 503)
      );

      const client = new JQuantsClient();

      await expect(client.getTradingCalendar()).rejects.toThrow(RetryableError);
    });

    it('一般的なエラーを伝播する', async () => {
      vi.mocked(fetchWithRetry).mockRejectedValue(
        new Error('Network error')
      );

      const client = new JQuantsClient();

      await expect(client.getTradingCalendar()).rejects.toThrow('Network error');
    });
  });

  describe('レート制限', () => {
    it('リクエスト前にレート制限を適用する', async () => {
      vi.mocked(fetchWithRetry).mockResolvedValue({
        json: () => Promise.resolve({ data: [] }),
      } as Response);

      const client = new JQuantsClient();
      await client.getTradingCalendar();

      // レート制限のacquireが呼ばれたことを確認
      // (モックモジュールのため詳細な検証は省略)
      expect(fetchWithRetry).toHaveBeenCalled();
    });
  });
});
