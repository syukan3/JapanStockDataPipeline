/**
 * cron/handlers/cron-a.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const {
  mockSyncTradingCalendarRange,
  mockSyncEquityBarsDailyForDate,
  mockSyncTopixBarsDailyForDate,
  mockSyncFinancialSummaryForDate,
  mockSyncEquityMasterSCD,
  mockDetermineTargetDates,
  mockCreateAdminClient,
  mockSendJobFailureEmail,
  mockGetJSTDate,
} = vi.hoisted(() => ({
  mockSyncTradingCalendarRange: vi.fn(),
  mockSyncEquityBarsDailyForDate: vi.fn(),
  mockSyncTopixBarsDailyForDate: vi.fn(),
  mockSyncFinancialSummaryForDate: vi.fn(),
  mockSyncEquityMasterSCD: vi.fn(),
  mockDetermineTargetDates: vi.fn(),
  mockCreateAdminClient: vi.fn(),
  mockSendJobFailureEmail: vi.fn(),
  mockGetJSTDate: vi.fn(),
}));

vi.mock('@/lib/jquants/endpoints/trading-calendar', () => ({
  syncTradingCalendarRange: mockSyncTradingCalendarRange,
}));

vi.mock('@/lib/jquants/endpoints/equity-bars-daily', () => ({
  syncEquityBarsDailyForDate: mockSyncEquityBarsDailyForDate,
}));

vi.mock('@/lib/jquants/endpoints/index-topix', () => ({
  syncTopixBarsDailyForDate: mockSyncTopixBarsDailyForDate,
}));

vi.mock('@/lib/jquants/endpoints/fins-summary', () => ({
  syncFinancialSummaryForDate: mockSyncFinancialSummaryForDate,
}));

vi.mock('@/lib/jquants/endpoints/equity-master', () => ({
  syncEquityMasterSCD: mockSyncEquityMasterSCD,
}));

vi.mock('@/lib/cron/catch-up', () => ({
  determineTargetDates: mockDetermineTargetDates,
}));

vi.mock('@/lib/supabase/admin', () => ({
  createAdminClient: mockCreateAdminClient,
}));

vi.mock('@/lib/notification/email', () => ({
  sendJobFailureEmail: mockSendJobFailureEmail,
}));

vi.mock('@/lib/utils/date', () => ({
  getJSTDate: mockGetJSTDate,
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

import { handleCronA, CronARequestSchema, CRON_A_DATASETS } from '@/lib/cron/handlers/cron-a';

describe('cron/handlers/cron-a.ts', () => {
  beforeEach(() => {
    mockSyncTradingCalendarRange.mockReset();
    mockSyncEquityBarsDailyForDate.mockReset();
    mockSyncTopixBarsDailyForDate.mockReset();
    mockSyncFinancialSummaryForDate.mockReset();
    mockSyncEquityMasterSCD.mockReset();
    mockDetermineTargetDates.mockReset();
    mockCreateAdminClient.mockReturnValue({});
    mockSendJobFailureEmail.mockReset();
    mockGetJSTDate.mockReturnValue('2024-01-15');
  });

  describe('CronARequestSchema', () => {
    it('有効なデータセットを受け付ける', () => {
      const result = CronARequestSchema.safeParse({ dataset: 'equity_bars' });
      expect(result.success).toBe(true);
    });

    it('無効なデータセットを拒否する', () => {
      const result = CronARequestSchema.safeParse({ dataset: 'invalid' });
      expect(result.success).toBe(false);
    });

    it('全5データセットが定義されている', () => {
      expect(CRON_A_DATASETS).toHaveLength(5);
      expect(CRON_A_DATASETS).toContain('calendar');
      expect(CRON_A_DATASETS).toContain('equity_bars');
    });
  });

  describe('calendarデータセット', () => {
    it('syncTradingCalendarRangeを呼び出す', async () => {
      mockSyncTradingCalendarRange.mockResolvedValue({
        fetched: 740,
        inserted: 740,
      });

      const result = await handleCronA('calendar', 'run-123');

      expect(result.success).toBe(true);
      expect(result.dataset).toBe('calendar');
      expect(result.fetched).toBe(740);
      expect(mockSyncTradingCalendarRange).toHaveBeenCalled();
    });

    it('キャッチアップをスキップする（determineTargetDatesを呼ばない）', async () => {
      mockSyncTradingCalendarRange.mockResolvedValue({
        fetched: 100,
        inserted: 100,
      });

      await handleCronA('calendar', 'run-123');

      expect(mockDetermineTargetDates).not.toHaveBeenCalled();
    });
  });

  describe('equity_barsデータセット', () => {
    it('正しいsync関数をディスパッチする', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncEquityBarsDailyForDate.mockResolvedValue({
        fetched: 5000,
        inserted: 5000,
        pageCount: 3,
      });

      const result = await handleCronA('equity_bars', 'run-123');

      expect(result.success).toBe(true);
      expect(result.dataset).toBe('equity_bars');
      expect(result.targetDate).toBe('2024-01-12');
      expect(mockSyncEquityBarsDailyForDate).toHaveBeenCalledWith('2024-01-12', expect.any(Object));
    });
  });

  describe('topixデータセット', () => {
    it('正しいsync関数をディスパッチする', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncTopixBarsDailyForDate.mockResolvedValue({
        fetched: 10,
        inserted: 10,
      });

      const result = await handleCronA('topix', 'run-123');

      expect(result.success).toBe(true);
      expect(mockSyncTopixBarsDailyForDate).toHaveBeenCalledWith('2024-01-12', expect.any(Object));
    });
  });

  describe('financialデータセット', () => {
    it('正しいsync関数をディスパッチする', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncFinancialSummaryForDate.mockResolvedValue({
        fetched: 200,
        inserted: 200,
        pageCount: 1,
      });

      const result = await handleCronA('financial', 'run-123');

      expect(result.success).toBe(true);
      expect(mockSyncFinancialSummaryForDate).toHaveBeenCalledWith('2024-01-12', expect.any(Object));
    });
  });

  describe('equity_masterデータセット', () => {
    it('正しいsync関数をディスパッチする', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncEquityMasterSCD.mockResolvedValue({
        fetched: 4000,
        inserted: 50,
        updated: 10,
        delisted: 2,
        errors: [],
      });

      const result = await handleCronA('equity_master', 'run-123');

      expect(result.success).toBe(true);
      expect(mockSyncEquityMasterSCD).toHaveBeenCalledWith('2024-01-12', expect.any(Object));
    });

    it('inserted + updatedを合算する', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncEquityMasterSCD.mockResolvedValue({
        fetched: 4000,
        inserted: 50,
        updated: 10,
        delisted: 2,
        errors: [],
      });

      const result = await handleCronA('equity_master', 'run-123');

      expect(result.inserted).toBe(60); // 50 + 10
    });
  });

  describe('対象日なし', () => {
    it('fetched=0で成功を返す', async () => {
      mockDetermineTargetDates.mockResolvedValue([]);

      const result = await handleCronA('equity_bars', 'run-123');

      expect(result.success).toBe(true);
      expect(result.targetDate).toBeNull();
      expect(result.fetched).toBe(0);
      expect(result.inserted).toBe(0);
    });
  });

  describe('複数日', () => {
    it('1日のみ処理する（先頭の日付）', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-10', '2024-01-11', '2024-01-12']);
      mockSyncEquityBarsDailyForDate.mockResolvedValue({
        fetched: 5000,
        inserted: 5000,
        pageCount: 3,
      });

      const result = await handleCronA('equity_bars', 'run-123');

      expect(result.targetDate).toBe('2024-01-10');
      expect(mockSyncEquityBarsDailyForDate).toHaveBeenCalledTimes(1);
    });
  });

  describe('エラーハンドリング', () => {
    it('success=falseを返す', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncEquityBarsDailyForDate.mockRejectedValue(new Error('API timeout'));

      const result = await handleCronA('equity_bars', 'run-123');

      expect(result.success).toBe(false);
      expect(result.error).toBe('API timeout');
    });

    it('メール送信を行う', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncEquityBarsDailyForDate.mockRejectedValue(new Error('API error'));
      mockSendJobFailureEmail.mockResolvedValue(undefined);

      await handleCronA('equity_bars', 'run-123');

      expect(mockSendJobFailureEmail).toHaveBeenCalledWith(
        expect.objectContaining({
          jobName: 'cron_a',
          error: 'API error',
          runId: 'run-123',
          dataset: 'equity_bars',
        })
      );
    });

    it('メール送信失敗でもクラッシュしない', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncEquityBarsDailyForDate.mockRejectedValue(new Error('Sync error'));
      mockSendJobFailureEmail.mockRejectedValue(new Error('SMTP error'));

      const result = await handleCronA('equity_bars', 'run-123');

      expect(result.success).toBe(false);
    });

    it('非Errorオブジェクトのエラーも処理する', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncEquityBarsDailyForDate.mockRejectedValue('string error');
      mockSendJobFailureEmail.mockResolvedValue(undefined);

      const result = await handleCronA('equity_bars', 'run-123');

      expect(result.error).toBe('string error');
    });
  });
});
