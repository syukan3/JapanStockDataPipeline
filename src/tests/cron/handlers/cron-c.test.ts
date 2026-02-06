/**
 * cron/handlers/cron-c.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const {
  mockSyncInvestorTypesWithWindow,
  mockGetLatestEquityBarDateFromDB,
  mockGetLatestTopixBarDateFromDB,
  mockCheckCalendarCoverage,
  mockCreateAdminClient,
  mockSendJobFailureEmail,
  mockGetJSTDate,
  mockAddDays,
} = vi.hoisted(() => ({
  mockSyncInvestorTypesWithWindow: vi.fn(),
  mockGetLatestEquityBarDateFromDB: vi.fn(),
  mockGetLatestTopixBarDateFromDB: vi.fn(),
  mockCheckCalendarCoverage: vi.fn(),
  mockCreateAdminClient: vi.fn(),
  mockSendJobFailureEmail: vi.fn(),
  mockGetJSTDate: vi.fn(),
  mockAddDays: vi.fn(),
}));

vi.mock('@/lib/jquants/endpoints/investor-types', () => ({
  syncInvestorTypesWithWindow: mockSyncInvestorTypesWithWindow,
}));

vi.mock('@/lib/jquants/endpoints/equity-bars-daily', () => ({
  getLatestEquityBarDateFromDB: mockGetLatestEquityBarDateFromDB,
}));

vi.mock('@/lib/jquants/endpoints/index-topix', () => ({
  getLatestTopixBarDateFromDB: mockGetLatestTopixBarDateFromDB,
}));

vi.mock('@/lib/cron/business-day', () => ({
  checkCalendarCoverage: mockCheckCalendarCoverage,
}));

vi.mock('@/lib/supabase/admin', () => ({
  createAdminClient: mockCreateAdminClient,
}));

vi.mock('@/lib/notification/email', () => ({
  sendJobFailureEmail: mockSendJobFailureEmail,
}));

vi.mock('@/lib/utils/date', () => ({
  getJSTDate: mockGetJSTDate,
  addDays: mockAddDays,
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

import { handleCronC } from '@/lib/cron/handlers/cron-c';

describe('cron/handlers/cron-c.ts', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
    mockSyncInvestorTypesWithWindow.mockReset();
    mockGetLatestEquityBarDateFromDB.mockReset();
    mockGetLatestTopixBarDateFromDB.mockReset();
    mockCheckCalendarCoverage.mockReset();
    mockCreateAdminClient.mockReturnValue({});
    mockSendJobFailureEmail.mockReset();
    mockGetJSTDate.mockReturnValue('2024-01-15');
    mockAddDays.mockReturnValue('2024-01-12');
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('getInvestorTypesWindowDays（間接テスト）', () => {
    it('デフォルトは60日', async () => {
      delete process.env.INVESTOR_TYPES_WINDOW_DAYS;

      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true, minDate: '2020-01-01', maxDate: '2030-12-31' });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      await handleCronC('run-123');

      expect(mockSyncInvestorTypesWithWindow).toHaveBeenCalledWith(60, expect.any(Object));
    });

    it('env変数から読み取る', async () => {
      process.env.INVESTOR_TYPES_WINDOW_DAYS = '90';

      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true, minDate: '2020-01-01', maxDate: '2030-12-31' });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      await handleCronC('run-123');

      expect(mockSyncInvestorTypesWithWindow).toHaveBeenCalledWith(90, expect.any(Object));
    });

    it('上限365でクランプする', async () => {
      process.env.INVESTOR_TYPES_WINDOW_DAYS = '500';

      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true, minDate: '2020-01-01', maxDate: '2030-12-31' });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      await handleCronC('run-123');

      expect(mockSyncInvestorTypesWithWindow).toHaveBeenCalledWith(365, expect.any(Object));
    });

    it('不正値はデフォルト60日に戻す', async () => {
      process.env.INVESTOR_TYPES_WINDOW_DAYS = 'invalid';

      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true, minDate: '2020-01-01', maxDate: '2030-12-31' });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      await handleCronC('run-123');

      expect(mockSyncInvestorTypesWithWindow).toHaveBeenCalledWith(60, expect.any(Object));
    });

    it('0以下はデフォルト値に戻す', async () => {
      process.env.INVESTOR_TYPES_WINDOW_DAYS = '0';

      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true, minDate: '2020-01-01', maxDate: '2030-12-31' });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      await handleCronC('run-123');

      expect(mockSyncInvestorTypesWithWindow).toHaveBeenCalledWith(60, expect.any(Object));
    });

    it('負の値はデフォルト値に戻す', async () => {
      process.env.INVESTOR_TYPES_WINDOW_DAYS = '-10';

      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true, minDate: '2020-01-01', maxDate: '2030-12-31' });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      await handleCronC('run-123');

      expect(mockSyncInvestorTypesWithWindow).toHaveBeenCalledWith(60, expect.any(Object));
    });
  });

  describe('handleCronC', () => {
    it('並列実行して成功を返す', async () => {
      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 100, inserted: 100 });
      mockCheckCalendarCoverage.mockResolvedValue({
        ok: true,
        minDate: '2020-01-01',
        maxDate: '2030-12-31',
      });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      const result = await handleCronC('run-123');

      expect(result.success).toBe(true);
      expect(result.fetched).toBe(100);
      expect(result.inserted).toBe(100);
    });

    it('警告ありでも成功を返す', async () => {
      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 100, inserted: 100 });
      mockCheckCalendarCoverage.mockResolvedValue({
        ok: false, // カレンダー不足 → 警告
        minDate: '2024-06-01',
        maxDate: '2024-12-31',
        requiredMinDate: '2023-01-01',
        requiredMaxDate: '2025-01-01',
      });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      const result = await handleCronC('run-123');

      expect(result.success).toBe(true);
      expect(result.integrityCheck.warnings.length).toBeGreaterThan(0);
    });

    it('成功時に整合性チェック結果を含む', async () => {
      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({
        ok: true,
        minDate: '2020-01-01',
        maxDate: '2030-12-31',
      });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      const result = await handleCronC('run-123');

      expect(result.integrityCheck.calendarOk).toBe(true);
      expect(result.integrityCheck.latestEquityBarDate).toBe('2024-01-14');
      expect(result.integrityCheck.latestTopixDate).toBe('2024-01-14');
    });
  });

  describe('整合性チェック', () => {
    it('カレンダーカバレッジ不足で警告を出す', async () => {
      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({
        ok: false,
        minDate: '2024-06-01',
        maxDate: '2024-12-31',
        requiredMinDate: '2023-01-01',
        requiredMaxDate: '2025-01-01',
      });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');

      const result = await handleCronC('run-123');

      expect(result.integrityCheck.calendarOk).toBe(false);
      expect(result.integrityCheck.warnings).toContainEqual(
        expect.stringContaining('Calendar coverage insufficient')
      );
    });

    it('株価データが古い場合に警告を出す', async () => {
      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true, minDate: '2020-01-01', maxDate: '2030-12-31' });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-01'); // 古い
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');
      // addDaysが3日前の日付を返す（2024-01-12）ため、2024-01-01 < 2024-01-12 で警告
      mockAddDays.mockReturnValue('2024-01-12');

      const result = await handleCronC('run-123');

      expect(result.integrityCheck.warnings).toContainEqual(
        expect.stringContaining('Equity bar data is stale')
      );
    });

    it('TOPIXデータが古い場合に警告を出す', async () => {
      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true, minDate: '2020-01-01', maxDate: '2030-12-31' });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-01'); // 古い
      mockAddDays.mockReturnValue('2024-01-12');

      const result = await handleCronC('run-123');

      expect(result.integrityCheck.warnings).toContainEqual(
        expect.stringContaining('TOPIX data is stale')
      );
    });

    it('全て正常なら警告なし', async () => {
      mockSyncInvestorTypesWithWindow.mockResolvedValue({ fetched: 10, inserted: 10 });
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true, minDate: '2020-01-01', maxDate: '2030-12-31' });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue('2024-01-14');
      mockGetLatestTopixBarDateFromDB.mockResolvedValue('2024-01-14');
      mockAddDays.mockReturnValue('2024-01-12');

      const result = await handleCronC('run-123');

      expect(result.integrityCheck.warnings).toHaveLength(0);
    });
  });

  describe('エラーハンドリング', () => {
    it('同期エラー時にsuccess=falseを返す', async () => {
      mockSyncInvestorTypesWithWindow.mockRejectedValue(new Error('API Error'));
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue(null);
      mockGetLatestTopixBarDateFromDB.mockResolvedValue(null);
      mockSendJobFailureEmail.mockResolvedValue(undefined);

      const result = await handleCronC('run-error');

      expect(result.success).toBe(false);
      expect(result.error).toBe('API Error');
    });

    it('エラー時にメール通知を送信する', async () => {
      mockSyncInvestorTypesWithWindow.mockRejectedValue(new Error('Sync failed'));
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue(null);
      mockGetLatestTopixBarDateFromDB.mockResolvedValue(null);
      mockSendJobFailureEmail.mockResolvedValue(undefined);

      await handleCronC('run-error');

      expect(mockSendJobFailureEmail).toHaveBeenCalledWith(
        expect.objectContaining({
          jobName: 'cron_c',
          error: 'Sync failed',
        })
      );
    });

    it('メール送信失敗でもクラッシュしない', async () => {
      mockSyncInvestorTypesWithWindow.mockRejectedValue(new Error('Sync error'));
      mockCheckCalendarCoverage.mockResolvedValue({ ok: true });
      mockGetLatestEquityBarDateFromDB.mockResolvedValue(null);
      mockGetLatestTopixBarDateFromDB.mockResolvedValue(null);
      mockSendJobFailureEmail.mockRejectedValue(new Error('SMTP error'));

      const result = await handleCronC('run-error');

      expect(result.success).toBe(false);
    });
  });
});
