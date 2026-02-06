/**
 * cron/catch-up.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.mock をモジュールレベルで宣言
vi.mock('@/lib/utils/date', () => ({
  getJSTDate: vi.fn(),
  addDays: vi.fn(),
}));

vi.mock('@/lib/cron/business-day', () => ({
  getBusinessDays: vi.fn(),
  getPreviousBusinessDay: vi.fn(),
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

import {
  getCatchUpConfig,
  findMissingBusinessDays,
  findMissingDatesInTable,
  needsCatchUp,
  getLastSuccessfulDate,
  determineTargetDates,
} from '@/lib/cron/catch-up';
import { getJSTDate, addDays } from '@/lib/utils/date';
import { getBusinessDays, getPreviousBusinessDay } from '@/lib/cron/business-day';

const mockGetJSTDate = vi.mocked(getJSTDate);
const mockAddDays = vi.mocked(addDays);
const mockGetBusinessDays = vi.mocked(getBusinessDays);
const mockGetPreviousBusinessDay = vi.mocked(getPreviousBusinessDay);

describe('cron/catch-up.ts', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
    mockGetJSTDate.mockReturnValue('2024-01-15');
    mockAddDays.mockImplementation((_date: string, days: number) => {
      // シンプルな日付加算
      const d = new Date('2024-01-15');
      d.setDate(d.getDate() + days);
      return d.toISOString().slice(0, 10);
    });
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('getCatchUpConfig', () => {
    it('デフォルト値を返す（env変数未設定）', () => {
      delete process.env.SYNC_MAX_CATCHUP_DAYS;
      delete process.env.SYNC_LOOKBACK_DAYS;

      const config = getCatchUpConfig();

      expect(config.maxDays).toBe(5);
      expect(config.lookbackDays).toBe(30);
    });

    it('env変数から値を読み取る', () => {
      process.env.SYNC_MAX_CATCHUP_DAYS = '10';
      process.env.SYNC_LOOKBACK_DAYS = '60';

      const config = getCatchUpConfig();

      expect(config.maxDays).toBe(10);
      expect(config.lookbackDays).toBe(60);
    });

    it('SYNC_MAX_CATCHUP_DAYSのみ設定時、lookbackDaysはデフォルト', () => {
      process.env.SYNC_MAX_CATCHUP_DAYS = '3';
      delete process.env.SYNC_LOOKBACK_DAYS;

      const config = getCatchUpConfig();

      expect(config.maxDays).toBe(3);
      expect(config.lookbackDays).toBe(30);
    });

    it('不正値はNaN（parseIntの挙動）', () => {
      process.env.SYNC_MAX_CATCHUP_DAYS = 'invalid';

      const config = getCatchUpConfig();

      expect(config.maxDays).toBeNaN();
    });
  });

  describe('findMissingBusinessDays', () => {
    it('未処理の営業日を検出する', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue([
        '2024-01-10',
        '2024-01-11',
        '2024-01-12',
      ]);

      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({
            data: [{ target_date: '2024-01-10' }],
            error: null,
          }),
        })),
      };

      const result = await findMissingBusinessDays(
        mockIngest as any,
        {} as any,
        'cron_a',
        { maxDays: 5, lookbackDays: 30 }
      );

      expect(result).toEqual(['2024-01-11', '2024-01-12']);
    });

    it('前営業日がnullの場合は空配列を返す', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue(null);

      const result = await findMissingBusinessDays(
        {} as any,
        {} as any,
        'cron_a',
        { maxDays: 5, lookbackDays: 30 }
      );

      expect(result).toEqual([]);
    });

    it('営業日がゼロの場合は空配列を返す', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue([]);

      const result = await findMissingBusinessDays(
        {} as any,
        {} as any,
        'cron_a',
        { maxDays: 5, lookbackDays: 30 }
      );

      expect(result).toEqual([]);
    });

    it('DBエラーの場合は空配列を返す', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue(['2024-01-10', '2024-01-11']);

      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({
            data: null,
            error: { message: 'DB Error' },
          }),
        })),
      };

      const result = await findMissingBusinessDays(
        mockIngest as any,
        {} as any,
        'cron_a',
        { maxDays: 5, lookbackDays: 30 }
      );

      expect(result).toEqual([]);
    });

    it('maxDays制限を適用する', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue([
        '2024-01-08',
        '2024-01-09',
        '2024-01-10',
        '2024-01-11',
        '2024-01-12',
      ]);

      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({
            data: [],
            error: null,
          }),
        })),
      };

      const result = await findMissingBusinessDays(
        mockIngest as any,
        {} as any,
        'cron_a',
        { maxDays: 2, lookbackDays: 30 }
      );

      expect(result).toHaveLength(2);
      expect(result).toEqual(['2024-01-08', '2024-01-09']);
    });

    it('全て処理済みの場合は空配列を返す', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue(['2024-01-10', '2024-01-11', '2024-01-12']);

      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({
            data: [
              { target_date: '2024-01-10' },
              { target_date: '2024-01-11' },
              { target_date: '2024-01-12' },
            ],
            error: null,
          }),
        })),
      };

      const result = await findMissingBusinessDays(
        mockIngest as any,
        {} as any,
        'cron_a',
        { maxDays: 5, lookbackDays: 30 }
      );

      expect(result).toEqual([]);
    });

    it('config省略時はデフォルト設定を使用する', async () => {
      delete process.env.SYNC_MAX_CATCHUP_DAYS;
      delete process.env.SYNC_LOOKBACK_DAYS;
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue([]);

      const result = await findMissingBusinessDays(
        {} as any,
        {} as any,
        'cron_a'
      );

      expect(result).toEqual([]);
      // addDaysが -30（デフォルトlookbackDays）で呼ばれることを確認
      expect(mockAddDays).toHaveBeenCalledWith('2024-01-15', -30);
    });
  });

  describe('findMissingDatesInTable', () => {
    it('RPCで欠損日付を検出する', async () => {
      mockGetBusinessDays.mockResolvedValue(['2024-01-10', '2024-01-11', '2024-01-12']);

      const mockCore = {
        rpc: vi.fn().mockResolvedValue({
          data: [{ date_value: '2024-01-10' }, { date_value: '2024-01-12' }],
          error: null,
        }),
      };

      const result = await findMissingDatesInTable(
        mockCore as any,
        'equity_bar_daily',
        'trade_date',
        '2024-01-10',
        '2024-01-12'
      );

      expect(result).toEqual(['2024-01-11']);
    });

    it('RPCエラーの場合は空配列を返す', async () => {
      mockGetBusinessDays.mockResolvedValue(['2024-01-10']);

      const mockCore = {
        rpc: vi.fn().mockResolvedValue({
          data: null,
          error: { message: 'RPC Error' },
        }),
      };

      const result = await findMissingDatesInTable(
        mockCore as any,
        'equity_bar_daily',
        'trade_date',
        '2024-01-10',
        '2024-01-10'
      );

      expect(result).toEqual([]);
    });

    it('営業日がゼロの場合は空配列を返す', async () => {
      mockGetBusinessDays.mockResolvedValue([]);

      const result = await findMissingDatesInTable(
        {} as any,
        'equity_bar_daily',
        'trade_date',
        '2024-01-10',
        '2024-01-12'
      );

      expect(result).toEqual([]);
    });
  });

  describe('needsCatchUp', () => {
    it('未処理があればtrueを返す', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue(['2024-01-12']);

      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({
            data: [],
            error: null,
          }),
        })),
      };

      const result = await needsCatchUp(mockIngest as any, {} as any, 'cron_a');

      expect(result).toBe(true);
    });

    it('全て処理済みならfalseを返す', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue(['2024-01-12']);

      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({
            data: [{ target_date: '2024-01-12' }],
            error: null,
          }),
        })),
      };

      const result = await needsCatchUp(mockIngest as any, {} as any, 'cron_a');

      expect(result).toBe(false);
    });

    it('軽量設定（maxDays=1, lookbackDays=7）で検索する', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue([]);

      await needsCatchUp({} as any, {} as any, 'cron_a');

      // lookbackDays=7が使われていることを確認
      expect(mockAddDays).toHaveBeenCalledWith('2024-01-15', -7);
    });
  });

  describe('getLastSuccessfulDate', () => {
    it('成功した最新のtarget_dateを返す', async () => {
      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          not: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: { target_date: '2024-01-12' },
            error: null,
          }),
        })),
      };

      const result = await getLastSuccessfulDate(mockIngest as any, 'cron_a');

      expect(result).toBe('2024-01-12');
    });

    it('PGRST116エラー（行なし）の場合はnullを返す', async () => {
      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          not: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116', message: 'No rows found' },
          }),
        })),
      };

      const result = await getLastSuccessfulDate(mockIngest as any, 'cron_a');

      expect(result).toBeNull();
    });

    it('その他のDBエラーの場合もnullを返す', async () => {
      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          not: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'P0001', message: 'DB Error' },
          }),
        })),
      };

      const result = await getLastSuccessfulDate(mockIngest as any, 'cron_a');

      expect(result).toBeNull();
    });

    it('dataが存在するがtarget_dateがnullの場合はnullを返す', async () => {
      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          not: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: { target_date: null },
            error: null,
          }),
        })),
      };

      const result = await getLastSuccessfulDate(mockIngest as any, 'cron_a');

      expect(result).toBeNull();
    });
  });

  describe('determineTargetDates', () => {
    it('キャッチアップ対象がある場合はそれを返す', async () => {
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue(['2024-01-10', '2024-01-11']);

      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({
            data: [],
            error: null,
          }),
        })),
      };

      const result = await determineTargetDates(
        mockIngest as any,
        {} as any,
        'cron_a'
      );

      expect(result).toEqual(['2024-01-10', '2024-01-11']);
    });

    it('キャッチアップ不要なら前営業日を返す', async () => {
      // findMissingBusinessDaysが空を返すようにモック
      mockGetPreviousBusinessDay.mockResolvedValue('2024-01-12');
      mockGetBusinessDays.mockResolvedValue(['2024-01-10', '2024-01-11', '2024-01-12']);

      const mockIngest = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({
            data: [
              { target_date: '2024-01-10' },
              { target_date: '2024-01-11' },
              { target_date: '2024-01-12' },
            ],
            error: null,
          }),
        })),
      };

      const result = await determineTargetDates(
        mockIngest as any,
        {} as any,
        'cron_a'
      );

      // 全て処理済み→キャッチアップなし→前営業日を返す
      expect(result).toEqual(['2024-01-12']);
    });

    it('前営業日もない場合は空配列を返す', async () => {
      // 最初のfindMissingBusinessDays呼び出しでpreviousBusinessDayがnull
      mockGetPreviousBusinessDay.mockResolvedValue(null);

      const result = await determineTargetDates(
        {} as any,
        {} as any,
        'cron_a'
      );

      expect(result).toEqual([]);
    });
  });
});
