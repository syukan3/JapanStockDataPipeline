/**
 * jquants/endpoints/equity-bars-daily.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockGetSupabaseAdmin, mockBatchUpsert, mockCreateJQuantsClient } = vi.hoisted(() => ({
  mockGetSupabaseAdmin: vi.fn(),
  mockBatchUpsert: vi.fn(),
  mockCreateJQuantsClient: vi.fn(),
}));

vi.mock('@/lib/supabase/admin', () => ({
  getSupabaseAdmin: mockGetSupabaseAdmin,
}));

vi.mock('@/lib/utils/batch', () => ({
  batchUpsert: mockBatchUpsert,
}));

vi.mock('@/lib/jquants/client', () => ({
  JQuantsClient: vi.fn(),
  createJQuantsClient: mockCreateJQuantsClient,
}));

vi.mock('@/lib/supabase/errors', () => ({
  POSTGREST_ERROR_CODES: { NO_ROWS_RETURNED: 'PGRST116' },
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
  toEquityBarDailyRecord,
  toEquityBarDailyRecords,
  syncEquityBarsDaily,
  syncEquityBarsDailySinglePage,
  getEquityBarFromDB,
  getEquityBarsFromDB,
  getAllEquityBarsByDateFromDB,
  getLatestEquityBarDateFromDB,
} from '@/lib/jquants/endpoints/equity-bars-daily';
import type { EquityBarDailyItem } from '@/lib/jquants/types';

/** テスト用APIレスポンスアイテム */
function createMockBarItem(overrides: Partial<EquityBarDailyItem> = {}): EquityBarDailyItem {
  return {
    Date: '2024-01-15',
    Code: '72030',
    // DAY
    O: 1000, H: 1050, L: 990, C: 1020,
    Vo: 500000, Va: 510000000,
    AdjFactor: 1.0,
    AdjO: 1000, AdjH: 1050, AdjL: 990, AdjC: 1020, AdjVo: 500000,
    UL: 1300, LL: 700,
    // AM
    MO: 1000, MH: 1030, ML: 995, MC: 1010,
    MVo: 200000, MVa: 202000000,
    MAdjO: 1000, MAdjH: 1030, MAdjL: 995, MAdjC: 1010, MAdjVo: 200000,
    MUL: 1300, MLL: 700,
    // PM
    AO: 1010, AH: 1050, AL: 990, AC: 1020,
    AVo: 300000, AVa: 306000000,
    AAdjO: 1010, AAdjH: 1050, AAdjL: 990, AAdjC: 1020, AAdjVo: 300000,
    AUL: 1300, ALL: 700,
    ...overrides,
  } as EquityBarDailyItem;
}

describe('jquants/endpoints/equity-bars-daily.ts', () => {
  beforeEach(() => {
    mockGetSupabaseAdmin.mockReset();
    mockBatchUpsert.mockReset();
    mockCreateJQuantsClient.mockReset();
  });

  describe('toEquityBarDailyRecord', () => {
    it('DAYセッションのフィールドをマッピングする', () => {
      const item = createMockBarItem();
      const record = toEquityBarDailyRecord(item, 'DAY');

      expect(record.session).toBe('DAY');
      expect(record.trade_date).toBe('2024-01-15');
      expect(record.local_code).toBe('72030');
      expect(record.open).toBe(1000);
      expect(record.high).toBe(1050);
      expect(record.low).toBe(990);
      expect(record.close).toBe(1020);
      expect(record.volume).toBe(500000);
    });

    it('AMセッションのフィールドをマッピングする', () => {
      const item = createMockBarItem();
      const record = toEquityBarDailyRecord(item, 'AM');

      expect(record.session).toBe('AM');
      expect(record.open).toBe(1000);
      expect(record.high).toBe(1030);
      expect(record.close).toBe(1010);
    });

    it('PMセッションのフィールドをマッピングする', () => {
      const item = createMockBarItem();
      const record = toEquityBarDailyRecord(item, 'PM');

      expect(record.session).toBe('PM');
      expect(record.open).toBe(1010);
      expect(record.high).toBe(1050);
      expect(record.close).toBe(1020);
    });

    it('デフォルトはDAYセッション', () => {
      const item = createMockBarItem();
      const record = toEquityBarDailyRecord(item);

      expect(record.session).toBe('DAY');
    });

    it('adjustment_factorをマッピングする', () => {
      const item = createMockBarItem({ AdjFactor: 2.0 });
      const record = toEquityBarDailyRecord(item, 'DAY');

      expect(record.adjustment_factor).toBe(2.0);
    });
  });

  describe('toEquityBarDailyRecords', () => {
    it('全セッション（DAY/AM/PM）を展開する', () => {
      const item = createMockBarItem();
      const records = toEquityBarDailyRecords(item);

      expect(records).toHaveLength(3);
      expect(records.map((r) => r.session)).toEqual(['DAY', 'AM', 'PM']);
    });

    it('前場データがない場合はAMを除外する', () => {
      const item = createMockBarItem({ MO: null as any, MC: null as any });
      const records = toEquityBarDailyRecords(item);

      const sessions = records.map((r) => r.session);
      expect(sessions).not.toContain('AM');
    });

    it('後場データがない場合はPMを除外する', () => {
      const item = createMockBarItem({ AO: null as any, AC: null as any });
      const records = toEquityBarDailyRecords(item);

      const sessions = records.map((r) => r.session);
      expect(sessions).not.toContain('PM');
    });

    it('部分データ（DAYのみ）を正しく処理する', () => {
      const item = createMockBarItem({
        MO: null as any, MC: null as any,
        AO: null as any, AC: null as any,
      });
      const records = toEquityBarDailyRecords(item);

      expect(records).toHaveLength(1);
      expect(records[0].session).toBe('DAY');
    });

    it('全てnullの場合は空配列を返す', () => {
      const item = createMockBarItem({
        O: null as any, C: null as any,
        MO: null as any, MC: null as any,
        AO: null as any, AC: null as any,
      });
      const records = toEquityBarDailyRecords(item);

      expect(records).toHaveLength(0);
    });

    it('OpenがnullでもCloseがあれば含める', () => {
      const item = createMockBarItem({ O: null as any });
      const records = toEquityBarDailyRecords(item);

      const dayRecord = records.find((r) => r.session === 'DAY');
      expect(dayRecord).toBeDefined();
    });
  });

  describe('syncEquityBarsDaily', () => {
    it('codeもdateもない場合はエラーを投げる', async () => {
      await expect(
        syncEquityBarsDaily({})
      ).rejects.toThrow('syncEquityBarsDaily requires either code or date parameter');
    });

    it('ページネーションで順次処理する', async () => {
      const mockClient = {
        getEquityBarsDailyPaginated: vi.fn().mockImplementation(async function* () {
          yield [createMockBarItem()];
          yield [createMockBarItem({ Code: '86970' })];
        }),
      };
      mockCreateJQuantsClient.mockReturnValue(mockClient);
      mockGetSupabaseAdmin.mockReturnValue({});
      mockBatchUpsert.mockResolvedValue({ inserted: 1, errors: [], batchCount: 1 });

      const result = await syncEquityBarsDaily(
        { date: '2024-01-15' },
        { client: mockClient as any }
      );

      expect(result.pageCount).toBe(2);
      expect(result.fetched).toBe(2);
      expect(mockBatchUpsert).toHaveBeenCalledTimes(2);
    });

    it('code指定で同期する', async () => {
      const mockClient = {
        getEquityBarsDailyPaginated: vi.fn().mockImplementation(async function* () {
          yield [createMockBarItem()];
        }),
      };
      mockGetSupabaseAdmin.mockReturnValue({});
      mockBatchUpsert.mockResolvedValue({ inserted: 1, errors: [], batchCount: 1 });

      const result = await syncEquityBarsDaily(
        { code: '72030' },
        { client: mockClient as any }
      );

      expect(result.fetched).toBe(1);
    });

    it('expandSessions=trueで全セッションを展開する', async () => {
      const mockClient = {
        getEquityBarsDailyPaginated: vi.fn().mockImplementation(async function* () {
          yield [createMockBarItem()];
        }),
      };
      mockGetSupabaseAdmin.mockReturnValue({});
      mockBatchUpsert.mockResolvedValue({ inserted: 3, errors: [], batchCount: 1 });

      await syncEquityBarsDaily(
        { date: '2024-01-15' },
        { client: mockClient as any, expandSessions: true }
      );

      // batchUpsertに3レコード（DAY/AM/PM）が渡されることを確認
      const records = mockBatchUpsert.mock.calls[0][2];
      expect(records).toHaveLength(3);
    });
  });

  describe('syncEquityBarsDailySinglePage', () => {
    it('codeもdateもない場合はエラーを投げる', async () => {
      await expect(
        syncEquityBarsDailySinglePage({})
      ).rejects.toThrow('syncEquityBarsDailySinglePage requires either code or date parameter');
    });

    it('1ページ分を処理してpagination_keyを返す', async () => {
      const mockClient = {
        getEquityBarsDailySinglePage: vi.fn().mockResolvedValue({
          data: [createMockBarItem()],
          paginationKey: 'next-page-key',
        }),
      };
      mockGetSupabaseAdmin.mockReturnValue({});
      mockBatchUpsert.mockResolvedValue({ inserted: 1, errors: [], batchCount: 1 });

      const result = await syncEquityBarsDailySinglePage(
        { date: '2024-01-15' },
        { client: mockClient as any }
      );

      expect(result.fetched).toBe(1);
      expect(result.paginationKey).toBe('next-page-key');
    });

    it('最終ページではpaginationKeyがundefined', async () => {
      const mockClient = {
        getEquityBarsDailySinglePage: vi.fn().mockResolvedValue({
          data: [createMockBarItem()],
          paginationKey: undefined,
        }),
      };
      mockGetSupabaseAdmin.mockReturnValue({});
      mockBatchUpsert.mockResolvedValue({ inserted: 1, errors: [], batchCount: 1 });

      const result = await syncEquityBarsDailySinglePage(
        { date: '2024-01-15' },
        { client: mockClient as any }
      );

      expect(result.paginationKey).toBeUndefined();
    });

    it('batchUpsertエラーで例外を投げる', async () => {
      const mockClient = {
        getEquityBarsDailySinglePage: vi.fn().mockResolvedValue({
          data: [createMockBarItem()],
          paginationKey: undefined,
        }),
      };
      mockGetSupabaseAdmin.mockReturnValue({});
      mockBatchUpsert.mockResolvedValue({
        inserted: 0,
        errors: [new Error('DB Error')],
        batchCount: 1,
      });

      await expect(
        syncEquityBarsDailySinglePage(
          { date: '2024-01-15' },
          { client: mockClient as any }
        )
      ).rejects.toThrow('batchUpsert failed');
    });
  });

  describe('DB取得関数', () => {
    it('getEquityBarFromDB: 単一レコードを取得する', async () => {
      const mockData = { trade_date: '2024-01-15', local_code: '72030', session: 'DAY' };
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({ data: mockData, error: null }),
        })),
      });

      const result = await getEquityBarFromDB('72030', '2024-01-15');

      expect(result).toEqual(mockData);
    });

    it('getEquityBarFromDB: PGRST116でnullを返す', async () => {
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' },
          }),
        })),
      });

      const result = await getEquityBarFromDB('72030', '2024-01-15');

      expect(result).toBeNull();
    });

    it('getEquityBarsFromDB: 期間指定でリストを取得する', async () => {
      const mockData = [
        { trade_date: '2024-01-15' },
        { trade_date: '2024-01-16' },
      ];
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          gte: vi.fn().mockReturnThis(),
          lte: vi.fn().mockReturnThis(),
          order: vi.fn().mockResolvedValue({ data: mockData, error: null }),
        })),
      });

      const result = await getEquityBarsFromDB('72030', '2024-01-15', '2024-01-16');

      expect(result).toHaveLength(2);
    });

    it('getAllEquityBarsByDateFromDB: 指定日の全銘柄を取得する', async () => {
      const mockData = [{ local_code: '72030' }, { local_code: '86970' }];
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockResolvedValue({ data: mockData, error: null }),
        })),
      });

      const result = await getAllEquityBarsByDateFromDB('2024-01-15');

      expect(result).toHaveLength(2);
    });

    it('getLatestEquityBarDateFromDB: 最新日付を取得する', async () => {
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: { trade_date: '2024-01-15' },
            error: null,
          }),
        })),
      });

      const result = await getLatestEquityBarDateFromDB();

      expect(result).toBe('2024-01-15');
    });

    it('getLatestEquityBarDateFromDB: データなしでnullを返す', async () => {
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' },
          }),
        })),
      });

      const result = await getLatestEquityBarDateFromDB();

      expect(result).toBeNull();
    });
  });
});
