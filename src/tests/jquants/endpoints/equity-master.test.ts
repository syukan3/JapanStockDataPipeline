/**
 * jquants/endpoints/equity-master.ts のユニットテスト
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
  toEquityMasterRecord,
  toEquityMasterSCDRecord,
  isSameEquityMaster,
  syncEquityMasterSCD,
  getEquityMasterFromDB,
  getEquityMasterAsOfDate,
  getAllCurrentEquityMaster,
  getEquityMasterHistory,
} from '@/lib/jquants/endpoints/equity-master';
import type { EquityMasterItem, EquityMasterRecord } from '@/lib/jquants/types';

/** テスト用のAPIレスポンスアイテム */
function createMockMasterItem(overrides: Partial<EquityMasterItem> = {}): EquityMasterItem {
  return {
    Date: '2024-01-15',
    Code: '72030',
    CoName: 'トヨタ自動車',
    CoNameEn: 'TOYOTA MOTOR CORP',
    S17: '02',
    S17Nm: '自動車・輸送機',
    S33: '3050',
    S33Nm: '輸送用機器',
    ScaleCat: 'TOPIX Large70',
    Mkt: '0111',
    MktNm: 'プライム',
    MarginCode: '1',
    MarginCodeNm: '貸借',
    ...overrides,
  } as EquityMasterItem;
}

describe('jquants/endpoints/equity-master.ts', () => {
  beforeEach(() => {
    mockGetSupabaseAdmin.mockReset();
    mockBatchUpsert.mockReset();
    mockCreateJQuantsClient.mockReset();
  });

  describe('toEquityMasterRecord', () => {
    it('APIレスポンスをDBレコード形式に変換する', () => {
      const item = createMockMasterItem();
      const record = toEquityMasterRecord(item);

      expect(record.as_of_date).toBe('2024-01-15');
      expect(record.local_code).toBe('72030');
      expect(record.company_name).toBe('トヨタ自動車');
      expect(record.company_name_en).toBe('TOYOTA MOTOR CORP');
      expect(record.sector17_code).toBe('02');
      expect(record.market_code).toBe('0111');
    });

    it('全フィールドをマッピングする', () => {
      const item = createMockMasterItem();
      const record = toEquityMasterRecord(item);

      expect(record.sector17_name).toBe('自動車・輸送機');
      expect(record.sector33_code).toBe('3050');
      expect(record.sector33_name).toBe('輸送用機器');
      expect(record.scale_category).toBe('TOPIX Large70');
      expect(record.market_name).toBe('プライム');
      expect(record.margin_code).toBe('1');
      expect(record.margin_code_name).toBe('貸借');
    });
  });

  describe('toEquityMasterSCDRecord', () => {
    it('SCD Type 2レコード形式に変換する', () => {
      const item = createMockMasterItem();
      const record = toEquityMasterSCDRecord(item);

      expect(record.local_code).toBe('72030');
      expect(record.valid_from).toBe('2024-01-15');
      expect(record.valid_to).toBeNull();
      expect(record.is_current).toBe(true);
    });

    it('MarginCodeがnull/undefinedの場合もnullに変換する', () => {
      const item = createMockMasterItem({ MarginCode: undefined as any, MarginCodeNm: undefined as any });
      const record = toEquityMasterSCDRecord(item);

      expect(record.margin_code).toBeNull();
      expect(record.margin_code_name).toBeNull();
    });

    it('MarginCodeが存在する場合はそのまま保持する', () => {
      const item = createMockMasterItem({ MarginCode: '2', MarginCodeNm: '信用' });
      const record = toEquityMasterSCDRecord(item);

      expect(record.margin_code).toBe('2');
      expect(record.margin_code_name).toBe('信用');
    });
  });

  describe('isSameEquityMaster', () => {
    it('同一レコードの場合trueを返す', () => {
      const a = toEquityMasterSCDRecord(createMockMasterItem());
      const b = toEquityMasterSCDRecord(createMockMasterItem());

      expect(isSameEquityMaster(a, b)).toBe(true);
    });

    it('差分がある場合falseを返す（company_name変更）', () => {
      const a = toEquityMasterSCDRecord(createMockMasterItem());
      const b = toEquityMasterSCDRecord(createMockMasterItem({ CoName: '新トヨタ' }));

      expect(isSameEquityMaster(a, b)).toBe(false);
    });

    it('差分がある場合falseを返す（market_code変更）', () => {
      const a = toEquityMasterSCDRecord(createMockMasterItem());
      const b = toEquityMasterSCDRecord(createMockMasterItem({ Mkt: '0112' }));

      expect(isSameEquityMaster(a, b)).toBe(false);
    });

    it('null/undefined正規化で同等として扱う', () => {
      const a = toEquityMasterSCDRecord(createMockMasterItem({ MarginCode: null as any }));
      const b = toEquityMasterSCDRecord(createMockMasterItem({ MarginCode: undefined as any }));

      expect(isSameEquityMaster(a, b)).toBe(true);
    });

    it('全11フィールドを比較する', () => {
      const base = createMockMasterItem();
      const baseRecord = toEquityMasterSCDRecord(base);

      // 各比較フィールドの変更で差分を検出
      const fields = [
        { CoName: 'X' },
        { CoNameEn: 'X' },
        { S17: 'X' },
        { S17Nm: 'X' },
        { S33: 'X' },
        { S33Nm: 'X' },
        { ScaleCat: 'X' },
        { Mkt: 'X' },
        { MktNm: 'X' },
        { MarginCode: 'X' },
        { MarginCodeNm: 'X' },
      ];

      for (const override of fields) {
        const modified = toEquityMasterSCDRecord(createMockMasterItem(override));
        expect(isSameEquityMaster(baseRecord, modified)).toBe(false);
      }
    });

    it('snapshot形式とSCD形式の比較もできる', () => {
      const snapshot = toEquityMasterRecord(createMockMasterItem());
      const scd = toEquityMasterSCDRecord(createMockMasterItem());

      expect(isSameEquityMaster(snapshot, scd)).toBe(true);
    });
  });

  describe('syncEquityMasterSCD', () => {
    it('空データの場合は即座に返す', async () => {
      const mockClient = {
        getEquityMaster: vi.fn().mockResolvedValue({ data: [] }),
      };
      mockCreateJQuantsClient.mockReturnValue(mockClient);
      mockGetSupabaseAdmin.mockReturnValue({});

      const result = await syncEquityMasterSCD('2024-01-15', { client: mockClient as any });

      expect(result.fetched).toBe(0);
      expect(result.inserted).toBe(0);
      expect(result.updated).toBe(0);
      expect(result.delisted).toBe(0);
    });

    it('新規銘柄を追加する', async () => {
      const mockClient = {
        getEquityMaster: vi.fn().mockResolvedValue({
          data: [createMockMasterItem()],
        }),
      };

      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          range: vi.fn().mockResolvedValue({ data: [], error: null }),
        })),
      };

      mockCreateJQuantsClient.mockReturnValue(mockClient);
      mockGetSupabaseAdmin.mockReturnValue(mockSupabase);
      mockBatchUpsert.mockResolvedValue({ inserted: 1, errors: [], batchCount: 1 });

      const result = await syncEquityMasterSCD('2024-01-15', { client: mockClient as any });

      expect(result.fetched).toBe(1);
      expect(result.inserted).toBe(1);
    });

    it('変更検出で旧レコードクローズ+新レコード追加する', async () => {
      const mockClient = {
        getEquityMaster: vi.fn().mockResolvedValue({
          data: [createMockMasterItem({ MktNm: 'スタンダード' })],
        }),
      };

      const existingRecord = {
        id: 1,
        local_code: '72030',
        company_name: 'トヨタ自動車',
        company_name_en: 'TOYOTA MOTOR CORP',
        sector17_code: '02',
        sector17_name: '自動車・輸送機',
        sector33_code: '3050',
        sector33_name: '輸送用機器',
        scale_category: 'TOPIX Large70',
        market_code: '0111',
        market_name: 'プライム', // ← 変更前
        margin_code: '1',
        margin_code_name: '貸借',
        valid_from: '2023-01-01',
        valid_to: null,
        is_current: true,
      };

      const mockSupabase = {
        from: vi.fn((table: string) => {
          if (table === 'equity_master') {
            return {
              select: vi.fn().mockReturnThis(),
              eq: vi.fn().mockReturnThis(),
              order: vi.fn().mockReturnThis(),
              range: vi.fn().mockResolvedValueOnce({ data: [existingRecord], error: null })
                .mockResolvedValue({ data: [], error: null }),
              update: vi.fn().mockReturnThis(),
              in: vi.fn().mockResolvedValue({ error: null }),
            };
          }
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            order: vi.fn().mockReturnThis(),
            range: vi.fn().mockResolvedValue({ data: [], error: null }),
          };
        }),
      };

      mockCreateJQuantsClient.mockReturnValue(mockClient);
      mockGetSupabaseAdmin.mockReturnValue(mockSupabase);
      mockBatchUpsert.mockResolvedValue({ inserted: 1, errors: [], batchCount: 1 });

      const result = await syncEquityMasterSCD('2024-01-15', { client: mockClient as any });

      expect(result.updated).toBe(1);
    });

    it('上場廃止を処理する（APIに存在しない銘柄）', async () => {
      const mockClient = {
        getEquityMaster: vi.fn().mockResolvedValue({
          data: [createMockMasterItem({ Code: '72030' })],
        }),
      };

      const existingRecords = [
        {
          id: 1,
          local_code: '72030',
          ...toEquityMasterSCDRecord(createMockMasterItem()),
        },
        {
          id: 2,
          local_code: '99990', // APIに存在しない → 上場廃止
          ...toEquityMasterSCDRecord(createMockMasterItem({ Code: '99990' })),
        },
      ];

      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          range: vi.fn().mockResolvedValueOnce({ data: existingRecords, error: null })
            .mockResolvedValue({ data: [], error: null }),
          update: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({ error: null }),
        })),
      };

      mockCreateJQuantsClient.mockReturnValue(mockClient);
      mockGetSupabaseAdmin.mockReturnValue(mockSupabase);
      mockBatchUpsert.mockResolvedValue({ inserted: 0, errors: [], batchCount: 0 });

      const result = await syncEquityMasterSCD('2024-01-15', { client: mockClient as any });

      expect(result.delisted).toBe(1);
    });

    it('ページネーションで1000件超のレコードを取得する', async () => {
      // APIも同じ1000件を返すことで上場廃止処理を回避
      const codes = Array.from({ length: 1000 }, (_, i) => String(10000 + i).padStart(5, '0'));
      const apiItems = codes.map((code) => createMockMasterItem({ Code: code }));

      const mockClient = {
        getEquityMaster: vi.fn().mockResolvedValue({ data: apiItems }),
      };

      const largeData = codes.map((code, i) => ({
        id: i + 1,
        local_code: code,
        ...toEquityMasterSCDRecord(createMockMasterItem({ Code: code })),
      }));

      let rangeCallCount = 0;
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          range: vi.fn().mockImplementation(() => {
            rangeCallCount++;
            if (rangeCallCount === 1) {
              return Promise.resolve({ data: largeData, error: null });
            }
            return Promise.resolve({ data: [], error: null });
          }),
          update: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({ error: null }),
        })),
      };

      mockCreateJQuantsClient.mockReturnValue(mockClient);
      mockGetSupabaseAdmin.mockReturnValue(mockSupabase);
      mockBatchUpsert.mockResolvedValue({ inserted: 0, errors: [], batchCount: 0 });

      await syncEquityMasterSCD('2024-01-15', { client: mockClient as any });

      // ページネーションで少なくとも2回呼ばれる（1000件 + 空ページ）
      expect(rangeCallCount).toBeGreaterThanOrEqual(2);
    });

    it('クローズエラー時はinsertを中止してエラーを投げる', async () => {
      const mockClient = {
        getEquityMaster: vi.fn().mockResolvedValue({
          data: [createMockMasterItem({ MktNm: '変更後' })],
        }),
      };

      const existingRecord = {
        id: 1,
        local_code: '72030',
        ...toEquityMasterSCDRecord(createMockMasterItem()),
      };

      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          range: vi.fn().mockResolvedValueOnce({ data: [existingRecord], error: null })
            .mockResolvedValue({ data: [], error: null }),
          update: vi.fn().mockReturnThis(),
          in: vi.fn().mockResolvedValue({ error: { message: 'Update failed' } }),
        })),
      };

      mockCreateJQuantsClient.mockReturnValue(mockClient);
      mockGetSupabaseAdmin.mockReturnValue(mockSupabase);

      await expect(
        syncEquityMasterSCD('2024-01-15', { client: mockClient as any })
      ).rejects.toThrow('Failed to close');

      // batchUpsertが呼ばれないことを確認
      expect(mockBatchUpsert).not.toHaveBeenCalled();
    });

    it('effectiveDateはAPIレスポンスの日付を使用する', async () => {
      // 非営業日2024-01-14を指定 → APIは2024-01-15を返す
      const mockClient = {
        getEquityMaster: vi.fn().mockResolvedValue({
          data: [createMockMasterItem({ Date: '2024-01-15' })],
        }),
      };

      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          range: vi.fn().mockResolvedValue({ data: [], error: null }),
        })),
      };

      mockCreateJQuantsClient.mockReturnValue(mockClient);
      mockGetSupabaseAdmin.mockReturnValue(mockSupabase);
      mockBatchUpsert.mockResolvedValue({ inserted: 1, errors: [], batchCount: 1 });

      const result = await syncEquityMasterSCD('2024-01-14', { client: mockClient as any });

      // batchUpsertに渡されたレコードのvalid_fromが2024-01-15であることを確認
      const insertedRecords = mockBatchUpsert.mock.calls[0][2];
      expect(insertedRecords[0].valid_from).toBe('2024-01-15');
    });
  });

  describe('DB取得関数', () => {
    it('getEquityMasterFromDB: 最新日付のレコードを取得する', async () => {
      const mockData = { local_code: '72030', company_name: 'トヨタ自動車' };
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({ data: mockData, error: null }),
        })),
      });

      const result = await getEquityMasterFromDB('72030');

      expect(result).toEqual(mockData);
    });

    it('getEquityMasterFromDB: PGRST116でnullを返す', async () => {
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' },
          }),
        })),
      });

      const result = await getEquityMasterFromDB('99999');

      expect(result).toBeNull();
    });

    it('getEquityMasterFromDB: includeRawJson=trueでselect(*)を使用する', async () => {
      const selectMock = vi.fn().mockReturnThis();
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: selectMock,
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({ data: {}, error: null }),
        })),
      });

      await getEquityMasterFromDB('72030', { includeRawJson: true });

      expect(selectMock).toHaveBeenCalledWith('*');
    });

    it('getEquityMasterAsOfDate: SCD形式で指定日時点のレコードを取得する', async () => {
      const mockData = { local_code: '72030', valid_from: '2024-01-01', is_current: true };
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          lte: vi.fn().mockReturnThis(),
          or: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({ data: mockData, error: null }),
        })),
      });

      const result = await getEquityMasterAsOfDate('72030', '2024-01-15');

      expect(result).toEqual(mockData);
    });

    it('getEquityMasterAsOfDate: PGRST116でnullを返す', async () => {
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          lte: vi.fn().mockReturnThis(),
          or: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' },
          }),
        })),
      });

      const result = await getEquityMasterAsOfDate('72030', '2024-01-15');

      expect(result).toBeNull();
    });

    it('getAllCurrentEquityMaster: 全銘柄を取得する', async () => {
      const mockData = [
        { local_code: '72030', is_current: true },
        { local_code: '86970', is_current: true },
      ];
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockResolvedValue({ data: mockData, error: null }),
        })),
      });

      const result = await getAllCurrentEquityMaster();

      expect(result).toHaveLength(2);
    });

    it('getEquityMasterHistory: 銘柄の履歴を取得する', async () => {
      const mockData = [
        { local_code: '72030', valid_from: '2023-01-01' },
        { local_code: '72030', valid_from: '2024-01-01' },
      ];
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockResolvedValue({ data: mockData, error: null }),
        })),
      });

      const result = await getEquityMasterHistory('72030');

      expect(result).toHaveLength(2);
    });

    it('DB取得でエラー（非PGRST116）は例外を投げる', async () => {
      mockGetSupabaseAdmin.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'P0001', message: 'DB Error' },
          }),
        })),
      });

      await expect(getEquityMasterFromDB('72030')).rejects.toEqual(
        expect.objectContaining({ code: 'P0001' })
      );
    });
  });
});
