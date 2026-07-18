/**
 * batch.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { chunkArray, batchUpsert, batchSelect, batchProcess } from '@/lib/utils/batch';

describe('batch.ts', () => {
  describe('chunkArray', () => {
    it('配列を指定サイズで分割する', () => {
      const array = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
      const chunks = chunkArray(array, 3);

      expect(chunks).toEqual([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]);
    });

    it('配列が均等に分割できる場合', () => {
      const array = [1, 2, 3, 4, 5, 6];
      const chunks = chunkArray(array, 2);

      expect(chunks).toEqual([
        [1, 2],
        [3, 4],
        [5, 6],
      ]);
    });

    it('空配列の場合は空配列を返す', () => {
      const array: number[] = [];
      const chunks = chunkArray(array, 5);

      expect(chunks).toEqual([]);
    });

    it('配列サイズがチャンクサイズより小さい場合', () => {
      const array = [1, 2, 3];
      const chunks = chunkArray(array, 10);

      expect(chunks).toEqual([[1, 2, 3]]);
    });

    it('チャンクサイズが1の場合', () => {
      const array = [1, 2, 3];
      const chunks = chunkArray(array, 1);

      expect(chunks).toEqual([[1], [2], [3]]);
    });

    it('オブジェクト配列でも動作する', () => {
      const array = [{ id: 1 }, { id: 2 }, { id: 3 }];
      const chunks = chunkArray(array, 2);

      expect(chunks).toEqual([[{ id: 1 }, { id: 2 }], [{ id: 3 }]]);
    });
  });

  describe('batchUpsert', () => {
    const createMockSupabase = (mockResponse: {
      error?: { message: string };
      count?: number;
    }) => ({
      from: vi.fn(() => ({
        upsert: vi.fn().mockResolvedValue({
          error: mockResponse.error ?? null,
          count: mockResponse.count ?? null,
        }),
      })),
    });

    it('空配列の場合は即座に返す', async () => {
      const mockSupabase = createMockSupabase({});

      const result = await batchUpsert(
        mockSupabase as any,
        'test_table',
        [],
        'id'
      );

      expect(result).toEqual({ inserted: 0, errors: [], batchCount: 0 });
      expect(mockSupabase.from).not.toHaveBeenCalled();
    });

    it('単一バッチで正常にupsertする', async () => {
      const mockSupabase = createMockSupabase({ count: 3 });
      const data = [{ id: 1 }, { id: 2 }, { id: 3 }];

      const result = await batchUpsert(
        mockSupabase as any,
        'test_table',
        data,
        'id',
        { batchSize: 10 }
      );

      expect(result.inserted).toBe(3);
      expect(result.errors).toHaveLength(0);
      expect(result.batchCount).toBe(1);
    });

    it('複数バッチに分割してupsertする', async () => {
      // バッチサイズ2で5件: [2, 2, 1] の3バッチ
      let callCount = 0;
      const mockSupabase = {
        from: vi.fn(() => ({
          upsert: vi.fn().mockImplementation((chunk) => {
            callCount++;
            // 実際のバッチ長をcountとして返す
            return Promise.resolve({ error: null, count: chunk.length });
          }),
        })),
      };
      const data = [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }];

      const result = await batchUpsert(
        mockSupabase as any,
        'test_table',
        data,
        'id',
        { batchSize: 2 }
      );

      expect(result.inserted).toBe(5); // 2 + 2 + 1 = 実際のデータ数
      expect(result.batchCount).toBe(3);
      expect(callCount).toBe(3);
    });

    it('エラー時にcontinueOnError=falseで中止する', async () => {
      const mockSupabase = createMockSupabase({ error: { message: 'DB Error' } });
      const data = [{ id: 1 }, { id: 2 }];

      await expect(
        batchUpsert(mockSupabase as any, 'test_table', data, 'id', {
          batchSize: 1,
          continueOnError: false,
        })
      ).rejects.toThrow('Batch 1/2 failed: DB Error');
    });

    it('エラー時にcontinueOnError=trueで続行する', async () => {
      let callCount = 0;
      const mockSupabase = {
        from: vi.fn(() => ({
          upsert: vi.fn().mockImplementation(() => {
            callCount++;
            if (callCount === 1) {
              return Promise.resolve({ error: { message: 'DB Error' }, count: null });
            }
            return Promise.resolve({ error: null, count: 1 });
          }),
        })),
      };

      const data = [{ id: 1 }, { id: 2 }];

      const result = await batchUpsert(
        mockSupabase as any,
        'test_table',
        data,
        'id',
        { batchSize: 1, continueOnError: true }
      );

      expect(result.errors).toHaveLength(1);
      expect(result.inserted).toBe(1);
      expect(result.batchCount).toBe(2);
    });

    it('onBatchCompleteコールバックが呼ばれる', async () => {
      const mockSupabase = createMockSupabase({ count: 2 });
      const data = [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }];
      const onBatchComplete = vi.fn();

      await batchUpsert(mockSupabase as any, 'test_table', data, 'id', {
        batchSize: 2,
        onBatchComplete,
      });

      expect(onBatchComplete).toHaveBeenCalledTimes(2);
      expect(onBatchComplete).toHaveBeenNthCalledWith(1, 1, 2, 4);
      expect(onBatchComplete).toHaveBeenNthCalledWith(2, 2, 4, 4);
    });

    it('テーブル別のデフォルトバッチサイズが適用される', async () => {
      const mockSupabase = createMockSupabase({ count: 1000 });
      const data = Array.from({ length: 1500 }, (_, i) => ({ id: i }));

      const result = await batchUpsert(
        mockSupabase as any,
        'equity_bar_daily',
        data,
        'id'
      );

      // equity_bar_daily のバッチサイズは 1000
      expect(result.batchCount).toBe(2);
    });
  });

  describe('batchSelect', () => {
    it('単一ページで全データを取得する', async () => {
      const mockData = [{ id: 1 }, { id: 2 }, { id: 3 }];
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          range: vi.fn().mockResolvedValue({ data: mockData, error: null }),
        })),
      };

      const result = await batchSelect(mockSupabase as any, 'test_table', {
        pageSize: 10,
      });

      expect(result).toEqual(mockData);
    });

    it('複数ページをまたいでデータを取得する', async () => {
      let callCount = 0;
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          range: vi.fn().mockImplementation(() => {
            callCount++;
            if (callCount === 1) {
              return Promise.resolve({ data: [{ id: 1 }, { id: 2 }], error: null });
            }
            if (callCount === 2) {
              return Promise.resolve({ data: [{ id: 3 }], error: null });
            }
            return Promise.resolve({ data: [], error: null });
          }),
        })),
      };

      const result = await batchSelect(mockSupabase as any, 'test_table', {
        pageSize: 2,
      });

      expect(result).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    });

    it('filterオプションが適用される', async () => {
      const eqMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: eqMock,
          range: vi.fn().mockResolvedValue({ data: [], error: null }),
        })),
      };

      await batchSelect(mockSupabase as any, 'test_table', {
        filter: { column: 'status', operator: 'eq', value: 'active' },
      });

      expect(eqMock).toHaveBeenCalledWith('status', 'active');
    });

    it('is演算子のfilterオプションが適用される(valid_to is nullなど)', async () => {
      const isMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          is: isMock,
          range: vi.fn().mockResolvedValue({ data: [], error: null }),
        })),
      };

      await batchSelect(mockSupabase as any, 'test_table', {
        filter: { column: 'valid_to', operator: 'is', value: null },
      });

      expect(isMock).toHaveBeenCalledWith('valid_to', null);
    });

    it('orderByオプションが適用される', async () => {
      const orderMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          order: orderMock,
          range: vi.fn().mockResolvedValue({ data: [], error: null }),
        })),
      };

      await batchSelect(mockSupabase as any, 'test_table', {
        orderBy: { column: 'created_at', ascending: false },
      });

      expect(orderMock).toHaveBeenCalledWith('created_at', { ascending: false });
    });

    it('エラー時に例外を投げる', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          range: vi.fn().mockResolvedValue({ data: null, error: { message: 'DB Error' } }),
        })),
      };

      await expect(
        batchSelect(mockSupabase as any, 'test_table')
      ).rejects.toThrow('Batch select failed at page 0: DB Error');
    });

    it('maxPagesで取得ページ数を制限する', async () => {
      let callCount = 0;
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          range: vi.fn().mockImplementation(() => {
            callCount++;
            return Promise.resolve({ data: [{ id: callCount }], error: null });
          }),
        })),
      };

      const result = await batchSelect(mockSupabase as any, 'test_table', {
        pageSize: 1,
        maxPages: 3,
      });

      expect(result).toHaveLength(3);
      expect(callCount).toBe(3);
    });
  });

  describe('batchProcess', () => {
    it('配列の各要素を処理する', async () => {
      const items = [1, 2, 3, 4, 5];
      const fn = vi.fn().mockImplementation(async (item) => item * 2);

      const result = await batchProcess(items, fn);

      expect(result).toEqual([2, 4, 6, 8, 10]);
      expect(fn).toHaveBeenCalledTimes(5);
    });

    it('並列実行数を制限する', async () => {
      vi.useFakeTimers();
      const items = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
      let maxConcurrent = 0;
      let currentConcurrent = 0;
      const processedItems: number[] = [];

      const fn = vi.fn().mockImplementation(async (item) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        await new Promise((resolve) => setTimeout(resolve, 10));
        currentConcurrent--;
        processedItems.push(item);
        return item;
      });

      const promise = batchProcess(items, fn, 3);

      // 全てのタイマーを進める
      await vi.runAllTimersAsync();
      const result = await promise;

      expect(maxConcurrent).toBeLessThanOrEqual(3);
      expect(result).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      expect(processedItems).toHaveLength(10);
      vi.useRealTimers();
    });

    it('空配列の場合は空配列を返す', async () => {
      const fn = vi.fn();

      const result = await batchProcess([], fn);

      expect(result).toEqual([]);
      expect(fn).not.toHaveBeenCalled();
    });

    it('インデックスが正しく渡される', async () => {
      const items = ['a', 'b', 'c'];
      const fn = vi.fn().mockImplementation(async (item, index) => `${item}-${index}`);

      const result = await batchProcess(items, fn, 2);

      expect(result).toEqual(['a-0', 'b-1', 'c-2']);
    });

    it('エラーが発生した場合は伝播する', async () => {
      const items = [1, 2, 3];
      const fn = vi.fn().mockImplementation(async (item) => {
        if (item === 2) throw new Error('Processing failed');
        return item;
      });

      await expect(batchProcess(items, fn, 1)).rejects.toThrow('Processing failed');
    });
  });
});
