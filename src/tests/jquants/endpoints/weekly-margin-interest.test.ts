/** 信用取引週末残高の変換・同期ウィンドウ・プルーニング保護を検証する。 */

import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { JQuantsClient } from '@/lib/jquants/client';
import type { WeeklyMarginInterestItem } from '@/lib/jquants/types';

const mocks = vi.hoisted(() => ({
  getSupabaseAdmin: vi.fn(),
  createAdminClient: vi.fn(),
  batchUpsert: vi.fn(),
  batchSelect: vi.fn(),
}));

vi.mock('@/lib/supabase/admin', () => ({
  getSupabaseAdmin: mocks.getSupabaseAdmin,
  createAdminClient: mocks.createAdminClient,
}));

vi.mock('@/lib/utils/batch', () => ({
  batchUpsert: mocks.batchUpsert,
  batchSelect: mocks.batchSelect,
}));

import {
  toWeeklyMarginInterestRecord,
  fetchWeeklyMarginInterest,
  syncWeeklyMarginInterest,
  syncWeeklyMarginInterestWithWindow,
  fetchProtectedLocalCodes,
  pruneWeeklyMarginInterest,
  getLatestWeeklyMarginInterestDateFromDB,
} from '@/lib/jquants/endpoints/weekly-margin-interest';

function item(overrides?: Partial<WeeklyMarginInterestItem>): WeeklyMarginInterestItem {
  return {
    Date: '2026-07-10',
    Code: '13010',
    ShrtVol: 1000,
    LongVol: 5000,
    ShrtNegVol: 100,
    LongNegVol: 500,
    ShrtStdVol: 900,
    LongStdVol: 4500,
    IssType: '2',
    ...overrides,
  };
}

function client(items: WeeklyMarginInterestItem[]): JQuantsClient {
  return {
    getWeeklyMarginInterest: vi.fn().mockResolvedValue(items),
  } as unknown as JQuantsClient;
}

/** Supabaseクエリチェーンのthenableモック（呼び出し順に結果を返す） */
type ChainMock = Record<string, ReturnType<typeof vi.fn>> & {
  then: (resolve: (value: unknown) => void) => void;
};

function createChain(result: unknown): ChainMock {
  const chain = {} as ChainMock;
  for (const method of ['select', 'delete', 'lt', 'gte', 'not', 'order', 'limit']) {
    chain[method] = vi.fn().mockReturnValue(chain);
  }
  chain.then = (resolve: (value: unknown) => void) => resolve(result);
  return chain;
}

function createSupabase(results: unknown[]): { from: ReturnType<typeof vi.fn>; chains: ChainMock[] } {
  const chains = results.map(createChain);
  let call = 0;
  const from = vi.fn().mockImplementation(() => {
    const chain = chains[Math.min(call, chains.length - 1)];
    call++;
    return chain;
  });
  return { from, chains };
}

describe('toWeeklyMarginInterestRecord', () => {
  it('APIフィールドをDB列名にマッピングする', () => {
    expect(toWeeklyMarginInterestRecord(item())).toEqual({
      local_code: '13010',
      application_date: '2026-07-10',
      short_total: 1000,
      long_total: 5000,
      short_negotiable: 100,
      long_negotiable: 500,
      short_standardized: 900,
      long_standardized: 4500,
      issue_type: 2,
    });
  });

  it('欠損数値とIssType未設定はnullにする', () => {
    const record = toWeeklyMarginInterestRecord({
      Date: '2026-07-10',
      Code: '13010',
    });
    expect(record.short_total).toBeNull();
    expect(record.long_total).toBeNull();
    expect(record.short_negotiable).toBeNull();
    expect(record.long_negotiable).toBeNull();
    expect(record.short_standardized).toBeNull();
    expect(record.long_standardized).toBeNull();
    expect(record.issue_type).toBeNull();
  });

  it('IssTypeが数値化できない場合はnullにする', () => {
    expect(toWeeklyMarginInterestRecord(item({ IssType: 'x' })).issue_type).toBeNull();
    expect(toWeeklyMarginInterestRecord(item({ IssType: '' })).issue_type).toBeNull();
    expect(toWeeklyMarginInterestRecord(item({ IssType: null })).issue_type).toBeNull();
  });
});

describe('fetchWeeklyMarginInterest', () => {
  it('401/403はStandard未契約の可能性を含むメッセージで再throwする', async () => {
    for (const statusCode of [401, 403]) {
      const error = Object.assign(new Error('Forbidden'), { statusCode });
      const failingClient = {
        getWeeklyMarginInterest: vi.fn().mockRejectedValue(error),
      } as unknown as JQuantsClient;

      await expect(
        fetchWeeklyMarginInterest(failingClient, { code: '13010' })
      ).rejects.toThrow('J-Quants Standard未契約の可能性');
    }
  });

  it('その他のエラーはそのまま伝播する', async () => {
    const error = Object.assign(new Error('HTTP 500'), { statusCode: 500 });
    const failingClient = {
      getWeeklyMarginInterest: vi.fn().mockRejectedValue(error),
    } as unknown as JQuantsClient;

    await expect(
      fetchWeeklyMarginInterest(failingClient, { code: '13010' })
    ).rejects.toBe(error);
  });
});

describe('syncWeeklyMarginInterest', () => {
  const supabase = { tag: 'core-client' };

  beforeEach(() => {
    mocks.getSupabaseAdmin.mockReturnValue(supabase);
    mocks.batchUpsert.mockResolvedValue({ inserted: 1, errors: [], batchCount: 1 });
  });

  it('変換したレコードをbatchUpsertに渡す', async () => {
    const result = await syncWeeklyMarginInterest(
      { from: '2026-06-13', to: '2026-07-18' },
      { client: client([item()]) }
    );

    expect(mocks.batchUpsert).toHaveBeenCalledWith(
      supabase,
      'weekly_margin_interest',
      [toWeeklyMarginInterestRecord(item())],
      'local_code,application_date',
      expect.any(Object)
    );
    expect(result).toEqual({ fetched: 1, inserted: 1, errors: [] });
  });

  it('データ0件はupsertせずに終了する', async () => {
    const result = await syncWeeklyMarginInterest({ date: '2026-01-02' }, { client: client([]) });

    expect(mocks.batchUpsert).not.toHaveBeenCalled();
    expect(result).toEqual({ fetched: 0, inserted: 0, errors: [] });
  });

  it('dryRunはDB保存をスキップする', async () => {
    const result = await syncWeeklyMarginInterest(
      { code: '13010' },
      { client: client([item()]), dryRun: true }
    );

    expect(mocks.batchUpsert).not.toHaveBeenCalled();
    expect(result).toEqual({ fetched: 1, inserted: 0, errors: [] });
  });
});

describe('syncWeeklyMarginInterestWithWindow', () => {
  beforeEach(() => {
    mocks.getSupabaseAdmin.mockReturnValue({});
    mocks.batchUpsert.mockResolvedValue({ inserted: 0, errors: [], batchCount: 0 });
  });

  it('デフォルト35日ウィンドウの暦日を date= 指定で日次ループ取得する', async () => {
    // /markets/margin-interest は from/to 単独指定不可（code か date が必須）のため日次ループになる
    const apiClient = client([]);
    await syncWeeklyMarginInterestWithWindow(undefined, {
      client: apiClient,
      baseDate: new Date('2026-07-18T00:00:00+09:00'),
    });

    expect(apiClient.getWeeklyMarginInterest).toHaveBeenCalledTimes(36); // 06-13〜07-18 の36暦日
    expect(apiClient.getWeeklyMarginInterest).toHaveBeenNthCalledWith(1, { date: '2026-06-13' });
    expect(apiClient.getWeeklyMarginInterest).toHaveBeenLastCalledWith({ date: '2026-07-18' });
  });

  it('windowDaysを1〜365にクランプする', async () => {
    const apiClient = client([]);
    await syncWeeklyMarginInterestWithWindow(1000, {
      client: apiClient,
      baseDate: new Date('2026-07-18T00:00:00+09:00'),
    });

    expect(apiClient.getWeeklyMarginInterest).toHaveBeenCalledTimes(366); // 365日クランプ+当日
    expect(apiClient.getWeeklyMarginInterest).toHaveBeenNthCalledWith(1, { date: '2025-07-18' });
    expect(apiClient.getWeeklyMarginInterest).toHaveBeenLastCalledWith({ date: '2026-07-18' });
  });

  it('非有限のwindowDaysを拒否する', async () => {
    await expect(syncWeeklyMarginInterestWithWindow(NaN)).rejects.toThrow(
      'windowDays must be a finite number'
    );
  });
});

describe('fetchProtectedLocalCodes', () => {
  it('未削除ポートフォリオの純保有>0とウォッチ銘柄を重複なしで返す', async () => {
    mocks.createAdminClient.mockReturnValue({ tag: 'portfolio-client' });
    mocks.batchSelect
      .mockResolvedValueOnce([
        { id: 'p1', deleted_at: null },
        { id: 'p2', deleted_at: '2026-01-01T00:00:00Z' },
      ])
      .mockResolvedValueOnce([
        // 13010: 買い切り済み（純保有0）→ 保護対象外
        { portfolio_id: 'p1', local_code: '13010', trade_type: 'buy', quantity: 100 },
        { portfolio_id: 'p1', local_code: '13010', trade_type: 'sell', quantity: 100 },
        // 57130: 純保有100 → 保護対象
        { portfolio_id: 'p1', local_code: '57130', trade_type: 'buy', quantity: 100 },
        // 99990: 削除済みポートフォリオ → 保護対象外
        { portfolio_id: 'p2', local_code: '99990', trade_type: 'buy', quantity: 100 },
      ])
      .mockResolvedValueOnce([
        { local_code: '86970' },
        { local_code: '57130' },
      ]);

    const codes = await fetchProtectedLocalCodes();

    expect(mocks.createAdminClient).toHaveBeenCalledWith('portfolio');
    expect(codes).toEqual(['57130', '86970']);
  });

  it('取引もウォッチも無ければ空配列を返す', async () => {
    mocks.createAdminClient.mockReturnValue({});
    mocks.batchSelect
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);

    await expect(fetchProtectedLocalCodes()).resolves.toEqual([]);
  });
});

describe('pruneWeeklyMarginInterest', () => {
  const baseDate = new Date('2026-07-18T00:00:00+09:00');
  const cutoffDate = '2025-07-18';

  it('カットオフより古い行を日付レンジを刻んで削除し、保護銘柄を除外する', async () => {
    const { from, chains } = createSupabase([
      { data: [{ application_date: '2025-03-01' }], error: null },
      { error: null, count: 10 },
      { error: null, count: 5 },
    ]);
    mocks.getSupabaseAdmin.mockReturnValue({ from });

    const result = await pruneWeeklyMarginInterest({
      baseDate,
      protectedCodes: ['13010', '57130'],
    });

    expect(result).toEqual({ deleted: 15, protectedCount: 2, cutoffDate });

    // 最古日の探索はカットオフ未満に限定される
    expect(chains[0].lt).toHaveBeenCalledWith('application_date', cutoffDate);

    // 90日刻み: [2025-03-01, 2025-05-30) → [2025-05-30, 2025-07-18)
    expect(chains[1].gte).toHaveBeenCalledWith('application_date', '2025-03-01');
    expect(chains[1].lt).toHaveBeenCalledWith('application_date', '2025-05-30');
    expect(chains[2].gte).toHaveBeenCalledWith('application_date', '2025-05-30');
    expect(chains[2].lt).toHaveBeenCalledWith('application_date', cutoffDate);

    // 保護銘柄はどのDELETEウィンドウでも除外される
    expect(chains[1].not).toHaveBeenCalledWith('local_code', 'in', '(13010,57130)');
    expect(chains[2].not).toHaveBeenCalledWith('local_code', 'in', '(13010,57130)');

    // カットオフ（1年）より新しい行に触れるウィンドウは存在しない
    for (const chain of chains.slice(1)) {
      for (const [, upperBound] of chain.lt.mock.calls) {
        expect(upperBound <= cutoffDate).toBe(true);
      }
    }
  });

  it('保護リストが空ならnotフィルタを付けない', async () => {
    const { from, chains } = createSupabase([
      { data: [{ application_date: '2025-07-01' }], error: null },
      { error: null, count: 3 },
    ]);
    mocks.getSupabaseAdmin.mockReturnValue({ from });

    const result = await pruneWeeklyMarginInterest({ baseDate, protectedCodes: [] });

    expect(result.deleted).toBe(3);
    expect(chains[1].not).not.toHaveBeenCalled();
  });

  it('カットオフより古い行が無ければ何も削除しない', async () => {
    const { from } = createSupabase([{ data: [], error: null }]);
    mocks.getSupabaseAdmin.mockReturnValue({ from });

    const result = await pruneWeeklyMarginInterest({ baseDate, protectedCodes: [] });

    expect(result).toEqual({ deleted: 0, protectedCount: 0, cutoffDate });
    expect(from).toHaveBeenCalledTimes(1);
  });

  it('保護リスト未指定ならportfolioスキーマから取得する', async () => {
    mocks.createAdminClient.mockReturnValue({});
    mocks.batchSelect
      .mockResolvedValueOnce([{ id: 'p1', deleted_at: null }])
      .mockResolvedValueOnce([
        { portfolio_id: 'p1', local_code: '57130', trade_type: 'buy', quantity: 100 },
      ])
      .mockResolvedValueOnce([]);

    const { from, chains } = createSupabase([
      { data: [{ application_date: '2025-07-01' }], error: null },
      { error: null, count: 1 },
    ]);
    mocks.getSupabaseAdmin.mockReturnValue({ from });

    const result = await pruneWeeklyMarginInterest({ baseDate });

    expect(result.protectedCount).toBe(1);
    expect(chains[1].not).toHaveBeenCalledWith('local_code', 'in', '(57130)');
  });

  it('DELETE失敗はエラーにする', async () => {
    const { from } = createSupabase([
      { data: [{ application_date: '2025-07-01' }], error: null },
      { error: { message: 'timeout' }, count: null },
    ]);
    mocks.getSupabaseAdmin.mockReturnValue({ from });

    await expect(
      pruneWeeklyMarginInterest({ baseDate, protectedCodes: [] })
    ).rejects.toThrow('Failed to prune weekly margin interest');
  });
});

describe('getLatestWeeklyMarginInterestDateFromDB', () => {
  it('最新の申込日を返す', async () => {
    const { from } = createSupabase([
      { data: [{ application_date: '2026-07-10' }], error: null },
    ]);
    mocks.getSupabaseAdmin.mockReturnValue({ from });

    await expect(getLatestWeeklyMarginInterestDateFromDB()).resolves.toBe('2026-07-10');
  });

  it('データが無ければnullを返す', async () => {
    const { from } = createSupabase([{ data: [], error: null }]);
    mocks.getSupabaseAdmin.mockReturnValue({ from });

    await expect(getLatestWeeklyMarginInterestDateFromDB()).resolves.toBeNull();
  });
});
