/**
 * analytics/weekly-bars-backfill.ts のユニットテスト
 *
 * 検証の要点:
 * - 取得は**取得専用**の fetchEquityBarsDailyPaginated（equity_bar_daily へ upsert する
 *   syncEquityBarsDailyForCode を使っていないこと）
 * - 書き込みは analytics スキーマの equity_bar_weekly のみ・on_conflict は local_code,week_start
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const {
  mockFetchPaginated,
  mockSyncForCode,
  mockCreateJQuantsClient,
  mockCreateAdminClient,
  mockBatchUpsert,
} = vi.hoisted(() => ({
  mockFetchPaginated: vi.fn(),
  mockSyncForCode: vi.fn(),
  mockCreateJQuantsClient: vi.fn(),
  mockCreateAdminClient: vi.fn(),
  mockBatchUpsert: vi.fn(),
}));

vi.mock('@/lib/jquants/endpoints/equity-bars-daily', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/jquants/endpoints/equity-bars-daily')>();
  return {
    ...actual,
    // toEquityBarDailyRecord は実物を使う（API → DBレコードのマッピング契約ごと検証する）
    fetchEquityBarsDailyPaginated: mockFetchPaginated,
    syncEquityBarsDailyForCode: mockSyncForCode,
  };
});

vi.mock('@/lib/jquants/client', () => ({
  JQuantsClient: vi.fn(),
  createJQuantsClient: mockCreateJQuantsClient,
}));

vi.mock('@/lib/supabase/admin', () => ({
  createAdminClient: mockCreateAdminClient,
  getSupabaseAdmin: vi.fn(),
}));

vi.mock('@/lib/utils/batch', () => ({
  batchUpsert: mockBatchUpsert,
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
  backfillWeeklyBarsForCode,
  WEEKLY_BARS_TABLE,
  WEEKLY_BARS_ON_CONFLICT,
} from '@/lib/analytics/weekly-bars-backfill';
import type { EquityBarDailyItem } from '@/lib/jquants/types';

/** APIレスポンスの1日分（DAYセッションのみ） */
function item(overrides: Partial<EquityBarDailyItem> & { Date: string }): EquityBarDailyItem {
  return {
    Code: '72030',
    O: 100,
    H: 110,
    L: 90,
    C: 105,
    Vo: 1000,
    Va: 105000,
    AdjFactor: 1,
    AdjO: 100,
    AdjH: 110,
    AdjL: 90,
    AdjC: 105,
    AdjVo: 1000,
    ...overrides,
  };
}

/** ページの配列を非同期ジェネレータとして返すモック実装を仕込む */
function givenPages(pages: EquityBarDailyItem[][]): void {
  mockFetchPaginated.mockImplementation(async function* () {
    for (const page of pages) {
      yield page;
    }
  });
}

const analyticsClient = { tag: 'analytics' };

beforeEach(() => {
  mockCreateJQuantsClient.mockReturnValue({ tag: 'jquants' });
  mockCreateAdminClient.mockReturnValue(analyticsClient);
  mockBatchUpsert.mockResolvedValue({ inserted: 0, errors: [], batchCount: 0 });
});

describe('backfillWeeklyBarsForCode', () => {
  it('取得 → 週足集計 → analytics.equity_bar_weekly へ upsert する', async () => {
    givenPages([
      [
        item({ Date: '2026-08-17', O: 100, H: 120, L: 95, C: 110, Vo: 1000, Va: 110000, AdjO: 100, AdjH: 120, AdjL: 95, AdjC: 110, AdjVo: 1000 }),
        item({ Date: '2026-08-21', O: 112, H: 130, L: 90, C: 92, Vo: 2000, Va: 184000, AdjO: 112, AdjH: 130, AdjL: 90, AdjC: 92, AdjVo: 2000 }),
      ],
    ]);
    mockBatchUpsert.mockResolvedValue({ inserted: 1, errors: [], batchCount: 1 });

    const result = await backfillWeeklyBarsForCode('72030', '2016-08-20', '2026-08-20');

    expect(mockFetchPaginated).toHaveBeenCalledWith(
      { tag: 'jquants' },
      { code: '72030', from: '2016-08-20', to: '2026-08-20' }
    );
    expect(mockCreateAdminClient).toHaveBeenCalledWith('analytics');
    expect(mockBatchUpsert).toHaveBeenCalledWith(
      analyticsClient,
      WEEKLY_BARS_TABLE,
      [
        {
          local_code: '72030',
          week_start: '2026-08-17',
          week_end: '2026-08-21',
          open: 100,
          high: 130,
          low: 90,
          close: 92,
          volume: 3000,
          turnover_value: 294000,
          adjustment_factor: 1,
          adj_open: 100,
          adj_high: 130,
          adj_low: 90,
          adj_close: 92,
          adj_volume: 3000,
        },
      ],
      WEEKLY_BARS_ON_CONFLICT
    );
    expect(result).toEqual({
      local_code: '72030',
      fetched: 2,
      weeks: 1,
      upserted: 1,
      pageCount: 1,
    });
  });

  it('on_conflict は local_code,week_start（週で安定させる）', () => {
    expect(WEEKLY_BARS_ON_CONFLICT).toBe('local_code,week_start');
    expect(WEEKLY_BARS_TABLE).toBe('equity_bar_weekly');
  });

  it('equity_bar_daily へは一切書かない（取得専用経路を使う）', async () => {
    givenPages([[item({ Date: '2026-08-17' })]]);

    await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31');

    expect(mockSyncForCode).not.toHaveBeenCalled();
    for (const call of mockBatchUpsert.mock.calls) {
      expect(call[1]).toBe('equity_bar_weekly');
    }
  });

  it('ページを跨いだ日足も同一週にまとまる', async () => {
    givenPages([
      [item({ Date: '2026-08-17', C: 110 })],
      [item({ Date: '2026-08-18', C: 120 })],
      [item({ Date: '2026-08-24', C: 130 })],
    ]);

    const result = await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31');

    const rows = mockBatchUpsert.mock.calls[0][2];
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ week_start: '2026-08-17', week_end: '2026-08-18', close: 120 });
    expect(rows[1]).toMatchObject({ week_start: '2026-08-24', week_end: '2026-08-24', close: 130 });
    expect(result.pageCount).toBe(3);
    expect(result.fetched).toBe(3);
  });

  it('upsert 行に ingested_at は含めない（DB default に任せる）', async () => {
    givenPages([[item({ Date: '2026-08-17' })]]);

    await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31');

    expect(mockBatchUpsert.mock.calls[0][2][0]).not.toHaveProperty('ingested_at');
  });

  it('分割週は週内 factor の積が adjustment_factor になる', async () => {
    givenPages([
      [
        item({ Date: '2026-08-17', AdjFactor: 1 }),
        item({ Date: '2026-08-18', AdjFactor: 0.5 }),
      ],
    ]);

    await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31');

    expect(mockBatchUpsert.mock.calls[0][2][0].adjustment_factor).toBe(0.5);
  });

  it('取得0件なら upsert しない', async () => {
    givenPages([[]]);

    const result = await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31');

    expect(mockBatchUpsert).not.toHaveBeenCalled();
    expect(result).toEqual({
      local_code: '72030',
      fetched: 0,
      weeks: 0,
      upserted: 0,
      pageCount: 1,
    });
  });

  it('注入されたクライアントを使い、既定クライアントを作らない', async () => {
    givenPages([[item({ Date: '2026-08-17' })]]);
    const injectedClient = { tag: 'injected-jquants' };
    const injectedAnalytics = { tag: 'injected-analytics' };

    await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31', {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      client: injectedClient as any,
      analytics: injectedAnalytics,
    });

    expect(mockCreateJQuantsClient).not.toHaveBeenCalled();
    expect(mockCreateAdminClient).not.toHaveBeenCalled();
    expect(mockFetchPaginated.mock.calls[0][0]).toBe(injectedClient);
    expect(mockBatchUpsert.mock.calls[0][0]).toBe(injectedAnalytics);
  });

  it('upsert エラーは銘柄コード付きで throw', async () => {
    givenPages([[item({ Date: '2026-08-17' })]]);
    mockBatchUpsert.mockResolvedValue({
      inserted: 0,
      errors: [new Error('duplicate key')],
      batchCount: 1,
    });

    await expect(
      backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31')
    ).rejects.toThrow('equity_bar_weekly upsert failed for 72030: duplicate key');
  });

  it('引数が不正なら API を呼ばずに throw', async () => {
    await expect(backfillWeeklyBarsForCode('', '2026-08-01', '2026-08-31')).rejects.toThrow('code は必須');
    await expect(backfillWeeklyBarsForCode('72030', '2026/08/01', '2026-08-31')).rejects.toThrow('YYYY-MM-DD');
    await expect(backfillWeeklyBarsForCode('72030', '2026-08-31', '2026-08-01')).rejects.toThrow('from は to 以前');
    expect(mockFetchPaginated).not.toHaveBeenCalled();
  });
});
