/**
 * analytics/weekly-bars-backfill.ts のユニットテスト
 *
 * 検証の要点:
 * - 取得は**取得専用**の fetchEquityBarsDailyPaginated（equity_bar_daily へ upsert する
 *   syncEquityBarsDailyForCode を使っていないこと）
 * - 書き込みは analytics スキーマの equity_bar_weekly と台帳のみ・on_conflict は local_code,week_start
 * - 二重調整防止の台帳シーディング（factor≠1 の日を「適用済み」として直接記録・RPCは呼ばない）
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
  extractRebaseEvents,
  WEEKLY_BACKFILL_BATCH_SIZE,
  WEEKLY_BARS_TABLE,
  WEEKLY_BARS_ON_CONFLICT,
  WEEKLY_REBASE_EVENTS_TABLE,
  WEEKLY_REBASE_EVENTS_ON_CONFLICT,
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

/** 台帳 upsert 用の analytics クライアントモック（batchUpsert 自体はモック済みなので from は台帳専用） */
function createAnalyticsClient(upsertResult: { error: { message: string } | null } = { error: null }) {
  const upsert = vi.fn(async () => upsertResult);
  const from = vi.fn(() => ({ upsert }));
  return { tag: 'analytics', from, upsert };
}

let analyticsClient = createAnalyticsClient();

beforeEach(() => {
  analyticsClient = createAnalyticsClient();
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
      WEEKLY_BARS_ON_CONFLICT,
      { batchSize: WEEKLY_BACKFILL_BATCH_SIZE }
    );
    expect(result).toEqual({
      local_code: '72030',
      fetched: 2,
      weeks: 1,
      upserted: 1,
      pageCount: 1,
      rebaseEvents: 0,
    });
  });

  it('on_conflict は local_code,week_start（週で安定させる）', () => {
    expect(WEEKLY_BARS_ON_CONFLICT).toBe('local_code,week_start');
    expect(WEEKLY_BARS_TABLE).toBe('equity_bar_weekly');
  });

  it('10年分（約520週）を1バッチで投入する（部分書き込みで「バックフィル済み」と誤判定させない）', async () => {
    // 10年 = 522 ISO週。バッチが割れると先行バッチだけ残った銘柄が翌日以降の
    // 新規判定（行の有無）から外れ、中間週が恒久欠損する。
    expect(WEEKLY_BACKFILL_BATCH_SIZE).toBeGreaterThan(522);

    const days = Array.from({ length: 600 }, (_, i) => {
      const d = new Date(Date.UTC(2016, 0, 4) + i * 7 * 86400000);
      return item({ Date: d.toISOString().slice(0, 10) });
    });
    givenPages([days]);

    await backfillWeeklyBarsForCode('72030', '2016-01-04', '2027-06-30');

    expect(mockBatchUpsert).toHaveBeenCalledTimes(1);
    expect(mockBatchUpsert.mock.calls[0][2]).toHaveLength(600);
    expect(mockBatchUpsert.mock.calls[0][4]).toEqual({ batchSize: WEEKLY_BACKFILL_BATCH_SIZE });
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
      rebaseEvents: 0,
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

// ---------------------------------------------------------------------------
// 台帳シーディング（二重調整防止・§4.2）
// ---------------------------------------------------------------------------

describe('extractRebaseEvents', () => {
  const bar = (overrides: Record<string, unknown>) => ({
    trade_date: '2026-08-17',
    local_code: '72030',
    session: 'DAY',
    adjustment_factor: 1,
    ...overrides,
  });

  it('factor が非 null かつ ≠1 の日だけを抽出し日付昇順で返す', () => {
    expect(
      extractRebaseEvents([
        bar({ trade_date: '2026-08-21', adjustment_factor: 0.5 }),
        bar({ trade_date: '2026-08-18', adjustment_factor: 1 }),
        bar({ trade_date: '2026-08-17', adjustment_factor: 2 }),
        bar({ trade_date: '2026-08-19', adjustment_factor: null }),
        bar({ trade_date: '2026-08-20', adjustment_factor: undefined }),
      ])
    ).toEqual([
      { local_code: '72030', event_date: '2026-08-17', adjustment_factor: 2 },
      { local_code: '72030', event_date: '2026-08-21', adjustment_factor: 0.5 },
    ]);
  });

  it('numeric が文字列でも数値化する', () => {
    expect(extractRebaseEvents([bar({ adjustment_factor: '0.2' })])).toEqual([
      { local_code: '72030', event_date: '2026-08-17', adjustment_factor: 0.2 },
    ]);
    expect(extractRebaseEvents([bar({ adjustment_factor: '1' })])).toEqual([]);
  });

  it('DAY 以外のセッションと数値化できない値は無視する', () => {
    expect(
      extractRebaseEvents([
        bar({ session: 'AM', adjustment_factor: 0.5 }),
        bar({ trade_date: '2026-08-18', adjustment_factor: 'N/A' }),
        bar({ trade_date: '2026-08-19', adjustment_factor: '' }),
      ])
    ).toEqual([]);
  });

  it('同一日の重複行は1件にまとめる', () => {
    expect(
      extractRebaseEvents([bar({ adjustment_factor: 0.5 }), bar({ adjustment_factor: 0.5 })])
    ).toHaveLength(1);
  });
});

describe('backfillWeeklyBarsForCode（台帳シーディング）', () => {
  it('factor≠1 の日を「適用済み」として台帳へ upsert する（ignoreDuplicates）', async () => {
    givenPages([
      [
        item({ Date: '2026-08-17', AdjFactor: 1 }),
        item({ Date: '2026-08-18', AdjFactor: 0.5 }),
        item({ Date: '2026-08-24', AdjFactor: 2 }),
      ],
    ]);

    const result = await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31');

    expect(analyticsClient.from).toHaveBeenCalledWith(WEEKLY_REBASE_EVENTS_TABLE);
    expect(analyticsClient.upsert).toHaveBeenCalledWith(
      [
        { local_code: '72030', event_date: '2026-08-18', adjustment_factor: 0.5 },
        { local_code: '72030', event_date: '2026-08-24', adjustment_factor: 2 },
      ],
      { onConflict: WEEKLY_REBASE_EVENTS_ON_CONFLICT, ignoreDuplicates: true }
    );
    expect(result.rebaseEvents).toBe(2);
  });

  it('RPC apply_weekly_rebase_event は呼ばない（API の adj は既にイベント織込み済み）', async () => {
    givenPages([[item({ Date: '2026-08-18', AdjFactor: 0.5 })]]);
    const injectedAnalytics = { ...createAnalyticsClient(), rpc: vi.fn() };

    await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31', {
      analytics: injectedAnalytics,
    });

    expect(injectedAnalytics.rpc).not.toHaveBeenCalled();
    expect(injectedAnalytics.from).toHaveBeenCalledWith(WEEKLY_REBASE_EVENTS_TABLE);
  });

  it('分割の無い銘柄では台帳に触らない', async () => {
    givenPages([[item({ Date: '2026-08-17', AdjFactor: 1 })]]);

    const result = await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31');

    expect(analyticsClient.from).not.toHaveBeenCalled();
    expect(result.rebaseEvents).toBe(0);
  });

  it('台帳は週足 upsert より前に書く（中断しても翌日のバックフィルで自己修復させる）', async () => {
    givenPages([[item({ Date: '2026-08-18', AdjFactor: 0.5 })]]);
    const order: string[] = [];
    analyticsClient.upsert.mockImplementation(async () => {
      order.push('ledger');
      return { error: null };
    });
    mockBatchUpsert.mockImplementation(async () => {
      order.push('weekly');
      return { inserted: 1, errors: [], batchCount: 1 };
    });

    await backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31');

    expect(order).toEqual(['ledger', 'weekly']);
  });

  it('台帳 upsert エラーは週足を書かずに throw', async () => {
    givenPages([[item({ Date: '2026-08-18', AdjFactor: 0.5 })]]);
    analyticsClient.upsert.mockResolvedValue({ error: { message: 'permission denied' } });

    await expect(backfillWeeklyBarsForCode('72030', '2026-08-01', '2026-08-31')).rejects.toThrow(
      'equity_bar_weekly_rebase_events upsert failed for 72030: permission denied'
    );
    expect(mockBatchUpsert).not.toHaveBeenCalled();
  });
});
