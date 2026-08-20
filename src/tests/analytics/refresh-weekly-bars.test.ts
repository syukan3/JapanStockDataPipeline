/**
 * analytics/refresh-weekly-bars.ts のユニットテスト（Cron A 週足ステップのロジック部）
 *
 * 検証の要点:
 * - 新規追跡銘柄の判定（週足0行）
 * - 検知イベントの追跡銘柄絞り込み・台帳除外・**イベント日の降順**（昇順だと二重適用になる）
 * - RPC apply_weekly_rebase_event の呼び出し順と引数
 * - 直近2 ISO週の対象期間境界
 * - weekly/daily 突合の warn 条件
 */

import { describe, it, expect, vi } from 'vitest';

import {
  aggregateWeeklyBarsByCode,
  applyWeeklyRebaseEvents,
  detectTrackedEventsInWindow,
  fetchAppliedRebaseEventKeys,
  fetchDailyBarsForCodes,
  fetchWeeklyClosesSince,
  findCodesWithoutWeeklyBars,
  findWeeklyDailyMismatches,
  rebaseEventKey,
  recentWeeksStart,
  selectUnappliedEvents,
  ADJ_CLOSE_TOLERANCE,
  CODE_CHUNK_SIZE,
  PAGE_SIZE,
  RECENT_WEEKS,
} from '@/lib/analytics/refresh-weekly-bars';
import {
  subtractDays,
  DETECT_LOOKBACK_DAYS,
  type AdjustmentEvent,
} from '@/lib/analytics/rebase-adjusted-bars';

// ---------------------------------------------------------------------------
// supabase クライアントのモック
//   - `.range()` 終端: Promise を返す（ページング）
//   - `.limit()` 終端: chain 自体が thenable（await で解決）
//   結果はテーブルごとにキューから順に取り出す
// ---------------------------------------------------------------------------

interface QueryResult {
  data: unknown;
  error: { message: string } | null;
}

interface MockClient {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  from: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rpc: any;
  /** テーブル別のチェーン呼び出し記録（[method, ...args]） */
  calls: Record<string, unknown[][]>;
}

function createClient(
  resultsByTable: Record<string, QueryResult[]>,
  rpcResults: QueryResult[] = []
): MockClient {
  const calls: Record<string, unknown[][]> = {};
  const cursor: Record<string, number> = {};
  let rpcCursor = 0;

  const next = (table: string): QueryResult => {
    const results = resultsByTable[table] ?? [{ data: [], error: null }];
    const index = cursor[table] ?? 0;
    cursor[table] = index + 1;
    return results[Math.min(index, results.length - 1)];
  };

  const from = vi.fn((table: string) => {
    calls[table] ??= [];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const chain: any = {};
    for (const method of ['select', 'eq', 'in', 'gte', 'lte', 'order', 'not', 'neq', 'limit']) {
      chain[method] = vi.fn((...args: unknown[]) => {
        calls[table].push([method, ...args]);
        return chain;
      });
    }
    chain.range = vi.fn(async (...args: unknown[]) => {
      calls[table].push(['range', ...args]);
      return next(table);
    });
    chain.then = (resolve: (v: QueryResult) => void) => resolve(next(table));
    return chain;
  });

  const rpc = vi.fn(async (...args: unknown[]) => {
    void args;
    const result = rpcResults[Math.min(rpcCursor, rpcResults.length - 1)] ?? { data: 0, error: null };
    rpcCursor++;
    return result;
  });

  return { from, rpc, calls };
}

const ok = (data: unknown): QueryResult => ({ data, error: null });
const fail = (message: string): QueryResult => ({ data: null, error: { message } });

// ---------------------------------------------------------------------------
// detectTrackedEventsInWindow
// ---------------------------------------------------------------------------

describe('detectTrackedEventsInWindow', () => {
  it('検知窓・追跡銘柄・factor≠1 で絞り、numeric 文字列を数値化する', async () => {
    const client = createClient({
      equity_bar_daily: [
        ok([
          { local_code: '13060', trade_date: '2026-08-17', adjustment_factor: '0.5' },
          { local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 2 },
        ]),
      ],
    });

    const events = await detectTrackedEventsInWindow(client, ['13060', '72030'], '2026-08-20');

    expect(events).toEqual([
      { local_code: '13060', trade_date: '2026-08-17', adjustment_factor: 0.5 },
      { local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 2 },
    ]);
    const calls = client.calls.equity_bar_daily;
    expect(calls).toContainEqual(['in', 'local_code', ['13060', '72030']]);
    expect(calls).toContainEqual(['gte', 'trade_date', subtractDays('2026-08-20', DETECT_LOOKBACK_DAYS)]);
    expect(calls).toContainEqual(['lte', 'trade_date', '2026-08-20']);
    expect(calls).toContainEqual(['not', 'adjustment_factor', 'is', null]);
    expect(calls).toContainEqual(['neq', 'adjustment_factor', 1]);
  });

  it('session=DAY 固定で拾う（週足集計・台帳シーディングと同じ基準に揃える）', async () => {
    const client = createClient({ equity_bar_daily: [ok([])] });
    await detectTrackedEventsInWindow(client, ['72030'], '2026-08-20');
    expect(client.calls.equity_bar_daily).toContainEqual(['eq', 'session', 'DAY']);
  });

  it('DAY 固定で一意になる (local_code, trade_date) 順に並べてページング順を確定させる', async () => {
    const client = createClient({ equity_bar_daily: [ok([])] });
    await detectTrackedEventsInWindow(client, ['72030'], '2026-08-20');
    expect(client.calls.equity_bar_daily.filter(([m]) => m === 'order')).toEqual([
      ['order', 'local_code', { ascending: true }],
      ['order', 'trade_date', { ascending: true }],
    ]);
  });

  it('同一 (銘柄, 日) がページ境界にまたがって重複しても1件に正規化する', async () => {
    const boundary = { local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 0.5 };
    const fullPage = [
      ...Array.from({ length: PAGE_SIZE - 1 }, (_, i) => ({
        local_code: String(10000 + i),
        trade_date: '2026-08-17',
        adjustment_factor: 0.5,
      })),
      boundary,
    ];
    const client = createClient({
      equity_bar_daily: [
        ok(fullPage),
        ok([boundary, { local_code: '99990', trade_date: '2026-08-19', adjustment_factor: 0.2 }]),
      ],
    });

    const events = await detectTrackedEventsInWindow(client, ['72030'], '2026-08-20');

    expect(events.filter((e) => e.local_code === '72030' && e.trade_date === '2026-08-18')).toHaveLength(1);
    expect(events).toHaveLength(PAGE_SIZE + 1);
  });

  it('lookbackDays を明示指定できる', async () => {
    const client = createClient({ equity_bar_daily: [ok([])] });
    await detectTrackedEventsInWindow(client, ['72030'], '2026-08-20', 3);
    expect(client.calls.equity_bar_daily).toContainEqual(['gte', 'trade_date', '2026-08-17']);
  });

  it('PostgREST の Max rows で打ち切られないようページングする', async () => {
    // 権利落ちが集中した週にイベントが1ページを超えても取りこぼさない（取りこぼすと
    // 7日窓を過ぎて再検知されず、片対数チャートに恒久的な段差が残る）
    const fullPage = Array.from({ length: PAGE_SIZE }, (_, i) => ({
      local_code: String(10000 + i),
      trade_date: '2026-08-17',
      adjustment_factor: 0.5,
    }));
    const client = createClient({
      equity_bar_daily: [
        ok(fullPage),
        ok([{ local_code: '99990', trade_date: '2026-08-18', adjustment_factor: 0.2 }]),
      ],
    });

    const events = await detectTrackedEventsInWindow(client, ['72030'], '2026-08-20');

    expect(events).toHaveLength(PAGE_SIZE + 1);
    expect(events[events.length - 1]).toEqual({
      local_code: '99990',
      trade_date: '2026-08-18',
      adjustment_factor: 0.2,
    });
  });

  it('同一 (銘柄, 日) の複数 session 行は1イベントに正規化する', async () => {
    const client = createClient({
      equity_bar_daily: [
        ok([
          { local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 0.5 },
          { local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 0.5 },
        ]),
      ],
    });

    await expect(detectTrackedEventsInWindow(client, ['72030'], '2026-08-20')).resolves.toHaveLength(1);
  });

  it('数値化できない係数は捨てる（NaN を RPC に渡さない）', async () => {
    const client = createClient({
      equity_bar_daily: [ok([{ local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 'N/A' }])],
    });

    await expect(detectTrackedEventsInWindow(client, ['72030'], '2026-08-20')).resolves.toEqual([]);
  });

  it('銘柄が CODE_CHUNK_SIZE を超えたら .in を分割する', async () => {
    const codes = Array.from({ length: CODE_CHUNK_SIZE + 1 }, (_, i) => `C${i}`);
    const client = createClient({ equity_bar_daily: [ok([])] });

    await detectTrackedEventsInWindow(client, codes, '2026-08-20');

    expect(client.calls.equity_bar_daily.filter(([m]) => m === 'in')).toHaveLength(2);
  });

  it('銘柄が0件なら問い合わせない', async () => {
    const client = createClient({});
    await expect(detectTrackedEventsInWindow(client, [], '2026-08-20')).resolves.toEqual([]);
    expect(client.from).not.toHaveBeenCalled();
  });

  it('取得エラーは検知窓付きで throw', async () => {
    const client = createClient({ equity_bar_daily: [fail('statement timeout')] });
    await expect(detectTrackedEventsInWindow(client, ['72030'], '2026-08-20')).rejects.toThrow(
      '分割・併合イベントの検知に失敗しました (2026-08-13..2026-08-20): statement timeout'
    );
  });
});

// ---------------------------------------------------------------------------
// findCodesWithoutWeeklyBars
// ---------------------------------------------------------------------------

describe('findCodesWithoutWeeklyBars', () => {
  it('週足が1行も無い銘柄だけを渡された順で返す', async () => {
    const client = createClient({
      equity_bar_weekly: [ok([{ week_start: '2020-01-06' }]), ok([]), ok([])],
    });

    await expect(findCodesWithoutWeeklyBars(client, ['72030', '285A0', '13060'])).resolves.toEqual([
      '285A0',
      '13060',
    ]);
    expect(client.calls.equity_bar_weekly).toContainEqual(['eq', 'local_code', '285A0']);
    expect(client.calls.equity_bar_weekly).toContainEqual(['limit', 1]);
  });

  it('全銘柄に週足があれば空配列（バックフィルしない）', async () => {
    const client = createClient({ equity_bar_weekly: [ok([{ week_start: '2020-01-06' }])] });
    await expect(findCodesWithoutWeeklyBars(client, ['72030', '13060'])).resolves.toEqual([]);
  });

  it('取得エラーは銘柄コード付きで throw', async () => {
    const client = createClient({ equity_bar_weekly: [fail('timeout')] });
    await expect(findCodesWithoutWeeklyBars(client, ['72030'])).rejects.toThrow(
      'equity_bar_weekly の存在確認に失敗しました (72030): timeout'
    );
  });
});

// ---------------------------------------------------------------------------
// fetchAppliedRebaseEventKeys
// ---------------------------------------------------------------------------

describe('fetchAppliedRebaseEventKeys', () => {
  it('期間内の記録済みイベントをキー集合で返す', async () => {
    const client = createClient({
      equity_bar_weekly_rebase_events: [
        ok([
          { local_code: '72030', event_date: '2026-08-18' },
          { local_code: '13060', event_date: '2026-08-17' },
        ]),
      ],
    });

    const keys = await fetchAppliedRebaseEventKeys(client, ['72030', '13060'], '2026-08-13', '2026-08-20');

    expect(keys).toEqual(new Set([rebaseEventKey('72030', '2026-08-18'), rebaseEventKey('13060', '2026-08-17')]));
    const calls = client.calls.equity_bar_weekly_rebase_events;
    expect(calls).toContainEqual(['in', 'local_code', ['72030', '13060']]);
    expect(calls).toContainEqual(['gte', 'event_date', '2026-08-13']);
    expect(calls).toContainEqual(['lte', 'event_date', '2026-08-20']);
  });

  it('銘柄が0件なら問い合わせない', async () => {
    const client = createClient({});
    await expect(fetchAppliedRebaseEventKeys(client, [], '2026-08-13', '2026-08-20')).resolves.toEqual(
      new Set()
    );
    expect(client.from).not.toHaveBeenCalled();
  });

  it('PAGE_SIZE 単位でページングする', async () => {
    const fullPage = Array.from({ length: PAGE_SIZE }, (_, i) => ({
      local_code: String(10000 + i),
      event_date: '2026-08-18',
    }));
    const client = createClient({
      equity_bar_weekly_rebase_events: [ok(fullPage), ok([{ local_code: '99990', event_date: '2026-08-19' }])],
    });

    const keys = await fetchAppliedRebaseEventKeys(client, ['72030'], '2026-08-13', '2026-08-20');

    expect(keys.size).toBe(PAGE_SIZE + 1);
    const ranges = client.calls.equity_bar_weekly_rebase_events.filter(([m]) => m === 'range');
    expect(ranges).toEqual([
      ['range', 0, PAGE_SIZE - 1],
      ['range', PAGE_SIZE, PAGE_SIZE * 2 - 1],
    ]);
  });

  it('銘柄が CODE_CHUNK_SIZE を超えたら .in を分割する', async () => {
    const codes = Array.from({ length: CODE_CHUNK_SIZE + 1 }, (_, i) => `C${i}`);
    const client = createClient({ equity_bar_weekly_rebase_events: [ok([])] });

    await fetchAppliedRebaseEventKeys(client, codes, '2026-08-13', '2026-08-20');

    const inCalls = client.calls.equity_bar_weekly_rebase_events.filter(([m]) => m === 'in');
    expect(inCalls).toHaveLength(2);
    expect((inCalls[0][2] as string[]).length).toBe(CODE_CHUNK_SIZE);
    expect((inCalls[1][2] as string[]).length).toBe(1);
  });

  it('取得エラーは期間付きで throw', async () => {
    const client = createClient({ equity_bar_weekly_rebase_events: [fail('boom')] });
    await expect(
      fetchAppliedRebaseEventKeys(client, ['72030'], '2026-08-13', '2026-08-20')
    ).rejects.toThrow('equity_bar_weekly_rebase_events の取得に失敗しました (2026-08-13..2026-08-20): boom');
  });
});

// ---------------------------------------------------------------------------
// selectUnappliedEvents
// ---------------------------------------------------------------------------

describe('selectUnappliedEvents', () => {
  const events: AdjustmentEvent[] = [
    { local_code: '72030', trade_date: '2026-08-17', adjustment_factor: 0.5 },
    { local_code: '99840', trade_date: '2026-08-19', adjustment_factor: 0.2 },
    { local_code: '13060', trade_date: '2026-08-18', adjustment_factor: 2 },
  ];

  it('追跡銘柄のイベントだけに絞る', () => {
    const selected = selectUnappliedEvents(events, ['72030', '13060'], new Set());
    expect(selected.map((e) => e.local_code)).toEqual(['13060', '72030']);
  });

  it('台帳に記録済みのイベントを除外する', () => {
    const applied = new Set([rebaseEventKey('13060', '2026-08-18')]);
    const selected = selectUnappliedEvents(events, ['72030', '13060'], applied);
    expect(selected).toEqual([{ local_code: '72030', trade_date: '2026-08-17', adjustment_factor: 0.5 }]);
  });

  it('イベント日の降順（新しい順）に並べる', () => {
    const selected = selectUnappliedEvents(events, ['72030', '13060', '99840'], new Set());
    expect(selected.map((e) => e.trade_date)).toEqual(['2026-08-19', '2026-08-18', '2026-08-17']);
  });

  it('同一日は銘柄コード昇順（実行順を決定的にする）', () => {
    const sameDay: AdjustmentEvent[] = [
      { local_code: '99840', trade_date: '2026-08-18', adjustment_factor: 0.5 },
      { local_code: '13060', trade_date: '2026-08-18', adjustment_factor: 0.5 },
    ];
    const selected = selectUnappliedEvents(sameDay, ['13060', '99840'], new Set());
    expect(selected.map((e) => e.local_code)).toEqual(['13060', '99840']);
  });

  it('同一銘柄の複数イベントも降順になる（古い週へ新イベント係数を重ねない）', () => {
    const twoEvents: AdjustmentEvent[] = [
      { local_code: '72030', trade_date: '2026-08-11', adjustment_factor: 0.5 },
      { local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 0.2 },
    ];
    const selected = selectUnappliedEvents(twoEvents, ['72030'], new Set());
    expect(selected.map((e) => e.trade_date)).toEqual(['2026-08-18', '2026-08-11']);
  });

  it('追跡外・記録済みしか無ければ空配列', () => {
    expect(selectUnappliedEvents(events, ['52020'], new Set())).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// applyWeeklyRebaseEvents
// ---------------------------------------------------------------------------

describe('applyWeeklyRebaseEvents', () => {
  it('渡された順に RPC を呼び、引数と戻り値を記録する', async () => {
    const client = createClient({}, [ok(12), ok('3')]);
    const events: AdjustmentEvent[] = [
      { local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 0.5 },
      { local_code: '13060', trade_date: '2026-08-11', adjustment_factor: 2 },
    ];

    await expect(applyWeeklyRebaseEvents(client, events)).resolves.toEqual([
      { local_code: '72030', event_date: '2026-08-18', adjustment_factor: 0.5, affected_rows: 12 },
      { local_code: '13060', event_date: '2026-08-11', adjustment_factor: 2, affected_rows: 3 },
    ]);
    expect(client.rpc).toHaveBeenNthCalledWith(1, 'apply_weekly_rebase_event', {
      p_local_code: '72030',
      p_event_date: '2026-08-18',
      p_factor: 0.5,
    });
    expect(client.rpc).toHaveBeenNthCalledWith(2, 'apply_weekly_rebase_event', {
      p_local_code: '13060',
      p_event_date: '2026-08-11',
      p_factor: 2,
    });
  });

  it('戻り値 -1（台帳に記録済みでスキップ）もそのまま返す', async () => {
    const client = createClient({}, [ok(-1)]);
    const results = await applyWeeklyRebaseEvents(client, [
      { local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 0.5 },
    ]);
    expect(results[0].affected_rows).toBe(-1);
  });

  it('イベント0件なら RPC を呼ばない', async () => {
    const client = createClient({});
    await expect(applyWeeklyRebaseEvents(client, [])).resolves.toEqual([]);
    expect(client.rpc).not.toHaveBeenCalled();
  });

  it('RPC エラーは銘柄とイベント日を含めて throw し、後続を実行しない', async () => {
    const client = createClient({}, [fail('deadlock detected')]);
    const events: AdjustmentEvent[] = [
      { local_code: '72030', trade_date: '2026-08-18', adjustment_factor: 0.5 },
      { local_code: '13060', trade_date: '2026-08-11', adjustment_factor: 2 },
    ];

    await expect(applyWeeklyRebaseEvents(client, events)).rejects.toThrow(
      'apply_weekly_rebase_event failed for 72030 on 2026-08-18: deadlock detected'
    );
    expect(client.rpc).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// recentWeeksStart
// ---------------------------------------------------------------------------

describe('recentWeeksStart', () => {
  it('既定は先週の月曜（直近2 ISO週）', () => {
    expect(RECENT_WEEKS).toBe(2);
    // 2026-08-20(木) の属する週は 08-17(月)始まり → 先週月曜は 08-10
    expect(recentWeeksStart('2026-08-20')).toBe('2026-08-10');
  });

  it('週初（月曜）・週末（日曜）どちらでも同じ週として扱う', () => {
    expect(recentWeeksStart('2026-08-17')).toBe('2026-08-10');
    expect(recentWeeksStart('2026-08-23')).toBe('2026-08-10');
    expect(recentWeeksStart('2026-08-24')).toBe('2026-08-17');
  });

  it('週数を指定できる（1週なら当週の月曜）', () => {
    expect(recentWeeksStart('2026-08-20', 1)).toBe('2026-08-17');
    expect(recentWeeksStart('2026-08-20', 3)).toBe('2026-08-03');
  });

  it('年をまたいでも暦通りに遡る', () => {
    expect(recentWeeksStart('2026-01-05')).toBe('2025-12-29');
  });
});

// ---------------------------------------------------------------------------
// fetchDailyBarsForCodes
// ---------------------------------------------------------------------------

describe('fetchDailyBarsForCodes', () => {
  it('DAY セッション・追跡銘柄・期間で絞り込む', async () => {
    const client = createClient({
      equity_bar_daily: [ok([{ trade_date: '2026-08-17', local_code: '72030', session: 'DAY' }])],
    });

    const rows = await fetchDailyBarsForCodes(client, ['72030'], '2026-08-10', '2026-08-20');

    expect(rows).toHaveLength(1);
    const calls = client.calls.equity_bar_daily;
    expect(calls).toContainEqual(['eq', 'session', 'DAY']);
    expect(calls).toContainEqual(['in', 'local_code', ['72030']]);
    expect(calls).toContainEqual(['gte', 'trade_date', '2026-08-10']);
    expect(calls).toContainEqual(['lte', 'trade_date', '2026-08-20']);
    expect(calls).toContainEqual(['order', 'local_code', { ascending: true }]);
    expect(calls).toContainEqual(['order', 'trade_date', { ascending: true }]);
  });

  it('PAGE_SIZE 単位でページングして全件返す', async () => {
    const fullPage = Array.from({ length: PAGE_SIZE }, (_, i) => ({
      trade_date: '2026-08-17',
      local_code: String(10000 + i),
    }));
    const client = createClient({
      equity_bar_daily: [ok(fullPage), ok([{ trade_date: '2026-08-18', local_code: '99990' }])],
    });

    const rows = await fetchDailyBarsForCodes(client, ['72030'], '2026-08-10', '2026-08-20');

    expect(rows).toHaveLength(PAGE_SIZE + 1);
    const ranges = client.calls.equity_bar_daily.filter(([m]) => m === 'range');
    expect(ranges).toHaveLength(2);
  });

  it('銘柄が0件なら問い合わせない', async () => {
    const client = createClient({});
    await expect(fetchDailyBarsForCodes(client, [], '2026-08-10', '2026-08-20')).resolves.toEqual([]);
    expect(client.from).not.toHaveBeenCalled();
  });

  it('取得エラーは期間付きで throw', async () => {
    const client = createClient({ equity_bar_daily: [fail('statement timeout')] });
    await expect(
      fetchDailyBarsForCodes(client, ['72030'], '2026-08-10', '2026-08-20')
    ).rejects.toThrow('equity_bar_daily の取得に失敗しました (2026-08-10..2026-08-20): statement timeout');
  });
});

// ---------------------------------------------------------------------------
// aggregateWeeklyBarsByCode
// ---------------------------------------------------------------------------

describe('aggregateWeeklyBarsByCode', () => {
  const bar = (localCode: string, tradeDate: string, close: number) => ({
    trade_date: tradeDate,
    local_code: localCode,
    session: 'DAY',
    open: close,
    high: close,
    low: close,
    close,
    volume: 100,
    turnover_value: 100 * close,
    adjustment_factor: 1,
    adj_open: close,
    adj_high: close,
    adj_low: close,
    adj_close: close,
    adj_volume: 100,
  });

  it('複数銘柄が混在した日足を銘柄ごとに週足へ集計する', () => {
    const records = aggregateWeeklyBarsByCode([
      bar('72030', '2026-08-17', 100),
      bar('13060', '2026-08-17', 3000),
      bar('72030', '2026-08-21', 110),
      bar('72030', '2026-08-24', 120),
    ]);

    expect(records).toHaveLength(3);
    expect(records.map((r) => [r.local_code, r.week_start, r.week_end, r.close])).toEqual([
      ['13060', '2026-08-17', '2026-08-17', 3000],
      ['72030', '2026-08-17', '2026-08-21', 110],
      ['72030', '2026-08-24', '2026-08-24', 120],
    ]);
  });

  it('空入力は空配列', () => {
    expect(aggregateWeeklyBarsByCode([])).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// fetchWeeklyClosesSince
// ---------------------------------------------------------------------------

describe('fetchWeeklyClosesSince', () => {
  it('指定週以降の週足を読み戻す', async () => {
    const client = createClient({
      equity_bar_weekly: [ok([{ local_code: '72030', week_start: '2026-08-17', adj_close: '110' }])],
    });

    const rows = await fetchWeeklyClosesSince(client, ['72030'], '2026-08-10');

    expect(rows).toEqual([{ local_code: '72030', week_start: '2026-08-17', adj_close: '110' }]);
    expect(client.calls.equity_bar_weekly).toContainEqual(['gte', 'week_start', '2026-08-10']);
  });

  it('銘柄が0件なら問い合わせない', async () => {
    const client = createClient({});
    await expect(fetchWeeklyClosesSince(client, [], '2026-08-10')).resolves.toEqual([]);
    expect(client.from).not.toHaveBeenCalled();
  });

  it('取得エラーは週指定付きで throw', async () => {
    const client = createClient({ equity_bar_weekly: [fail('boom')] });
    await expect(fetchWeeklyClosesSince(client, ['72030'], '2026-08-10')).rejects.toThrow(
      'equity_bar_weekly の読み戻しに失敗しました (>= 2026-08-10): boom'
    );
  });
});

// ---------------------------------------------------------------------------
// findWeeklyDailyMismatches
// ---------------------------------------------------------------------------

describe('findWeeklyDailyMismatches', () => {
  const daily = (localCode: string, tradeDate: string, adjClose: unknown) => ({
    trade_date: tradeDate,
    local_code: localCode,
    session: 'DAY',
    adj_close: adjClose,
  });

  it('直近 adj_close が一致していれば警告しない', () => {
    const mismatches = findWeeklyDailyMismatches(
      [{ local_code: '72030', week_start: '2026-08-17', adj_close: '110' }],
      [daily('72030', '2026-08-17', 105), daily('72030', '2026-08-21', 110)]
    );
    expect(mismatches).toEqual([]);
  });

  it('相対差が許容内（0.1%）なら警告しない', () => {
    const mismatches = findWeeklyDailyMismatches(
      [{ local_code: '72030', week_start: '2026-08-17', adj_close: 110.1 }],
      [daily('72030', '2026-08-21', 110)]
    );
    expect(ADJ_CLOSE_TOLERANCE).toBe(0.001);
    expect(mismatches).toEqual([]);
  });

  it('相対差が許容超なら乖離を返す（分割検知漏れの兆候）', () => {
    const mismatches = findWeeklyDailyMismatches(
      [{ local_code: '72030', week_start: '2026-08-17', adj_close: 220 }],
      [daily('72030', '2026-08-21', 110)]
    );
    expect(mismatches).toEqual([
      {
        local_code: '72030',
        reason: 'adj_close_diff',
        week_start: '2026-08-17',
        weekly_adj_close: 220,
        trade_date: '2026-08-21',
        daily_adj_close: 110,
        relative_diff: 1,
      },
    ]);
  });

  it('週足行が読み戻せない銘柄は weekly_row_missing', () => {
    const mismatches = findWeeklyDailyMismatches([], [daily('72030', '2026-08-21', 110)]);
    expect(mismatches).toEqual([
      {
        local_code: '72030',
        reason: 'weekly_row_missing',
        week_start: null,
        weekly_adj_close: null,
        trade_date: '2026-08-21',
        daily_adj_close: 110,
        relative_diff: null,
      },
    ]);
  });

  it('週足の adj_close が null なら乖離として報告する', () => {
    const mismatches = findWeeklyDailyMismatches(
      [{ local_code: '72030', week_start: '2026-08-17', adj_close: null }],
      [daily('72030', '2026-08-21', 110)]
    );
    expect(mismatches[0]).toMatchObject({ reason: 'adj_close_diff', weekly_adj_close: null, relative_diff: null });
  });

  it('直近日足に adj_close が無い銘柄（売買停止等）は比較しない', () => {
    const mismatches = findWeeklyDailyMismatches(
      [{ local_code: '72030', week_start: '2026-08-17', adj_close: 110 }],
      [daily('72030', '2026-08-21', null)]
    );
    expect(mismatches).toEqual([]);
  });

  it('DAY 以外のセッションは比較対象にしない', () => {
    const mismatches = findWeeklyDailyMismatches(
      [{ local_code: '72030', week_start: '2026-08-17', adj_close: 110 }],
      [{ trade_date: '2026-08-21', local_code: '72030', session: 'AM', adj_close: 220 }]
    );
    expect(mismatches).toEqual([]);
  });

  it('日足の最新日・週足の最新週で比較する（古い行に引きずられない）', () => {
    const mismatches = findWeeklyDailyMismatches(
      [
        { local_code: '72030', week_start: '2026-08-10', adj_close: 50 },
        { local_code: '72030', week_start: '2026-08-17', adj_close: 110 },
      ],
      [daily('72030', '2026-08-17', 50), daily('72030', '2026-08-21', 110)]
    );
    expect(mismatches).toEqual([]);
  });

  it('日足が0円でも0除算せず、値が違えば乖離として報告する', () => {
    const zeroMatch = findWeeklyDailyMismatches(
      [{ local_code: '72030', week_start: '2026-08-17', adj_close: 0 }],
      [daily('72030', '2026-08-21', 0)]
    );
    expect(zeroMatch).toEqual([]);

    const zeroDiff = findWeeklyDailyMismatches(
      [{ local_code: '72030', week_start: '2026-08-17', adj_close: 1 }],
      [daily('72030', '2026-08-21', 0)]
    );
    expect(zeroDiff[0]).toMatchObject({ reason: 'adj_close_diff', relative_diff: Infinity });
  });

  it('複数銘柄の乖離を銘柄コード昇順で返す', () => {
    const mismatches = findWeeklyDailyMismatches(
      [
        { local_code: '72030', week_start: '2026-08-17', adj_close: 220 },
        { local_code: '13060', week_start: '2026-08-17', adj_close: 6000 },
      ],
      [daily('72030', '2026-08-21', 110), daily('13060', '2026-08-21', 3000)]
    );
    expect(mismatches.map((m) => m.local_code)).toEqual(['13060', '72030']);
  });
});
