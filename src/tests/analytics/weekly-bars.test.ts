/**
 * analytics/weekly-bars.ts のユニットテスト
 *
 * 集計規約は migration 00124 の apply_weekly_rebase_event 内 SQL と一致させる必要があるため、
 * open/close は「週内最初/最後の営業日の値（null もそのまま）」、high/low/sum は「null 無視・
 * 全 null なら null」、adjustment_factor は「coalesce(factor,1) の積」を固定する。
 */

import { describe, it, expect } from 'vitest';

import {
  isoWeekStart,
  aggregateWeeklyBars,
  type WeeklyBarSourceRow,
} from '@/lib/analytics/weekly-bars';

/** 日足行のファクトリ（既定は factor=1 の平穏な1日） */
function bar(overrides: Partial<WeeklyBarSourceRow> & { trade_date: string }): WeeklyBarSourceRow {
  return {
    local_code: '72030',
    session: 'DAY',
    open: 100,
    high: 110,
    low: 90,
    close: 105,
    volume: 1000,
    turnover_value: 105000,
    adjustment_factor: 1,
    adj_open: 100,
    adj_high: 110,
    adj_low: 90,
    adj_close: 105,
    adj_volume: 1000,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// isoWeekStart
// ---------------------------------------------------------------------------

describe('isoWeekStart', () => {
  it('平日はその週の月曜を返す', () => {
    expect(isoWeekStart('2026-08-20')).toBe('2026-08-17'); // 木 → 月
    expect(isoWeekStart('2026-08-17')).toBe('2026-08-17'); // 月 → 当日
    expect(isoWeekStart('2026-08-21')).toBe('2026-08-17'); // 金
  });

  it('日曜は前の月曜（ISO週は月曜起点）', () => {
    expect(isoWeekStart('2026-08-23')).toBe('2026-08-17');
  });

  it('年またぎのISO週でも月曜を返す', () => {
    // 2025-12-29(月)〜2026-01-04(日) は同一ISO週
    expect(isoWeekStart('2025-12-31')).toBe('2025-12-29');
    expect(isoWeekStart('2026-01-01')).toBe('2025-12-29');
    expect(isoWeekStart('2026-01-04')).toBe('2025-12-29');
    expect(isoWeekStart('2026-01-05')).toBe('2026-01-05');
  });

  it('うるう日を含む週も正しく扱う', () => {
    expect(isoWeekStart('2024-02-29')).toBe('2024-02-26');
    expect(isoWeekStart('2024-03-01')).toBe('2024-02-26');
  });

  it('ローカルタイムゾーンに依存しない（TZ=UTC 以外でも同じ結果）', () => {
    const original = process.env.TZ;
    try {
      process.env.TZ = 'Pacific/Kiritimati'; // UTC+14
      expect(isoWeekStart('2026-08-17')).toBe('2026-08-17');
      process.env.TZ = 'Pacific/Midway'; // UTC-11
      expect(isoWeekStart('2026-08-17')).toBe('2026-08-17');
    } finally {
      process.env.TZ = original;
    }
  });

  it('形式不正・実在しない日付は throw', () => {
    expect(() => isoWeekStart('2026/08/20')).toThrow('YYYY-MM-DD');
    expect(() => isoWeekStart('2026-02-30')).toThrow('実在しない日付');
  });
});

// ---------------------------------------------------------------------------
// aggregateWeeklyBars
// ---------------------------------------------------------------------------

describe('aggregateWeeklyBars', () => {
  it('空入力は空配列', () => {
    expect(aggregateWeeklyBars([])).toEqual([]);
  });

  it('1週間の日足を week_start（月曜）キーの1行に集計する', () => {
    const bars = [
      bar({ trade_date: '2026-08-17', open: 100, high: 120, low: 95, close: 110, volume: 1000, turnover_value: 110000, adj_open: 100, adj_high: 120, adj_low: 95, adj_close: 110, adj_volume: 1000 }),
      bar({ trade_date: '2026-08-18', open: 110, high: 130, low: 105, close: 125, volume: 2000, turnover_value: 250000, adj_open: 110, adj_high: 130, adj_low: 105, adj_close: 125, adj_volume: 2000 }),
      bar({ trade_date: '2026-08-21', open: 125, high: 128, low: 90, close: 92, volume: 3000, turnover_value: 276000, adj_open: 125, adj_high: 128, adj_low: 90, adj_close: 92, adj_volume: 3000 }),
    ];

    expect(aggregateWeeklyBars(bars)).toEqual([
      {
        local_code: '72030',
        week_start: '2026-08-17',
        week_end: '2026-08-21',
        open: 100,
        high: 130,
        low: 90,
        close: 92,
        volume: 6000,
        turnover_value: 636000,
        adjustment_factor: 1,
        adj_open: 100,
        adj_high: 130,
        adj_low: 90,
        adj_close: 92,
        adj_volume: 6000,
      },
    ]);
  });

  it('入力順が不同でも同じ結果になる（内部でソート）', () => {
    const bars = [
      bar({ trade_date: '2026-08-21', open: 125, close: 92 }),
      bar({ trade_date: '2026-08-17', open: 100, close: 110 }),
      bar({ trade_date: '2026-08-18', open: 110, close: 125 }),
    ];

    const [week] = aggregateWeeklyBars(bars);
    expect(week.open).toBe(100); // 月曜の始値
    expect(week.close).toBe(92); // 金曜の終値
    expect(week.week_end).toBe('2026-08-21');
  });

  it('年またぎのISO週は1行にまとまり、翌週から別行になる', () => {
    const bars = [
      bar({ trade_date: '2025-12-30', close: 200 }),
      bar({ trade_date: '2026-01-05', close: 210 }),
      bar({ trade_date: '2026-01-06', close: 220 }),
    ];

    const weeks = aggregateWeeklyBars(bars);
    expect(weeks.map((w) => [w.week_start, w.week_end, w.close])).toEqual([
      ['2025-12-29', '2025-12-30', 200],
      ['2026-01-05', '2026-01-06', 220],
    ]);
  });

  it('祝日で営業日が欠けても week_start は月曜、week_end は週内最終営業日', () => {
    // 2026-08-17(月) が休場、実データは火〜木のみ
    const bars = [
      bar({ trade_date: '2026-08-18', open: 111 }),
      bar({ trade_date: '2026-08-19' }),
      bar({ trade_date: '2026-08-20', close: 130 }),
    ];

    const [week] = aggregateWeeklyBars(bars);
    expect(week.week_start).toBe('2026-08-17');
    expect(week.week_end).toBe('2026-08-20');
    expect(week.open).toBe(111);
    expect(week.close).toBe(130);
  });

  it('週初値・週末値は null でもそのまま採用する（SQL の array_agg[1] と同じ）', () => {
    const bars = [
      bar({ trade_date: '2026-08-17', open: null, adj_open: null, high: 120, low: 95, close: 110 }),
      bar({ trade_date: '2026-08-18', open: 110, close: null, adj_close: null, high: 130, low: 105 }),
    ];

    const [week] = aggregateWeeklyBars(bars);
    expect(week.open).toBeNull();
    expect(week.adj_open).toBeNull();
    expect(week.close).toBeNull();
    expect(week.adj_close).toBeNull();
    // high/low は null を無視した極値
    expect(week.high).toBe(130);
    expect(week.low).toBe(95);
  });

  it('high/low/sum は null を無視し、全て null なら null', () => {
    const bars = [
      bar({ trade_date: '2026-08-17', high: null, low: null, volume: null, turnover_value: 100, adj_high: null, adj_low: null, adj_volume: null }),
      bar({ trade_date: '2026-08-18', high: null, low: null, volume: 500, turnover_value: null, adj_high: 140, adj_low: 80, adj_volume: null }),
    ];

    const [week] = aggregateWeeklyBars(bars);
    expect(week.high).toBeNull();
    expect(week.low).toBeNull();
    expect(week.volume).toBe(500); // null 無視の合計
    expect(week.turnover_value).toBe(100);
    expect(week.adj_high).toBe(140);
    expect(week.adj_low).toBe(80);
    expect(week.adj_volume).toBeNull(); // 全 null
  });

  it('adjustment_factor は週内の積、null は 1 として扱う', () => {
    const bars = [
      bar({ trade_date: '2026-08-17', adjustment_factor: 1 }),
      bar({ trade_date: '2026-08-18', adjustment_factor: 0.5 }), // 分割
      bar({ trade_date: '2026-08-19', adjustment_factor: null }),
      bar({ trade_date: '2026-08-20', adjustment_factor: 0.2 }),
    ];

    expect(aggregateWeeklyBars(bars)[0].adjustment_factor).toBeCloseTo(0.1, 12);
  });

  it('factor が無い週は 1（NOT NULL 制約を満たす）', () => {
    const bars = [{ trade_date: '2026-08-17', local_code: '72030', close: 100 }];
    expect(aggregateWeeklyBars(bars)[0].adjustment_factor).toBe(1);
  });

  it('numeric が文字列で来ても数値化する', () => {
    const bars = [
      bar({ trade_date: '2026-08-17', open: '100.5', high: '120.25', low: '95', close: '110', volume: '1000', turnover_value: '110000', adjustment_factor: '0.5', adj_volume: '2000' }),
      bar({ trade_date: '2026-08-18', high: '130', volume: '500', adj_volume: '1000' }),
    ];

    const [week] = aggregateWeeklyBars(bars);
    expect(week.open).toBe(100.5);
    expect(week.high).toBe(130);
    expect(week.volume).toBe(1500);
    expect(week.adjustment_factor).toBe(0.5);
    expect(week.adj_volume).toBe(3000);
  });

  it('DAY 以外のセッション行は集計対象外', () => {
    const bars = [
      bar({ trade_date: '2026-08-17', volume: 1000 }),
      bar({ trade_date: '2026-08-17', session: 'AM', volume: 400 }),
      bar({ trade_date: '2026-08-17', session: 'PM', volume: 600 }),
    ];

    const weeks = aggregateWeeklyBars(bars);
    expect(weeks).toHaveLength(1);
    expect(weeks[0].volume).toBe(1000);
  });

  it('session 未指定の行は DAY 扱い（DBの生行・API変換後の両対応）', () => {
    const bars = [{ trade_date: '2026-08-17', local_code: '72030', close: 100, volume: 10 }];
    expect(aggregateWeeklyBars(bars)[0].volume).toBe(10);
  });

  it('DAY 以外しか無い入力は空配列', () => {
    expect(aggregateWeeklyBars([bar({ trade_date: '2026-08-17', session: 'AM' })])).toEqual([]);
  });

  it('複数銘柄が混在した入力は throw', () => {
    const bars = [
      bar({ trade_date: '2026-08-17' }),
      bar({ trade_date: '2026-08-18', local_code: '285A0' }),
    ];
    expect(() => aggregateWeeklyBars(bars)).toThrow('単一銘柄');
  });

  it('複数週は week_start 昇順で返る', () => {
    const bars = [
      bar({ trade_date: '2026-08-24' }),
      bar({ trade_date: '2026-08-10' }),
      bar({ trade_date: '2026-08-17' }),
    ];
    expect(aggregateWeeklyBars(bars).map((w) => w.week_start)).toEqual([
      '2026-08-10',
      '2026-08-17',
      '2026-08-24',
    ]);
  });

  it('週の途中まで（暫定値）と週末確定で同じ week_start に集計される（冪等 upsert 前提）', () => {
    const partial = aggregateWeeklyBars([
      bar({ trade_date: '2026-08-17', close: 110 }),
      bar({ trade_date: '2026-08-18', close: 115 }),
    ]);
    const full = aggregateWeeklyBars([
      bar({ trade_date: '2026-08-17', close: 110 }),
      bar({ trade_date: '2026-08-18', close: 115 }),
      bar({ trade_date: '2026-08-21', close: 125 }),
    ]);

    expect(partial[0].week_start).toBe(full[0].week_start);
    expect(partial[0].week_end).toBe('2026-08-18');
    expect(full[0].week_end).toBe('2026-08-21');
    expect(full[0].close).toBe(125);
  });
});
