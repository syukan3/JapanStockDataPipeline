/**
 * 週足の集計規約が「TS の aggregateWeeklyBars」と「00124 の RPC 内 SQL 集計」で一致することを固定する。
 *
 * 分割イベント週だけは SQL 側（apply_weekly_rebase_event）が再基準化済み日足から再集計して
 * 行ごと置換するため、両者の規約がずれると同じ週の値が「どちらの経路で書かれたか」で変わる。
 * ここでは SQL 側の各集計句を明示的に固定し、同じ入力に対する TS 側の出力と突き合わせる。
 */

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

import { aggregateWeeklyBars, type WeeklyBarSourceRow } from '@/lib/analytics/weekly-bars';
import { WEEKLY_BARS_ON_CONFLICT } from '@/lib/analytics/weekly-bars-backfill';

const migration = readFileSync(
  resolve(process.cwd(), 'supabase/migrations/00124_create_equity_bar_weekly.sql'),
  'utf8'
);

describe('00124 の週足集計規約と aggregateWeeklyBars の一致', () => {
  it('upsert キーはテーブルの主キー (local_code, week_start) と一致する', () => {
    expect(migration).toContain('primary key (local_code, week_start)');
    expect(migration).toContain('on conflict (local_code, week_start) do update');
    expect(WEEKLY_BARS_ON_CONFLICT).toBe('local_code,week_start');
  });

  it('SQL は DAY セッションの日足のみを1週間ぶん集計する', () => {
    expect(migration).toContain("d.session = 'DAY'");
    expect(migration).toContain('d.trade_date >= v_week_start');
    expect(migration).toContain('d.trade_date < v_week_start + 7');
    // week_start は ISO週の月曜（date_trunc('week') は月曜起点）
    expect(migration).toContain("date_trunc('week', p_event_date)::date");
  });

  it('open/close は週内最初・最後の営業日の値（null もそのまま採用）', () => {
    expect(migration).toMatch(/array_agg\(d\.open\s+order by d\.trade_date asc\)\)\[1\]/);
    expect(migration).toMatch(/array_agg\(d\.close order by d\.trade_date desc\)\)\[1\]/);
    expect(migration).toMatch(/array_agg\(d\.adj_open\s+order by d\.trade_date asc\)\)\[1\]/);
    expect(migration).toMatch(/array_agg\(d\.adj_close order by d\.trade_date desc\)\)\[1\]/);
  });

  it('high/low は max/min、volume 系は sum、factor は週内の積', () => {
    for (const clause of [
      'max(d.high)',
      'min(d.low)',
      'max(d.adj_high)',
      'min(d.adj_low)',
      'sum(d.volume)',
      'sum(d.turnover_value)',
      'sum(d.adj_volume)',
      'max(d.trade_date)', // week_end
      'jquants_core.numeric_product(coalesce(d.adjustment_factor, 1))',
    ]) {
      expect(migration).toContain(clause);
    }
  });

  it('同じ日足入力に対する TS 側の出力が SQL の各集計句どおりになる', () => {
    // 2026-08-17(月)〜2026-08-21(金)。火曜が休場（欠損）、水曜に分割（factor=0.5）、
    // 木曜は high/low 欠損、金曜は volume 欠損という混在ケース。
    const bars: WeeklyBarSourceRow[] = [
      { trade_date: '2026-08-17', local_code: '72030', session: 'DAY', open: 100, high: 120, low: 95, close: 110, volume: 1000, turnover_value: 110000, adjustment_factor: 1, adj_open: 50, adj_high: 60, adj_low: 47.5, adj_close: 55, adj_volume: 2000 },
      { trade_date: '2026-08-19', local_code: '72030', session: 'DAY', open: 112, high: 130, low: 90, close: 60, volume: 2000, turnover_value: 120000, adjustment_factor: 0.5, adj_open: 56, adj_high: 65, adj_low: 45, adj_close: 60, adj_volume: 2000 },
      { trade_date: '2026-08-20', local_code: '72030', session: 'DAY', open: 60, high: null, low: null, close: 62, volume: 500, turnover_value: 31000, adjustment_factor: 1, adj_open: 60, adj_high: null, adj_low: null, adj_close: 62, adj_volume: 500 },
      { trade_date: '2026-08-21', local_code: '72030', session: 'DAY', open: 62, high: 70, low: 61, close: 68, volume: null, turnover_value: null, adjustment_factor: 1, adj_open: 62, adj_high: 70, adj_low: 61, adj_close: 68, adj_volume: null },
    ];

    const [week] = aggregateWeeklyBars(bars);

    expect(week).toEqual({
      local_code: '72030',
      week_start: '2026-08-17', // date_trunc('week', ...)
      week_end: '2026-08-21', // max(trade_date)
      open: 100, // array_agg(open order by trade_date asc)[1]
      high: 130, // max(high)（null 行は無視）
      low: 61, // min(low)
      close: 68, // array_agg(close order by trade_date desc)[1]
      volume: 3500, // sum(volume)（null 行は無視）
      turnover_value: 261000, // sum(turnover_value)
      adjustment_factor: 0.5, // numeric_product(coalesce(factor, 1))
      adj_open: 50,
      adj_high: 70,
      adj_low: 45,
      adj_close: 68,
      adj_volume: 4500,
    });
  });
});
