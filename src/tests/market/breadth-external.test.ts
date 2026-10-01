/**
 * indicators-sync.ts の planBreadthExternalUpdates（J-Quants OFF 中の breadth 代替）のテスト
 */

import { describe, it, expect } from 'vitest';
import { planBreadthExternalUpdates, emptyRow, type IndicatorRow } from '@/lib/market/indicators-sync';

const src = (date: string, adr: number | null, hi: number | null, lo: number | null) => ({
  date,
  refAdvDecRatio: adr,
  refNewHighs: hi,
  refNewLows: lo,
});

describe('planBreadthExternalUpdates', () => {
  it('3列とも NULL の日だけを nikkei225jp の値で埋め、出所を残す', () => {
    const rowMap = new Map<string, IndicatorRow>([
      ['2026-10-01', emptyRow('2026-10-01')],
      ['2026-09-30', { ...emptyRow('2026-09-30'), adv_dec_ratio_25d: 100.9, new_highs: 27, new_lows: 33 }],
    ]);
    const plan = planBreadthExternalUpdates(['2026-09-30', '2026-10-01'], rowMap, [
      src('2026-09-30', 101.11, 23, 35),
      src('2026-10-01', 103.2, 40, 12),
    ]);
    expect(plan.rows).toEqual([
      { as_of_date: '2026-10-01', adv_dec_ratio_25d: 103.2, new_highs: 40, new_lows: 12, breadth_source: 'nikkei225jp' },
    ]);
  });

  it('自前計算の値が1列でもあれば触らない（部分的な出所の混在を作らない）', () => {
    const rowMap = new Map<string, IndicatorRow>([['2026-10-01', { ...emptyRow('2026-10-01'), new_highs: 5 }]]);
    expect(planBreadthExternalUpdates(['2026-10-01'], rowMap, [src('2026-10-01', 103, 40, 12)]).rows).toEqual([]);
  });

  it('ソースの列が欠けている・値域外の日は書かない', () => {
    const plan = planBreadthExternalUpdates(['2026-10-01', '2026-10-02', '2026-10-05'], new Map(), [
      src('2026-10-01', null, 40, 12),
      src('2026-10-02', 1000, 40, 12),
    ]);
    expect(plan.rows).toEqual([]);
    expect(plan.noSource).toBe(2); // 10-01 は列欠け、10-05 はソース行なし
    expect(plan.outOfRange).toBe(1);
  });
});
