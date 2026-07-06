/**
 * market/indicators-sync.ts の純関数部分のユニットテスト
 *
 * - planDaily2Updates: 部分upsertの列保護（既存非NULL列へnullを送らない）
 * - planRatioUpserts: 正準営業日軸での25日窓＋欠損修復の伝播再計算
 */

import { describe, it, expect, vi } from 'vitest';

vi.mock('@/lib/utils/logger', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
}));

import {
  planDaily2Updates,
  planRatioUpserts,
  emptyRow,
  type IndicatorRow,
} from '@/lib/market/indicators-sync';

function rowWith(date: string, patch: Partial<IndicatorRow>): IndicatorRow {
  return { ...emptyRow(date), ...patch };
}

function src(date: string, patch: Partial<{ nikkeiClose: number | null; per: number | null; shortSellingRatio: number | null }> = {}) {
  return { date, nikkeiClose: 40000, per: 18.0, shortSellingRatio: 40.0, ...patch };
}

describe('planDaily2Updates', () => {
  it('NULL列のみ更新対象にする（既存非NULL列は payload に含めない）', () => {
    const rowMap = new Map([
      ['2026-07-01', rowWith('2026-07-01', { nikkei_close: 40000, nikkei_per: 18.5 })],
    ]);
    const plan = planDaily2Updates(['2026-07-01'], rowMap, [src('2026-07-01')]);
    expect(plan.perRows).toEqual([]); // 既にPERあり → 触らない
    expect(plan.shortRows).toEqual([
      { as_of_date: '2026-07-01', short_selling_ratio: 40.0 },
    ]);
  });

  it('ソース側がnullの列は更新しない（既存値をnullで上書きしない）', () => {
    const rowMap = new Map([
      ['2026-07-01', rowWith('2026-07-01', { nikkei_close: 40000, nikkei_per: 18.5 })],
    ]);
    // PERが値域外でnull化されたソース: shortだけ更新され、既存PERは無傷
    const plan = planDaily2Updates(['2026-07-01'], rowMap, [src('2026-07-01', { per: null })]);
    expect(plan.perRows).toEqual([]);
    expect(plan.shortRows).toHaveLength(1);
    expect(rowMap.get('2026-07-01')!.nikkei_per).toBe(18.5);
  });

  it('終値がYahoo保存値と乖離したらそのソース由来の全列をスキップ', () => {
    const rowMap = new Map([
      ['2026-07-01', rowWith('2026-07-01', { nikkei_close: 40000 })],
    ]);
    const plan = planDaily2Updates(['2026-07-01'], rowMap, [
      src('2026-07-01', { nikkeiClose: 41000 }), // 2.5% 乖離
    ]);
    expect(plan.closeMismatch).toBe(1);
    expect(plan.perRows).toEqual([]);
    expect(plan.shortRows).toEqual([]);
  });

  it('Yahoo欠損日はdaily2終値でフォールバック', () => {
    const rowMap = new Map<string, IndicatorRow>();
    const plan = planDaily2Updates(['2026-07-01'], rowMap, [src('2026-07-01')]);
    expect(plan.closeRows).toEqual([{ as_of_date: '2026-07-01', nikkei_close: 40000 }]);
    expect(plan.perRows).toHaveLength(1);
    expect(plan.shortRows).toHaveLength(1);
  });

  it('ソースに日付が無い場合は noSource としてカウント', () => {
    const rowMap = new Map<string, IndicatorRow>();
    const plan = planDaily2Updates(['2026-07-01'], rowMap, []);
    expect(plan.noSource).toBe(1);
  });
});

describe('planRatioUpserts', () => {
  /** 連続する営業日軸を生成（D001..D0NN 形式の擬似日付・辞書順=時系列順） */
  function days(n: number): string[] {
    return Array.from({ length: n }, (_, i) => `D${String(i + 1).padStart(3, '0')}`);
  }
  function mapWithCounts(dates: string[], adv = 100, dec = 50): Map<string, IndicatorRow> {
    return new Map(dates.map((d) => [d, rowWith(d, { advancers: adv, decliners: dec })]));
  }

  it('25営業日窓が揃った日の ratio を計算する', () => {
    const axis = days(26);
    const rowMap = mapWithCounts(axis);
    const out = planRatioUpserts(axis, rowMap);
    // i=24（25日目）と i=25（26日目）が計算対象
    expect(out).toHaveLength(2);
    expect(out[0]).toEqual({ as_of_date: 'D025', adv_dec_ratio_25d: 200 });
  });

  it('窓内に行が欠けた営業日があると保存しない（非連続窓の禁止）', () => {
    const axis = days(25);
    const rowMap = mapWithCounts(axis);
    rowMap.delete('D010'); // 営業日D010の行が無い（未同期）
    const out = planRatioUpserts(axis, rowMap);
    expect(out).toEqual([]);
  });

  it('既に ratio がある日はスキップする', () => {
    const axis = days(25);
    const rowMap = mapWithCounts(axis);
    rowMap.get('D025')!.adv_dec_ratio_25d = 123.45;
    expect(planRatioUpserts(axis, rowMap)).toEqual([]);
  });

  it('recomputeAfter の日付を窓に含む日は既存 ratio があっても再計算する', () => {
    const axis = days(30);
    const rowMap = mapWithCounts(axis);
    // 全日に古い ratio が入っている（値が変わる状況: D005 の騰落数を埋め直した）
    for (const d of axis) rowMap.get(d)!.adv_dec_ratio_25d = 999;
    rowMap.get('D005')!.advancers = 200;
    const out = planRatioUpserts(axis, rowMap, new Set(['D005']));
    // D005 を窓に含むのは D025〜D029（D030の窓は D006〜D030）
    expect(out.map((o) => o.as_of_date)).toEqual(['D025', 'D026', 'D027', 'D028', 'D029']);
    expect(out[0].adv_dec_ratio_25d).toBeCloseTo(((100 * 24 + 200) / (50 * 25)) * 100, 1);
  });

  it('再計算しても値が同じ場合は upsert しない', () => {
    const axis = days(25);
    const rowMap = mapWithCounts(axis);
    rowMap.get('D025')!.adv_dec_ratio_25d = 200;
    expect(planRatioUpserts(axis, rowMap, new Set(['D001']))).toEqual([]);
  });
});
