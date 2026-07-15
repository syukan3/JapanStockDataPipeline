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
  planYahooUpdates,
  emptyRow,
  type IndicatorRow,
} from '@/lib/market/indicators-sync';
import type { DailyBar } from '@/lib/market/yahoo-chart-client';

function rowWith(date: string, patch: Partial<IndicatorRow>): IndicatorRow {
  return { ...emptyRow(date), ...patch };
}

function src(
  date: string,
  patch: Partial<{
    nikkeiClose: number | null;
    per: number | null;
    nikkeiVi: number | null;
    shortSellingRestricted: number | null;
    shortSellingUnrestricted: number | null;
  }> = {}
) {
  return {
    date,
    nikkeiClose: 40000,
    per: 18.0,
    nikkeiVi: 22.5,
    shortSellingRestricted: 30.0,
    shortSellingUnrestricted: 9.0,
    ...patch,
  };
}

describe('planDaily2Updates', () => {
  it('NULL列のみ更新対象にする（既存非NULL列は payload に含めない）', () => {
    const rowMap = new Map([
      ['2026-07-01', rowWith('2026-07-01', { nikkei_close: 40000, nikkei_per: 18.5 })],
    ]);
    const plan = planDaily2Updates(['2026-07-01'], rowMap, [src('2026-07-01')]);
    expect(plan.perRows).toEqual([]); // 既にPERあり → 触らない
    expect(plan.viRows).toEqual([
      { as_of_date: '2026-07-01', nikkei_vi: 22.5 },
    ]);
    expect(plan.ssRestrictedRows).toEqual([
      { as_of_date: '2026-07-01', short_selling_ratio_restricted: 30.0 },
    ]);
    expect(plan.ssUnrestrictedRows).toEqual([
      { as_of_date: '2026-07-01', short_selling_ratio_unrestricted: 9.0 },
    ]);
  });

  it('空売り比率成分は片方だけ値域外null化されても他方は更新する', () => {
    const rowMap = new Map<string, IndicatorRow>();
    // 規制なし成分がソース側でnull（値域外null化を模擬）→ 規制あり側だけ更新
    const plan = planDaily2Updates(
      ['2026-07-01'],
      rowMap,
      [src('2026-07-01', { shortSellingUnrestricted: null })]
    );
    expect(plan.ssRestrictedRows).toHaveLength(1);
    expect(plan.ssUnrestrictedRows).toEqual([]);
  });

  it('ソース側がnullの列は更新しない（既存値をnullで上書きしない）', () => {
    const rowMap = new Map([
      ['2026-07-01', rowWith('2026-07-01', { nikkei_close: 40000, nikkei_per: 18.5 })],
    ]);
    // PERが値域外でnull化されたソース: VIだけ更新され、既存PERは無傷
    const plan = planDaily2Updates(['2026-07-01'], rowMap, [src('2026-07-01', { per: null })]);
    expect(plan.perRows).toEqual([]);
    expect(plan.viRows).toHaveLength(1);
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
    expect(plan.viRows).toEqual([]);
    expect(plan.ssRestrictedRows).toEqual([]);
    expect(plan.ssUnrestrictedRows).toEqual([]);
  });

  it('Yahoo欠損日はdaily2終値でフォールバック', () => {
    const rowMap = new Map<string, IndicatorRow>();
    const plan = planDaily2Updates(['2026-07-01'], rowMap, [src('2026-07-01')]);
    expect(plan.closeRows).toEqual([{ as_of_date: '2026-07-01', nikkei_close: 40000 }]);
    expect(plan.perRows).toHaveLength(1);
    expect(plan.viRows).toHaveLength(1);
    expect(plan.ssRestrictedRows).toHaveLength(1);
    expect(plan.ssUnrestrictedRows).toHaveLength(1);
  });

  it('ソースに日付が無い場合は noSource としてカウント', () => {
    const rowMap = new Map<string, IndicatorRow>();
    const plan = planDaily2Updates(['2026-07-01'], rowMap, []);
    expect(plan.noSource).toBe(1);
  });
});

describe('planYahooUpdates', () => {
  function bar(date: string, patch: Partial<DailyBar> = {}): DailyBar {
    return { date, close: 40000, open: 39800, high: 40100, low: 39700, ...patch };
  }

  it('行が無い日は close/open/high/low の全列を更新対象にする', () => {
    const plan = planYahooUpdates(['2026-07-01'], new Map(), [bar('2026-07-01')]);
    expect(plan.closeRows).toEqual([{ as_of_date: '2026-07-01', nikkei_close: 40000 }]);
    expect(plan.openRows).toEqual([{ as_of_date: '2026-07-01', nikkei_open: 39800 }]);
    expect(plan.highRows).toEqual([{ as_of_date: '2026-07-01', nikkei_high: 40100 }]);
    expect(plan.lowRows).toEqual([{ as_of_date: '2026-07-01', nikkei_low: 39700 }]);
    expect(plan.missing).toEqual([]);
  });

  it('NULL列のみ更新対象にする（既存 close は payload に含めない = OHLC後埋め）', () => {
    const rowMap = new Map([
      ['2026-07-01', rowWith('2026-07-01', { nikkei_close: 40000 })],
    ]);
    const plan = planYahooUpdates(['2026-07-01'], rowMap, [bar('2026-07-01')]);
    expect(plan.closeRows).toEqual([]); // 既に終値あり → 触らない
    expect(plan.openRows).toHaveLength(1);
    expect(plan.highRows).toHaveLength(1);
    expect(plan.lowRows).toHaveLength(1);
  });

  it('ソースの OHLC null穴は列単位でスキップ（既存値をnullで上書きしない）', () => {
    const rowMap = new Map([
      ['2026-07-01', rowWith('2026-07-01', { nikkei_high: 40100 })],
    ]);
    // ソースは open のみ null 穴、high はソースにあるが保存済み → open/high とも payload 無し
    const plan = planYahooUpdates(['2026-07-01'], rowMap, [
      bar('2026-07-01', { open: null }),
    ]);
    expect(plan.openRows).toEqual([]);
    expect(plan.highRows).toEqual([]);
    expect(plan.lowRows).toEqual([{ as_of_date: '2026-07-01', nikkei_low: 39700 }]);
    expect(plan.closeRows).toHaveLength(1);
    expect(rowMap.get('2026-07-01')!.nikkei_high).toBe(40100);
  });

  it('ソースに日付が無い日は missing としてカウント', () => {
    const plan = planYahooUpdates(['2026-07-01', '2026-07-02'], new Map(), [bar('2026-07-02')]);
    expect(plan.missing).toEqual(['2026-07-01']);
    expect(plan.closeRows).toEqual([{ as_of_date: '2026-07-02', nikkei_close: 40000 }]);
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
