/**
 * market/indicators-sync.ts の純関数部分のユニットテスト
 *
 * - planDaily2Updates: 部分upsertの列保護（既存非NULL列へnullを送らない）
 * - planRatioUpserts: 正準営業日軸での25日窓＋欠損修復の伝播再計算
 */

import { describe, it, expect, vi } from 'vitest';

const { mockOfficial, mockYahooBars } = vi.hoisted(() => ({
  mockOfficial: vi.fn(),
  mockYahooBars: vi.fn(),
}));
vi.mock('@/lib/market/nikkei-official-client', () => ({
  fetchNikkeiOfficialDaily: mockOfficial,
  NIKKEI_OFFICIAL_CSV_FROM: '2023-01-04',
}));
vi.mock('@/lib/market/yahoo-chart-client', () => ({
  BROWSER_USER_AGENT: 'test-ua',
  fetchNikkeiDailyBars: mockYahooBars,
}));

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
  isOhlcPending,
  fillYahoo,
  emptyRow,
  aggregateShortSellingByDate,
  planShortSellingOfficial,
  type IndicatorRow,
  type ShortSellingSectorRow,
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

describe('planDaily2Updates: skipShortSelling ゲート（公式データ切替）', () => {
  it('デフォルト（未指定）は空売り2成分を従来どおり書き込む', () => {
    const plan = planDaily2Updates(['2026-07-01'], new Map(), [src('2026-07-01')]);
    expect(plan.ssRestrictedRows).toHaveLength(1);
    expect(plan.ssUnrestrictedRows).toHaveLength(1);
  });

  it('skipShortSelling=true は空売り2成分を一切書かない（per/vi/close は従来どおり）', () => {
    const plan = planDaily2Updates(['2026-07-01'], new Map(), [src('2026-07-01')], {
      skipShortSelling: true,
    });
    expect(plan.ssRestrictedRows).toEqual([]);
    expect(plan.ssUnrestrictedRows).toEqual([]);
    expect(plan.perRows).toHaveLength(1);
    expect(plan.viRows).toHaveLength(1);
    expect(plan.closeRows).toHaveLength(1);
  });

  it('空売りソース欠損日: 非skipは noSource=1 / skip では 0（空売り欠損で失敗扱いしない）', () => {
    const makeMap = () =>
      new Map([
        [
          '2026-07-01',
          rowWith('2026-07-01', { nikkei_close: 40000, nikkei_per: 18, nikkei_vi: 22 }),
        ],
      ]);
    const noSs = src('2026-07-01', {
      shortSellingRestricted: null,
      shortSellingUnrestricted: null,
    });
    expect(planDaily2Updates(['2026-07-01'], makeMap(), [noSs]).noSource).toBe(1);
    expect(
      planDaily2Updates(['2026-07-01'], makeMap(), [noSs], { skipShortSelling: true }).noSource
    ).toBe(0);
  });
});

describe('aggregateShortSellingByDate', () => {
  function sector(
    date: string,
    sell: number | null,
    withRes: number | null,
    noRes: number | null
  ): ShortSellingSectorRow {
    return {
      as_of_date: date,
      selling_ex_short_value: sell,
      short_with_restrictions_value: withRes,
      short_without_restrictions_value: noRes,
    };
  }

  it('全業種を合算し分母（3列合計）で規制あり/なし比率[%]を算出する', () => {
    // sell=1000, with=400, no=100 → denom=1500 → restricted=26.67, unrestricted=6.67
    const agg = aggregateShortSellingByDate([
      sector('2026-07-15', 600, 300, 100),
      sector('2026-07-15', 400, 100, 0),
    ]);
    expect(agg.get('2026-07-15')).toEqual({ restricted: 26.67, unrestricted: 6.67 });
  });

  it('null金額は0として扱う', () => {
    // sell=600, with=300, no=100 → denom=1000 → restricted=30, unrestricted=10
    const agg = aggregateShortSellingByDate([
      sector('2026-07-15', null, 300, 100),
      sector('2026-07-15', 600, null, null),
    ]);
    expect(agg.get('2026-07-15')).toEqual({ restricted: 30, unrestricted: 10 });
  });

  it('分母0以下の日は比率算出不能として含めない', () => {
    const agg = aggregateShortSellingByDate([sector('2026-07-15', 0, 0, 0)]);
    expect(agg.has('2026-07-15')).toBe(false);
  });

  it('日付ごとに独立して集計する', () => {
    const agg = aggregateShortSellingByDate([
      sector('2026-07-14', 800, 200, 0),
      sector('2026-07-15', 900, 0, 100),
    ]);
    expect(agg.get('2026-07-14')).toEqual({ restricted: 20, unrestricted: 0 });
    expect(agg.get('2026-07-15')).toEqual({ restricted: 0, unrestricted: 10 });
  });
});

describe('planShortSellingOfficial', () => {
  const agg = new Map([['2026-07-15', { restricted: 30, unrestricted: 10 }]]);

  it('fill-null: 2成分がNULLの日を両列同時に書く', () => {
    expect(planShortSellingOfficial(['2026-07-15'], new Map(), agg, false)).toEqual([
      {
        as_of_date: '2026-07-15',
        short_selling_ratio_restricted: 30,
        short_selling_ratio_unrestricted: 10,
      },
    ]);
  });

  it('fill-null: 2成分が既に非NULLの日はスキップ', () => {
    const rowMap = new Map([
      [
        '2026-07-15',
        rowWith('2026-07-15', {
          short_selling_ratio_restricted: 28,
          short_selling_ratio_unrestricted: 8,
        }),
      ],
    ]);
    expect(planShortSellingOfficial(['2026-07-15'], rowMap, agg, false)).toEqual([]);
  });

  it('fill-null: 片方だけNULLの半端な日は両列を書き直す', () => {
    const rowMap = new Map([
      ['2026-07-15', rowWith('2026-07-15', { short_selling_ratio_restricted: 28 })],
    ]);
    expect(planShortSellingOfficial(['2026-07-15'], rowMap, agg, false)).toHaveLength(1);
  });

  it('overwrite=true: 既存の非NULL値も公式値で上書きする', () => {
    const rowMap = new Map([
      [
        '2026-07-15',
        rowWith('2026-07-15', {
          short_selling_ratio_restricted: 99,
          short_selling_ratio_unrestricted: 99,
        }),
      ],
    ]);
    expect(planShortSellingOfficial(['2026-07-15'], rowMap, agg, true)).toEqual([
      {
        as_of_date: '2026-07-15',
        short_selling_ratio_restricted: 30,
        short_selling_ratio_unrestricted: 10,
      },
    ]);
  });

  it('集計値の無い日はスキップ（overwriteでも書かない）', () => {
    expect(planShortSellingOfficial(['2026-07-16'], new Map(), agg, true)).toEqual([]);
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

describe('isOhlcPending', () => {
  it('公式CSV収録開始(2023-01-04)より前はOHLC nullでもpending扱いしない', () => {
    const row = rowWith('2022-06-01', { nikkei_close: 27000 });
    expect(isOhlcPending('2022-06-01', row)).toBe(false);
  });

  it('収録開始以降はOHLCいずれかがnullならpending', () => {
    const row = rowWith('2023-01-04', { nikkei_close: 25716.86, nikkei_open: 25834.93 });
    expect(isOhlcPending('2023-01-04', row)).toBe(true);
  });

  it('close未取得は期間を問わずpending / OHLC全て揃えば非pending', () => {
    expect(isOhlcPending('2022-06-01', rowWith('2022-06-01', {}))).toBe(true);
    expect(isOhlcPending('2022-06-01', undefined)).toBe(true);
    const full = rowWith('2023-01-04', {
      nikkei_close: 25716.86,
      nikkei_open: 25834.93,
      nikkei_high: 25840.68,
      nikkei_low: 25661.89,
    });
    expect(isOhlcPending('2023-01-04', full)).toBe(false);
  });
});

describe('fillYahoo: 公式CSV一次+Yahooフォールバック', () => {
  it('公式CSVが成功したら source=official でYahooを呼ばない', async () => {
    mockOfficial.mockResolvedValueOnce([
      { date: '2026-07-15', close: 68751.51, open: 68000.1, high: 68800.2, low: 67900.3 },
      { date: '2026-07-16', close: 66835.54, open: 67900.43, high: 68069.82, low: 66499.49 },
    ]);
    const rowMap = new Map<string, IndicatorRow>();
    const summary = await fillYahoo(
      {} as never,
      ['2026-07-15', '2026-07-16'],
      rowMap,
      true // dryRun: DBに触れない
    );
    expect(summary.source).toBe('official');
    expect(summary.pending).toBe(2);
    expect(summary.missing).toBe(0);
    expect(mockYahooBars).not.toHaveBeenCalled();
    expect(rowMap.get('2026-07-16')?.nikkei_high).toBe(68069.82);
  });

  it('公式CSVがthrowしたらYahooへフォールバックし source=yahoo', async () => {
    mockOfficial.mockRejectedValueOnce(new Error('HTTP 500'));
    mockYahooBars.mockResolvedValueOnce([
      { date: '2026-07-16', close: 66835.54, open: null, high: null, low: null },
    ]);
    const rowMap = new Map<string, IndicatorRow>();
    const summary = await fillYahoo({} as never, ['2026-07-16'], rowMap, true);
    expect(summary.source).toBe('yahoo');
    expect(mockYahooBars).toHaveBeenCalledWith('2026-07-16', '2026-07-16');
    expect(rowMap.get('2026-07-16')?.nikkei_close).toBe(66835.54);
    expect(rowMap.get('2026-07-16')?.nikkei_high).toBeNull();
  });

  it('pendingの日付範囲外の公式CSV行はウィンドウ化で除外される', async () => {
    mockOfficial.mockResolvedValueOnce([
      { date: '2023-01-04', close: 25716.86, open: 25834.93, high: 25840.68, low: 25661.89 },
      { date: '2026-07-16', close: 66835.54, open: 67900.43, high: 68069.82, low: 66499.49 },
    ]);
    const rowMap = new Map<string, IndicatorRow>();
    const summary = await fillYahoo({} as never, ['2026-07-16'], rowMap, true);
    expect(summary.pending).toBe(1);
    expect(rowMap.has('2023-01-04')).toBe(false);
    expect(rowMap.get('2026-07-16')?.nikkei_open).toBe(67900.43);
  });
});
