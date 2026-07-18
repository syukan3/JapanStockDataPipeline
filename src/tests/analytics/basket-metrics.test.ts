/** basket-metrics.ts の純関数群を検証する。 */

import { describe, it, expect } from 'vitest';
import {
  toNumberOrNull,
  parseBasketConstituentRow,
  parseStockMetricRow,
  computeWeights,
  weightedHarmonicMean,
  weightedAverage,
  selectEffectiveForecastEps,
  computeIndexLevel,
  computeWeightedEpsLevel,
  type BasketConstituent,
  type StockMetricSnapshot,
  type WeightedMetricEntry,
  type IndexReturnEntry,
} from '@/lib/analytics/basket-metrics';

describe('toNumberOrNull', () => {
  it('nullとundefinedはnullにする', () => {
    expect(toNumberOrNull(null)).toBeNull();
    expect(toNumberOrNull(undefined)).toBeNull();
  });

  it('数値文字列(PostgRESTのnumeric)を数値に変換する', () => {
    expect(toNumberOrNull('123.45')).toBe(123.45);
  });

  it('数値はそのまま返す', () => {
    expect(toNumberOrNull(42)).toBe(42);
  });

  it('数値化できない文字列はnullにする', () => {
    expect(toNumberOrNull('abc')).toBeNull();
  });

  it('NaN/Infinityはnullにする', () => {
    expect(toNumberOrNull(NaN)).toBeNull();
    expect(toNumberOrNull(Infinity)).toBeNull();
  });
});

describe('parseBasketConstituentRow', () => {
  it('numeric列を数値化する', () => {
    expect(
      parseBasketConstituentRow({
        local_code: '68570',
        weight_factor: '1.23456789',
        official_weight: '5.500',
      })
    ).toEqual({ local_code: '68570', weight_factor: 1.23456789, official_weight: 5.5 });
  });

  it('weight_factorがnullなら0にする（欠損データでの暴走防止）', () => {
    expect(
      parseBasketConstituentRow({ local_code: '68570', weight_factor: null, official_weight: null })
    ).toEqual({ local_code: '68570', weight_factor: 0, official_weight: null });
  });
});

describe('parseStockMetricRow', () => {
  it('numeric列を数値化する', () => {
    expect(
      parseStockMetricRow({
        local_code: '68570',
        market_cap: '123456789.00',
        per: '25.50',
        pbr: '3.10',
        psr: '8.20',
        dividend_yield: '0.500',
        close: '9800.00',
      })
    ).toEqual({
      local_code: '68570',
      market_cap: 123456789,
      per: 25.5,
      pbr: 3.1,
      psr: 8.2,
      dividend_yield: 0.5,
      close: 9800,
    });
  });
});

describe('computeWeights', () => {
  function constituent(overrides: Partial<BasketConstituent>): BasketConstituent {
    return { local_code: 'X', weight_factor: 1, official_weight: null, ...overrides };
  }
  function metric(overrides: Partial<StockMetricSnapshot>): StockMetricSnapshot {
    return {
      local_code: 'X',
      market_cap: null,
      per: null,
      pbr: null,
      psr: null,
      dividend_yield: null,
      close: null,
      ...overrides,
    };
  }

  it('weight_factor×market_capで正規化し合計が1になる', () => {
    const constituents = [
      constituent({ local_code: 'A', weight_factor: 2 }),
      constituent({ local_code: 'B', weight_factor: 1 }),
    ];
    const metrics = new Map([
      ['A', metric({ local_code: 'A', market_cap: 100 })], // adjMcap=200
      ['B', metric({ local_code: 'B', market_cap: 100 })], // adjMcap=100
    ]);

    const { weights, coveragePct } = computeWeights(constituents, metrics);

    expect(weights.get('A')).toBeCloseTo(200 / 300);
    expect(weights.get('B')).toBeCloseTo(100 / 300);
    expect(coveragePct).toBe(100);
  });

  it('market_capがnullまたは0以下の銘柄は除外して再正規化する', () => {
    const constituents = [
      constituent({ local_code: 'A' }),
      constituent({ local_code: 'B' }),
      constituent({ local_code: 'C' }),
    ];
    const metrics = new Map([
      ['A', metric({ local_code: 'A', market_cap: 100 })],
      ['B', metric({ local_code: 'B', market_cap: null })],
      ['C', metric({ local_code: 'C', market_cap: 0 })],
    ]);

    const { weights } = computeWeights(constituents, metrics);

    expect(weights.get('A')).toBe(1);
    expect(weights.has('B')).toBe(false);
    expect(weights.has('C')).toBe(false);
  });

  it('official_weightの合計比率でcoverage_pctを算出する', () => {
    const constituents = [
      constituent({ local_code: 'A', official_weight: 15 }),
      constituent({ local_code: 'B', official_weight: 5 }),
    ];
    const metrics = new Map([
      ['A', metric({ local_code: 'A', market_cap: 100 })],
      // B は当日メトリクス欠損
    ]);

    const { coveragePct } = computeWeights(constituents, metrics);

    expect(coveragePct).toBe(75); // 15 / (15+5) * 100
  });

  it('official_weightが全銘柄nullなら採用銘柄数の単純割合にフォールバックする', () => {
    const constituents = [
      constituent({ local_code: 'A' }),
      constituent({ local_code: 'B' }),
      constituent({ local_code: 'C' }),
      constituent({ local_code: 'D' }),
    ];
    const metrics = new Map([
      ['A', metric({ local_code: 'A', market_cap: 100 })],
      ['B', metric({ local_code: 'B', market_cap: 100 })],
    ]);

    const { coveragePct } = computeWeights(constituents, metrics);

    expect(coveragePct).toBe(50); // 2/4 * 100
  });

  it('構成銘柄が空なら重みも空でcoverage_pctは0', () => {
    const { weights, coveragePct } = computeWeights([], new Map());
    expect(weights.size).toBe(0);
    expect(coveragePct).toBe(0);
  });

  it('全銘柄market_cap欠損なら重みが空でcoverage_pctは0', () => {
    const constituents = [constituent({ local_code: 'A', official_weight: 10 })];
    const { weights, coveragePct } = computeWeights(constituents, new Map());
    expect(weights.size).toBe(0);
    expect(coveragePct).toBe(0);
  });
});

describe('weightedHarmonicMean', () => {
  function entry(overrides: Partial<WeightedMetricEntry>): WeightedMetricEntry {
    return { localCode: 'X', weightFactor: 1, marketCap: 100, value: 10, ...overrides };
  }

  it('Σadj_mcap / Σ(adj_mcap/value) で調和集計する', () => {
    // A: adjMcap=100, per=10 → term=10
    // B: adjMcap=200, per=20 → term=10
    // weighted_per = (100+200) / (10+10) = 15
    const result = weightedHarmonicMean([
      entry({ localCode: 'A', weightFactor: 1, marketCap: 100, value: 10 }),
      entry({ localCode: 'B', weightFactor: 2, marketCap: 100, value: 20 }),
    ]);
    expect(result).toBeCloseTo(15);
  });

  it('valueがnullまたは0以下の銘柄は分子分母から除外する', () => {
    const result = weightedHarmonicMean([
      entry({ localCode: 'A', marketCap: 100, value: 10 }),
      entry({ localCode: 'B', marketCap: 100, value: null }),
      entry({ localCode: 'C', marketCap: 100, value: -5 }),
      entry({ localCode: 'D', marketCap: 100, value: 0 }),
    ]);
    // A のみ採用 → weighted_per = 100 / (100/10) = 10
    expect(result).toBeCloseTo(10);
  });

  it('market_capがnullまたは0以下の銘柄は除外する', () => {
    const result = weightedHarmonicMean([
      entry({ localCode: 'A', marketCap: 100, value: 10 }),
      entry({ localCode: 'B', marketCap: null, value: 10 }),
      entry({ localCode: 'C', marketCap: 0, value: 10 }),
    ]);
    expect(result).toBeCloseTo(10);
  });

  it('採用銘柄が0件（全滅）ならnullを返す', () => {
    const result = weightedHarmonicMean([
      entry({ localCode: 'A', value: null }),
      entry({ localCode: 'B', marketCap: null }),
    ]);
    expect(result).toBeNull();
  });

  it('空配列はnullを返す', () => {
    expect(weightedHarmonicMean([])).toBeNull();
  });
});

describe('weightedAverage', () => {
  function entry(overrides: Partial<WeightedMetricEntry>): WeightedMetricEntry {
    return { localCode: 'X', weightFactor: 1, marketCap: 100, value: 1, ...overrides };
  }

  it('Σ(adj_mcap×value) / Σadj_mcap で加重平均する', () => {
    // A: adjMcap=100, value=2 → 200
    // B: adjMcap=100, value=4 → 400
    // weighted = 600 / 200 = 3
    const result = weightedAverage([
      entry({ localCode: 'A', marketCap: 100, value: 2 }),
      entry({ localCode: 'B', marketCap: 100, value: 4 }),
    ]);
    expect(result).toBeCloseTo(3);
  });

  it('value=0は有効値として許容する（配当利回り0%）', () => {
    const result = weightedAverage([
      entry({ localCode: 'A', marketCap: 100, value: 0 }),
      entry({ localCode: 'B', marketCap: 100, value: 2 }),
    ]);
    expect(result).toBeCloseTo(1); // (0+200)/200
  });

  it('valueがnullの銘柄は除外して再正規化する', () => {
    const result = weightedAverage([
      entry({ localCode: 'A', marketCap: 100, value: 5 }),
      entry({ localCode: 'B', marketCap: 100, value: null }),
    ]);
    expect(result).toBeCloseTo(5);
  });

  it('採用銘柄が0件ならnullを返す', () => {
    expect(weightedAverage([entry({ marketCap: null }), entry({ value: null })])).toBeNull();
  });
});

describe('selectEffectiveForecastEps', () => {
  it('forecast_epsがあればそれを使う', () => {
    expect(selectEffectiveForecastEps({ forecast_eps: 120, next_forecast_eps: 150 })).toBe(120);
  });

  it('forecast_epsがnullならnext_forecast_epsにフォールバックする（期末開示の既知の癖）', () => {
    expect(selectEffectiveForecastEps({ forecast_eps: null, next_forecast_eps: 150 })).toBe(150);
  });

  it('両方nullならnullを返す', () => {
    expect(selectEffectiveForecastEps({ forecast_eps: null, next_forecast_eps: null })).toBeNull();
  });

  it('sourceがundefinedならnullを返す', () => {
    expect(selectEffectiveForecastEps(undefined)).toBeNull();
  });
});

describe('computeIndexLevel', () => {
  function entry(overrides: Partial<IndexReturnEntry>): IndexReturnEntry {
    return {
      localCode: 'X',
      weightFactor: 1,
      prevMarketCap: 100,
      prevClose: 100,
      currClose: 100,
      ...overrides,
    };
  }

  it('前行なし(prevIndexLevel=null)ならnullを返す', () => {
    const result = computeIndexLevel(null, [entry({})]);
    expect(result).toEqual({ indexLevel: null, indexReturn: null, includedCount: 0 });
  });

  it('前日ウエート×リターンの加重平均で連結する', () => {
    // A: prevMcap=100(weight_factor=1)→ contribution=100, ret=110/100=1.1
    // B: prevMcap=100(weight_factor=1)→ contribution=100, ret=90/100=0.9
    // w_i(t-1) は50%ずつ → indexReturn = 0.5*1.1 + 0.5*0.9 = 1.0
    const result = computeIndexLevel(1000, [
      entry({ localCode: 'A', prevMarketCap: 100, prevClose: 100, currClose: 110 }),
      entry({ localCode: 'B', prevMarketCap: 100, prevClose: 100, currClose: 90 }),
    ]);
    expect(result.indexReturn).toBeCloseTo(1.0);
    expect(result.indexLevel).toBeCloseTo(1000);
    expect(result.includedCount).toBe(2);
  });

  it('adj_closeが欠損する銘柄は除外して残りで再正規化する', () => {
    // A: 採用, ret=1.2 / B: currCloseがnullで除外
    // 再正規化後はAのみ100%採用 → indexReturn = 1.2
    const result = computeIndexLevel(1000, [
      entry({ localCode: 'A', prevMarketCap: 100, prevClose: 100, currClose: 120 }),
      entry({ localCode: 'B', prevMarketCap: 100, prevClose: 100, currClose: null }),
    ]);
    expect(result.indexReturn).toBeCloseTo(1.2);
    expect(result.indexLevel).toBeCloseTo(1200);
    expect(result.includedCount).toBe(1);
  });

  it('全銘柄prevMarketCapが欠損ならnullを返す', () => {
    const result = computeIndexLevel(1000, [
      entry({ prevMarketCap: null }),
      entry({ prevMarketCap: 0 }),
    ]);
    expect(result).toEqual({ indexLevel: null, indexReturn: null, includedCount: 0 });
  });

  it('全銘柄でリターン計算不能ならnullを返す（w_i(t-1)は存在するが再正規化分母が0）', () => {
    const result = computeIndexLevel(1000, [
      entry({ prevMarketCap: 100, prevClose: null }),
      entry({ prevMarketCap: 100, currClose: null }),
    ]);
    expect(result).toEqual({ indexLevel: null, indexReturn: null, includedCount: 0 });
  });

  it('空配列ならnullを返す', () => {
    expect(computeIndexLevel(1000, [])).toEqual({
      indexLevel: null,
      indexReturn: null,
      includedCount: 0,
    });
  });
});

describe('computeWeightedEpsLevel', () => {
  it('index_level / weighted_per を計算する', () => {
    expect(computeWeightedEpsLevel(1500, 15)).toBe(100);
  });

  it('index_levelがnullならnullを返す', () => {
    expect(computeWeightedEpsLevel(null, 15)).toBeNull();
  });

  it('weighted_perがnullまたは0ならnullを返す', () => {
    expect(computeWeightedEpsLevel(1500, null)).toBeNull();
    expect(computeWeightedEpsLevel(1500, 0)).toBeNull();
  });
});
