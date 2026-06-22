import { describe, it, expect } from 'vitest';
import {
  sma,
  ema,
  rsi,
  macd,
  bollinger,
  stochastic,
  atr,
  obv,
  ichimokuState,
  detectCross,
  computeTechnicalSnapshot,
  type Bar,
} from '@/lib/analytics/technical';
import golden from '@/lib/analytics/__fixtures__/golden-vectors.json';

describe('sma', () => {
  it('単純移動平均（period 未満は null）', () => {
    expect(sma([1, 2, 3, 4, 5], 3)).toEqual([null, null, 2, 3, 4]);
  });
});

describe('ema', () => {
  it('先頭 period 本の SMA をシードに EMA を計算', () => {
    // seed=SMA(1,2,3)=2, k=0.5 → 4*.5+2*.5=3, 5*.5+3*.5=4
    expect(ema([1, 2, 3, 4, 5], 3)).toEqual([null, null, 2, 3, 4]);
  });
});

describe('rsi (Wilder)', () => {
  it('手計算ケース period=3', () => {
    // closes [10,11,10,11,12] → [_,_,_,66.67,77.78]
    const r = rsi([10, 11, 10, 11, 12], 3);
    expect(r[0]).toBeNull();
    expect(r[2]).toBeNull();
    expect(r[3]).toBeCloseTo(66.6667, 3);
    expect(r[4]).toBeCloseTo(77.7778, 3);
  });

  it('単調増加は 100、単調減少は 0', () => {
    expect(rsi([1, 2, 3, 4, 5, 6, 7, 8], 3).at(-1)).toBe(100);
    expect(rsi([8, 7, 6, 5, 4, 3, 2, 1], 3).at(-1)).toBe(0);
  });
});

describe('macd', () => {
  it('macd = ema(fast) - ema(slow)、hist = macd - signal', () => {
    const closes = Array.from({ length: 60 }, (_, i) => 100 + i);
    const ef = ema(closes, 12);
    const es = ema(closes, 26);
    const { macd: m, signal, hist } = macd(closes, 12, 26, 9);
    const i = closes.length - 1;
    expect(m[i]).toBeCloseTo((ef[i] as number) - (es[i] as number), 6);
    expect(hist[i]).toBeCloseTo((m[i] as number) - (signal[i] as number), 6);
  });
});

describe('bollinger', () => {
  it('定数系列はバンド幅0・%Bはnull（上下バンド一致）', () => {
    const closes = new Array(25).fill(100);
    const { mid, upper, lower, bandwidth, percentB } = bollinger(closes, 20, 2);
    const i = closes.length - 1;
    expect(mid[i]).toBe(100);
    expect(upper[i]).toBe(100);
    expect(lower[i]).toBe(100);
    expect(bandwidth[i]).toBe(0);
    expect(percentB[i]).toBeNull();
  });
});

describe('stochastic', () => {
  it('手計算ケース（smooth=1,dPeriod=1）', () => {
    const closes = [2, 4, 6, 8, 10];
    const highs = closes.map((c) => c + 1);
    const lows = closes.map((c) => c - 1);
    // i=4 window[3,4]: hh=11, ll=7, %K=(10-7)/(11-7)*100=75
    const { k, d } = stochastic(highs, lows, closes, 2, 1, 1);
    expect(k.at(-1)).toBeCloseTo(75, 6);
    expect(d.at(-1)).toBeCloseTo(75, 6);
  });
});

describe('atr (Wilder)', () => {
  it('手計算ケース period=2', () => {
    const closes = [10, 11, 12, 11, 10];
    const highs = [10.5, 11.5, 12.5, 11.5, 10.5];
    const lows = [9.5, 10.5, 11.5, 10.5, 9.5];
    // TR[1..4]=1.5, seed avg(TR1,TR2)=1.5 → 以降1.5
    const a = atr(highs, lows, closes, 2);
    expect(a[0]).toBeNull();
    expect(a[1]).toBeNull();
    expect(a[2]).toBeCloseTo(1.5, 6);
    expect(a.at(-1)).toBeCloseTo(1.5, 6);
  });
});

describe('obv', () => {
  it('上昇日は加算・下落日は減算', () => {
    // closes 10→11(+vol2)→10(-vol3)→10(0)
    expect(obv([10, 11, 10, 10], [1, 2, 3, 4])).toEqual([0, 2, -1, -1]);
  });
});

describe('ichimokuState', () => {
  it('単調増加トレンドは雲の上(above)', () => {
    const n = 90;
    const closes = Array.from({ length: n }, (_, i) => 100 + i);
    const highs = closes.map((c) => c + 1);
    const lows = closes.map((c) => c - 1);
    expect(ichimokuState(highs, lows, closes)).toBe('above');
  });

  it('データ不足(雲が算出できない)は null', () => {
    const closes = [1, 2, 3, 4, 5];
    expect(ichimokuState(closes.map((c) => c + 1), closes.map((c) => c - 1), closes)).toBeNull();
  });
});

describe('detectCross', () => {
  it('short が long を上抜け=golden、経過営業日も返す', () => {
    const c = detectCross([1, 2, 3, 4], [2, 2, 2, 2]);
    expect(c).toEqual({ type: 'golden', age: 1 });
  });

  it('short が long を下抜け=dead', () => {
    const c = detectCross([3, 3, 1, 0], [2, 2, 2, 2]);
    expect(c?.type).toBe('dead');
  });

  it('クロスなしは null', () => {
    expect(detectCross([3, 4, 5], [1, 1, 1])).toBeNull();
  });
});

describe('computeTechnicalSnapshot', () => {
  it('データ不足でもクラッシュせず、計算できない指標は null', () => {
    const bars: Bar[] = [
      { open: 10, high: 11, low: 9, close: 10, volume: 100 },
      { open: 10, high: 12, low: 10, close: 11, volume: 120 },
    ];
    const snap = computeTechnicalSnapshot(bars);
    expect(snap).not.toBeNull();
    expect(snap!.close).toBe(11);
    expect(snap!.sma_25).toBeNull();
    expect(snap!.sma_200).toBeNull();
    expect(snap!.rsi_14).toBeNull();
  });

  it('空配列は null', () => {
    expect(computeTechnicalSnapshot([])).toBeNull();
  });

  // ゴールデンテストベクタ: Portfolio 側 lib/indicators.ts と同一値であることを担保
  it('ゴールデンベクタと一致（両リポ数式整合）', () => {
    const snap = computeTechnicalSnapshot(golden.input as Bar[]);
    expect(snap).toEqual(golden.expected);
  });
});
