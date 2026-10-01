/**
 * market/alt-prices.ts と yahoo-equity-client.ts（純関数部分）のユニットテスト
 */

import { describe, it, expect } from 'vitest';
import {
  cumulativeSplitRatioAfter,
  toAltBarRecords,
  filterAnomalies,
  computeTopixProxy,
  FIXED_ALT_CODES,
} from '@/lib/market/alt-prices';
import { parseYahooEquityChart, toYahooSymbol, buildChartQuery } from '@/lib/market/yahoo-equity-client';
import type { YahooEquityBar } from '@/lib/market/yahoo-equity-client';

const bar = (date: string, adjClose: number, extra?: Partial<YahooEquityBar>): YahooEquityBar => ({
  date,
  adjOpen: adjClose,
  adjHigh: adjClose,
  adjLow: adjClose,
  adjClose,
  adjVolume: 1000,
  ...extra,
});

// 2026-09-29 に 1→2 分割した 1925 の実データ（Yahoo quote.close = J-Quants adj_close）
const daiwa = [bar('2026-09-24', 2266), bar('2026-09-25', 2275.5), bar('2026-09-28', 2260.5), bar('2026-09-29', 2173.5), bar('2026-09-30', 2196)];
const daiwaSplit = [{ exDate: '2026-09-29', ratio: 2 }];

describe('toYahooSymbol', () => {
  it('5文字目が 0 のコードは4文字＋.T', () => {
    expect(toYahooSymbol('50160')).toBe('5016.T');
    expect(toYahooSymbol('200A0')).toBe('200A.T');
  });
  it('5文字目が 0 でない・形式違いは null', () => {
    expect(toYahooSymbol('25935')).toBeNull();
    expect(toYahooSymbol('5016')).toBeNull();
  });
});

describe('buildChartQuery', () => {
  it('to の翌日 00:00 JST までを指定し、分割イベントを要求する', () => {
    expect(buildChartQuery('2026-09-01', '2026-09-30')).toBe(
      'period1=1788188400&period2=1790780400&interval=1d&events=split'
    );
  });
});

describe('parseYahooEquityChart', () => {
  it('quote を調整値として読み、分割イベントを比率に直す', () => {
    const json = {
      chart: {
        result: [
          {
            timestamp: [1790640000, 1790726400], // 2026-09-29 / 09-30 09:00 JST
            indicators: { quote: [{ open: [2200, 2180], high: [2210, 2200], low: [2170, 2175], close: [2173.4999, 2196], volume: [300, null] }] },
            events: { splits: { '1790640000': { date: 1790640000, numerator: 2, denominator: 1 } } },
          },
        ],
      },
    };
    const parsed = parseYahooEquityChart(json);
    expect(parsed.bars).toEqual([
      { date: '2026-09-29', adjOpen: 2200, adjHigh: 2210, adjLow: 2170, adjClose: 2173.5, adjVolume: 300 },
      { date: '2026-09-30', adjOpen: 2180, adjHigh: 2200, adjLow: 2175, adjClose: 2196, adjVolume: null },
    ]);
    expect(parsed.splits).toEqual([{ exDate: '2026-09-29', ratio: 2 }]);
  });

  it('result が無ければ例外', () => {
    expect(() => parseYahooEquityChart({ chart: { result: null, error: { code: 'Not Found', description: 'x' } } })).toThrow(/Not Found/);
  });
});

describe('toAltBarRecords', () => {
  it('分割前の日は生値＝調整値×比率、分割日以降は生値＝調整値（J-Quants の close と一致）', () => {
    const rows = toAltBarRecords('19250', daiwa, daiwaSplit);
    expect(rows.map((r) => [r.trade_date, r.close, r.adj_close])).toEqual([
      ['2026-09-24', 4532, 2266],
      ['2026-09-25', 4551, 2275.5],
      ['2026-09-28', 4521, 2260.5],
      ['2026-09-29', 2173.5, 2173.5],
      ['2026-09-30', 2196, 2196],
    ]);
    expect(rows[0].volume).toBe(500); // 調整後出来高 1000 → 生値 500
    expect(rows.every((r) => r.source === 'yahoo')).toBe(true);
  });

  it('1→4 分割（1904）でも J-Quants の生値と一致', () => {
    const rows = toAltBarRecords('19040', [bar('2026-09-28', 1277.5), bar('2026-09-29', 1255)], [{ exDate: '2026-09-29', ratio: 4 }]);
    expect(rows.map((r) => r.close)).toEqual([5110, 1255]);
  });

  it('cumulativeSplitRatioAfter は ex-date 当日を含めない', () => {
    expect(cumulativeSplitRatioAfter('2026-09-29', daiwaSplit)).toBe(1);
    expect(cumulativeSplitRatioAfter('2026-09-28', daiwaSplit)).toBe(2);
  });
});

describe('filterAnomalies', () => {
  it('1306 の 1/10 異常（2026-03-30/31）を落とし、翌日は正常値と比べる', () => {
    const bars = [bar('2026-03-27', 376.4), bar('2026-03-30', 37.64), bar('2026-03-31', 37.14), bar('2026-04-01', 371.2)];
    const { accepted, rejected } = filterAnomalies(bars, 375);
    expect(accepted.map((b) => b.date)).toEqual(['2026-03-27', '2026-04-01']);
    expect(rejected.map((r) => [r.date, r.prevAdjClose])).toEqual([
      ['2026-03-30', 376.4],
      ['2026-03-31', 376.4],
    ]);
  });

  it('調整値は分割をまたいでも連続なので、分割日は落とさない', () => {
    expect(filterAnomalies(daiwa, 2250).rejected).toEqual([]);
  });

  it('基準が無ければ最初の日は採用', () => {
    expect(filterAnomalies([bar('2026-09-30', 100)], null).accepted).toHaveLength(1);
  });
});

describe('computeTopixProxy', () => {
  const etf = [
    { date: '2026-09-29', adjOpen: 425, adjHigh: 426, adjLow: 424, adjClose: 425.4 },
    { date: '2026-09-30', adjOpen: 428, adjHigh: 432, adjLow: 427, adjClose: 431.5 },
    { date: '2026-10-01', adjOpen: 430, adjHigh: 433, adjLow: 429, adjClose: 432 },
  ];

  it('基準日の比率で延長し、基準日より後だけを返す', () => {
    const rows = computeTopixProxy({ date: '2026-09-29', close: 4041.13 }, etf);
    expect(rows.map((r) => r.trade_date)).toEqual(['2026-09-30', '2026-10-01']);
    const f = 4041.13 / 425.4;
    expect(rows[0].close).toBeCloseTo(431.5 * f, 2);
    expect(rows[0].open).toBeCloseTo(428 * f, 2);
    expect(rows.every((r) => r.source === 'proxy_etf_13060')).toBe(true);
  });

  it('基準日に ETF の値が無ければ例外', () => {
    expect(() => computeTopixProxy({ date: '2026-09-28', close: 4112 }, etf)).toThrow(/基準日/);
  });
});

describe('FIXED_ALT_CODES', () => {
  it('TOPIX 推計用 1306 とマクロ対立軸の8本', () => {
    expect(FIXED_ALT_CODES).toHaveLength(9);
    expect(FIXED_ALT_CODES[0]).toBe('13060');
  });
});

describe('YAHOO_USER_AGENT', () => {
  it('Yahoo には短い UA を送る（Chrome 風の長い UA は 429 になる・2026-10-02 実測）', async () => {
    const { YAHOO_USER_AGENT } = await import('@/lib/market/yahoo-equity-client');
    expect(YAHOO_USER_AGENT).toBe('Mozilla/5.0');
  });
});
