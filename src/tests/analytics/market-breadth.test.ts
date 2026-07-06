/**
 * analytics/market-breadth.ts のユニットテスト
 */

import { describe, it, expect } from 'vitest';
import {
  BreadthAccumulator,
  computeAdvDecRatio25,
  includesPreviousYear,
  PRIME_MARKET_CODES,
  type BreadthBar,
} from '@/lib/analytics/market-breadth';

function bar(code: string, close: number, high?: number, low?: number, turnover = 0): BreadthBar {
  return {
    code,
    adjClose: close,
    adjHigh: high ?? close,
    adjLow: low ?? close,
    turnoverValue: turnover,
  };
}

describe('includesPreviousYear', () => {
  it('1〜3月は前年来', () => {
    expect(includesPreviousYear('2026-01-05')).toBe(true);
    expect(includesPreviousYear('2026-03-31')).toBe(true);
  });
  it('4月以降は年初来', () => {
    expect(includesPreviousYear('2026-04-01')).toBe(false);
    expect(includesPreviousYear('2026-12-30')).toBe(false);
  });
});

describe('BreadthAccumulator', () => {
  const prime = new Set(['A', 'B', 'C']);

  it('騰落数: 前日終値との比較（初日はカウント対象外）', () => {
    const acc = new BreadthAccumulator();
    const d1 = acc.addDay('2026-05-01', [bar('A', 100), bar('B', 200), bar('C', 300)], prime);
    expect(d1.advancers + d1.decliners + d1.unchanged).toBe(0); // 前日データなし

    const d2 = acc.addDay('2026-05-02', [bar('A', 110), bar('B', 190), bar('C', 300)], prime);
    expect(d2.advancers).toBe(1); // A
    expect(d2.decliners).toBe(1); // B
    expect(d2.unchanged).toBe(1); // C
  });

  it('ユニバース外の銘柄はカウントしないが状態は維持する', () => {
    const acc = new BreadthAccumulator();
    acc.addDay('2026-05-01', [bar('A', 100), bar('X', 500)], prime);
    const d2 = acc.addDay('2026-05-02', [bar('A', 110), bar('X', 600)], prime);
    expect(d2.advancers).toBe(1); // X はプライム外
    expect(d2.primeBarCount).toBe(1);

    // X が後日プライム入りしたら、過去からの状態（前日終値）を使ってカウントされる
    const primeWithX = new Set(['A', 'X']);
    const d3 = acc.addDay('2026-05-07', [bar('A', 105), bar('X', 700)], primeWithX);
    expect(d3.advancers).toBe(1); // X: 600→700
    expect(d3.decliners).toBe(1); // A: 110→105
  });

  it('新高値/新安値: 基準期間（当日を含まない）の高値/安値更新をカウント', () => {
    const acc = new BreadthAccumulator();
    acc.addDay('2026-05-01', [bar('A', 100, 105, 95)], prime);
    // 高値更新
    const d2 = acc.addDay('2026-05-02', [bar('A', 106, 107, 100)], prime);
    expect(d2.newHighs).toBe(1);
    expect(d2.newLows).toBe(0);
    // 前日高値(107)未満の高値 → 新高値ではない
    const d3 = acc.addDay('2026-05-07', [bar('A', 90, 100, 88)], prime);
    expect(d3.newHighs).toBe(0);
    expect(d3.newLows).toBe(1); // 安値88 < 過去最安値95
  });

  it('新高値: 履歴のない銘柄（上場初日）はカウント対象外', () => {
    const acc = new BreadthAccumulator();
    const d1 = acc.addDay('2026-05-01', [bar('NEW', 1000, 1200, 900)], new Set(['NEW']));
    expect(d1.newHighs).toBe(0);
    expect(d1.newLows).toBe(0);
  });

  it('年初来リセット: 4月以降は当年のみが基準期間', () => {
    const acc = new BreadthAccumulator();
    // 前年に高値150
    acc.addDay('2025-12-30', [bar('A', 100, 150, 90)], prime);
    // 当年1月: 前年来なので150が基準 → 120では新高値ではない
    const jan = acc.addDay('2026-01-06', [bar('A', 110, 120, 100)], prime);
    expect(jan.newHighs).toBe(0);
    // 4月: 年初来（2026-01-06以降の高値120が基準）→ 130で新高値
    const apr = acc.addDay('2026-04-01', [bar('A', 125, 130, 120)], prime);
    expect(apr.newHighs).toBe(1);
    // 前年の安値90は4月以降は基準外: 95では新安値ではない（当年安値は100）
    const apr2 = acc.addDay('2026-04-02', [bar('A', 96, 100, 95)], prime);
    expect(apr2.newLows).toBe(1); // 当年最安値100を更新
  });

  it('1〜3月は前年来: 前年の高値も基準期間に含む', () => {
    const acc = new BreadthAccumulator();
    acc.addDay('2025-06-02', [bar('A', 100, 200, 50)], prime);
    // 2026年1月: 前年高値200が基準 → 180は新高値ではない
    const jan = acc.addDay('2026-01-06', [bar('A', 170, 180, 160)], prime);
    expect(jan.newHighs).toBe(0);
    // 210なら新高値
    const jan2 = acc.addDay('2026-01-07', [bar('A', 205, 210, 200)], prime);
    expect(jan2.newHighs).toBe(1);
  });

  it('2年以上取引のない銘柄は基準期間の履歴なし扱い', () => {
    const acc = new BreadthAccumulator();
    acc.addDay('2024-06-03', [bar('A', 100, 150, 50)], prime);
    // 2026年1月（前年来=2025年以降）: 2024年の高値は基準外
    const d = acc.addDay('2026-01-06', [bar('A', 200, 210, 190)], prime);
    expect(d.newHighs).toBe(0);
    expect(d.newLows).toBe(0);
  });

  it('売買代金はプライムユニバースのみ合算', () => {
    const acc = new BreadthAccumulator();
    const d = acc.addDay(
      '2026-05-01',
      [bar('A', 100, 100, 100, 1000), bar('X', 100, 100, 100, 9999)],
      prime
    );
    expect(d.turnoverValue).toBe(1000);
  });

  it('adj値がnullの銘柄は騰落・高安の対象外でも落ちない', () => {
    const acc = new BreadthAccumulator();
    acc.addDay('2026-05-01', [bar('A', 100)], prime);
    const d = acc.addDay('2026-05-02', [{ code: 'A', adjClose: null, adjHigh: null, adjLow: null, turnoverValue: null }], prime);
    expect(d.advancers + d.decliners + d.unchanged).toBe(0);
    expect(d.newHighs).toBe(0);
  });
});

describe('computeAdvDecRatio25', () => {
  it('25日窓で Σ値上がり/Σ値下がり×100', () => {
    const adv = Array(25).fill(150);
    const dec = Array(25).fill(100);
    expect(computeAdvDecRatio25(adv, dec)).toBe(150);
  });
  it('窓が25未満/nullを含む/分母0はnull', () => {
    expect(computeAdvDecRatio25(Array(24).fill(1), Array(24).fill(1))).toBeNull();
    const withNull = [...Array(24).fill(1), null];
    expect(computeAdvDecRatio25(withNull, Array(25).fill(1))).toBeNull();
    expect(computeAdvDecRatio25(Array(25).fill(1), Array(25).fill(0))).toBeNull();
  });
});

describe('PRIME_MARKET_CODES', () => {
  it('プライム(0111)と旧東証一部(0101)を含む', () => {
    expect(PRIME_MARKET_CODES.has('0111')).toBe(true);
    expect(PRIME_MARKET_CODES.has('0101')).toBe(true);
    expect(PRIME_MARKET_CODES.has('0112')).toBe(false);
  });
});
