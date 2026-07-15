/**
 * analytics/market-breadth.ts のユニットテスト
 */

import { describe, it, expect } from 'vitest';
import {
  BreadthAccumulator,
  computeAdvDecRatio25,
  computeSma,
  includesPreviousYear,
  PRIME_MARKET_CODES,
  SMA_LONG_WINDOW,
  SMA_SHORT_WINDOW,
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

/** 2026年始からdayIndex日後の日付（年をまたがない範囲でのみ使う） */
function seqDate(dayIndex: number): string {
  const d = new Date(Date.UTC(2026, 0, 1));
  d.setUTCDate(d.getUTCDate() + dayIndex);
  return d.toISOString().slice(0, 10);
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

describe('computeSma', () => {
  it('件数がwindow未満はnull（分母除外）', () => {
    expect(computeSma([1, 2, 3], 5)).toBeNull();
    expect(computeSma(Array(24).fill(100), 25)).toBeNull();
    expect(computeSma([], 25)).toBeNull();
  });

  it('件数がちょうどwindowなら平均を返す', () => {
    expect(computeSma([10, 20, 30], 3)).toBe(20);
    expect(computeSma(Array(25).fill(100), 25)).toBe(100);
  });

  it('windowを超える配列は直近window件のみで平均する（古い値は無視）', () => {
    expect(computeSma([10000, 10, 20, 30], 3)).toBe(20);
  });
});

describe('BreadthAccumulator: breadth %（SMA上回り比率）', () => {
  const primeA = new Set(['A']);

  it('SMA25境界: 24件はnull、25件目から算出（当日終値を含む）', () => {
    const acc = new BreadthAccumulator();
    let last: ReturnType<BreadthAccumulator['addDay']> | undefined;
    for (let i = 0; i < SMA_SHORT_WINDOW - 1; i++) {
      last = acc.addDay(seqDate(i), [bar('A', 100)], primeA);
    }
    expect(last!.pctAboveSma25).toBeNull(); // 24件: 分母0

    // 25件目: SMA25=(24*100+110)/25=100.4、close110は上回る
    const d25 = acc.addDay(seqDate(SMA_SHORT_WINDOW - 1), [bar('A', 110)], primeA);
    expect(d25.pctAboveSma25).toBe(100);
  });

  it('SMA200境界: 199件はnull、200件目から算出', () => {
    const acc = new BreadthAccumulator();
    let last: ReturnType<BreadthAccumulator['addDay']> | undefined;
    for (let i = 0; i < SMA_LONG_WINDOW - 1; i++) {
      last = acc.addDay(seqDate(i), [bar('A', 100)], primeA);
    }
    expect(last!.pctAboveSma200).toBeNull(); // 199件: 分母0

    // 200件目: SMA200=(199*100+90)/200=99.95、close90は下回る
    const d200 = acc.addDay(seqDate(SMA_LONG_WINDOW - 1), [bar('A', 90)], primeA);
    expect(d200.pctAboveSma200).toBe(0);
  });

  it('ユニバース内で全銘柄が分母不足の日はnull（0%ではない）', () => {
    const acc = new BreadthAccumulator();
    const d1 = acc.addDay('2026-05-01', [bar('A', 100), bar('B', 200)], new Set(['A', 'B']));
    expect(d1.pctAboveSma25).toBeNull();
    expect(d1.pctAboveSma200).toBeNull();
  });

  it('上場直後銘柄（履歴不足）は分母除外され、既存銘柄のみで比率が決まる', () => {
    const acc = new BreadthAccumulator();
    for (let i = 0; i < SMA_SHORT_WINDOW - 1; i++) {
      acc.addDay(seqDate(i), [bar('A', 100)], primeA); // A は25日分の履歴を積む
    }
    // 25日目にNEWが上場（履歴1件のみ）。NEWは分母除外され、Aのみで比率が決まる
    const d = acc.addDay(
      seqDate(SMA_SHORT_WINDOW - 1),
      [bar('A', 110), bar('NEW', 500)],
      new Set(['A', 'NEW'])
    );
    expect(d.pctAboveSma25).toBe(100); // NEWが分母に入れば50%になるはずだが除外され100%
  });

  it('プライム外の銘柄も終値状態(closes)は維持され、プライム入り後すぐにSMAへ反映される', () => {
    const acc = new BreadthAccumulator();
    // X はプライム外だが24日分の終値が状態として積まれる
    for (let i = 0; i < SMA_SHORT_WINDOW - 1; i++) {
      acc.addDay(seqDate(i), [bar('A', 100), bar('X', 100)], primeA);
    }
    // 25日目にXがプライム入り。過去の終値履歴があるため直後からSMA25が算出できる
    const d = acc.addDay(
      seqDate(SMA_SHORT_WINDOW - 1),
      [bar('A', 100), bar('X', 130)],
      new Set(['A', 'X'])
    );
    // A: SMA25=100（全て100）→ close100は上回らない。X: SMA25=101.2→ close130は上回る
    expect(d.pctAboveSma25).toBe(50); // 2銘柄中1銘柄(X)のみ上回る
  });

  it('分割調整: 調整後終値の連続系列であれば分割日の見せかけの急落・急騰は起きない', () => {
    const acc = new BreadthAccumulator();
    // パイプライン側で既に分割調整済みの調整後終値を渡す前提（生値なら分割日に半値等に
    // ジャンプするところ、調整後終値は緩やかに連続する）
    const adjustedCloses = Array.from({ length: SMA_SHORT_WINDOW }, (_, i) => 100 + i); // 100..124
    let last: ReturnType<BreadthAccumulator['addDay']> | undefined;
    adjustedCloses.forEach((close, i) => {
      last = acc.addDay(seqDate(i), [bar('A', close)], primeA);
    });
    const expectedSma = computeSma(adjustedCloses, SMA_SHORT_WINDOW);
    expect(expectedSma).toBe(112); // (100+...+124)/25
    expect(last!.pctAboveSma25).toBe(100); // 124 > 112
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
