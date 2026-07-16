import { describe, it, expect } from 'vitest';
import {
  DIMENSIONS,
  VECTOR_LENGTH,
  toNumberOrNull,
  parseScreenRow,
  shouldExcludeStock,
  deriveRawDimensions,
  countCoverage,
  percentile,
  winsorize,
  zScore,
  clamp,
  fillNullWithZero,
  applyWeight,
  transformColumn,
  buildVectors,
  formatEmbeddingLiteral,
  cosineSimilarity,
  distributionStats,
  type RawScreenRow,
  type ParsedScreenRow,
} from '@/lib/analytics/factor-vectors';

describe('DIMENSIONS', () => {
  it('13次元・順序固定', () => {
    expect(DIMENSIONS.length).toBe(13);
    expect(VECTOR_LENGTH).toBe(13);
    expect(DIMENSIONS.map((d) => d.key)).toEqual([
      'earnings_yield',
      'log_pbr',
      'dividend_yield',
      'roe',
      'log_mcap',
      'value_pct',
      'quality_pct',
      'momentum_pct',
      'dev_25',
      'dev_200',
      'rsi_14',
      'atr_pct',
      'vol_ratio_20',
    ]);
  });

  it('重みは仕様通り', () => {
    const w = Object.fromEntries(DIMENSIONS.map((d) => [d.key, d.weight]));
    expect(w.earnings_yield).toBe(1.0);
    expect(w.dev_25).toBe(0.5);
    expect(w.dev_200).toBe(0.75);
    expect(w.rsi_14).toBe(0.5);
    expect(w.vol_ratio_20).toBe(0.5);
    expect(w.atr_pct).toBe(1.0);
  });
});

describe('toNumberOrNull', () => {
  it('null/undefinedはnull', () => {
    expect(toNumberOrNull(null)).toBeNull();
    expect(toNumberOrNull(undefined)).toBeNull();
  });

  it('文字列numericをnumberへ変換（PostgREST由来）', () => {
    expect(toNumberOrNull('12.5')).toBe(12.5);
    expect(toNumberOrNull('0')).toBe(0);
  });

  it('数値はそのまま', () => {
    expect(toNumberOrNull(3)).toBe(3);
  });

  it('非数値文字列はnull', () => {
    expect(toNumberOrNull('abc')).toBeNull();
    expect(toNumberOrNull('NaN')).toBeNull();
  });
});

describe('parseScreenRow', () => {
  it('全列をNumber()変換する', () => {
    const raw: RawScreenRow = {
      as_of_date: '2026-07-15',
      local_code: '6315',
      sector17_code: '13',
      sector17_name: '機械',
      market_cap: '123456789.5',
      per: '14.7',
      pbr: '1.2',
      dividend_yield: '2.5',
      roe: '10.1',
      value_pct: '55.5',
      quality_pct: '60.0',
      momentum_pct: '45.0',
      dev_25: '3.2',
      dev_200: null,
      rsi_14: '58.1',
      atr_pct: '2.1',
      vol_ratio_20: '1.1',
    };
    const parsed = parseScreenRow(raw);
    expect(parsed.local_code).toBe('6315');
    expect(parsed.market_cap).toBe(123456789.5);
    expect(parsed.per).toBe(14.7);
    expect(parsed.dev_200).toBeNull();
  });
});

function row(overrides: Partial<ParsedScreenRow> = {}): ParsedScreenRow {
  return {
    as_of_date: '2026-07-15',
    local_code: '0000',
    sector17_code: null,
    sector17_name: null,
    market_cap: 1_000_000,
    per: 15,
    pbr: 1.5,
    dividend_yield: 2.0,
    roe: 10,
    value_pct: 50,
    quality_pct: 50,
    momentum_pct: 50,
    dev_25: 1,
    dev_200: 1,
    rsi_14: 50,
    atr_pct: 2,
    vol_ratio_20: 1,
    ...overrides,
  };
}

describe('shouldExcludeStock', () => {
  it('market_capがnullなら除外', () => {
    expect(shouldExcludeStock(row({ market_cap: null }))).toBe(true);
  });

  it('perとpbrが両方nullなら除外', () => {
    expect(shouldExcludeStock(row({ per: null, pbr: null }))).toBe(true);
  });

  it('perかpbrのどちらか一方があれば非除外', () => {
    expect(shouldExcludeStock(row({ per: null, pbr: 1.5 }))).toBe(false);
    expect(shouldExcludeStock(row({ per: 15, pbr: null }))).toBe(false);
  });

  it('market_cap・per・pbrが揃っていれば非除外', () => {
    expect(shouldExcludeStock(row())).toBe(false);
  });
});

describe('deriveRawDimensions', () => {
  it('per>0はearnings_yield=1/per、per<=0/nullはnull', () => {
    expect(deriveRawDimensions(row({ per: 10 }))[0]).toBeCloseTo(0.1, 10);
    expect(deriveRawDimensions(row({ per: 0 }))[0]).toBeNull();
    expect(deriveRawDimensions(row({ per: -5 }))[0]).toBeNull();
    expect(deriveRawDimensions(row({ per: null }))[0]).toBeNull();
  });

  it('pbr>0はlog_pbr=ln(pbr)、pbr<=0/nullはnull', () => {
    expect(deriveRawDimensions(row({ pbr: 1 }))[1]).toBeCloseTo(0, 10);
    expect(deriveRawDimensions(row({ pbr: 0 }))[1]).toBeNull();
    expect(deriveRawDimensions(row({ pbr: -1 }))[1]).toBeNull();
  });

  it('market_cap>0はlog_mcap=ln(market_cap)', () => {
    const d = deriveRawDimensions(row({ market_cap: Math.E }));
    expect(d[4]).toBeCloseTo(1, 10);
  });

  it('raw列（dividend_yield等）はそのまま転記', () => {
    const d = deriveRawDimensions(row({ dividend_yield: 3.3, roe: 12.2, dev_25: -1.1 }));
    expect(d[2]).toBe(3.3);
    expect(d[3]).toBe(12.2);
    expect(d[8]).toBe(-1.1);
  });

  it('13要素を返す', () => {
    expect(deriveRawDimensions(row())).toHaveLength(13);
  });
});

describe('countCoverage', () => {
  it('非NULL数を数える', () => {
    expect(countCoverage([1, null, 2, null, 3])).toBe(3);
    expect(countCoverage(new Array(13).fill(null))).toBe(0);
    expect(countCoverage(new Array(13).fill(1))).toBe(13);
  });
});

describe('percentile', () => {
  it('空配列はNaN', () => {
    expect(Number.isNaN(percentile([], 0.5))).toBe(true);
  });

  it('1件のみはその値', () => {
    expect(percentile([42], 0.01)).toBe(42);
    expect(percentile([42], 0.99)).toBe(42);
  });

  it('中央値（奇数件）', () => {
    expect(percentile([1, 2, 3, 4, 5], 0.5)).toBe(3);
  });

  it('線形補間（偶数件）', () => {
    expect(percentile([1, 2, 3, 4], 0.5)).toBeCloseTo(2.5, 10);
  });
});

describe('winsorize', () => {
  it('全NULLはそのまま', () => {
    expect(winsorize([null, null, null])).toEqual([null, null, null]);
  });

  it('外れ値をp1/p99にクリップする', () => {
    // 1..100 の一様分布に極端な外れ値を混ぜる
    const values = Array.from({ length: 100 }, (_, i) => i + 1);
    values.push(100000); // 明確な外れ値
    const result = winsorize(values as (number | null)[]);
    const nonNull = result.filter((v): v is number => v != null);
    expect(Math.max(...nonNull)).toBeLessThan(100000);
  });

  it('NULLは変換されずNULLのまま', () => {
    const result = winsorize([1, null, 2, null, 3]);
    expect(result[1]).toBeNull();
    expect(result[3]).toBeNull();
  });

  it('1件のみ非NULLなら変化しない', () => {
    expect(winsorize([null, 5, null])).toEqual([null, 5, null]);
  });
});

describe('zScore', () => {
  it('全NULLはそのまま', () => {
    expect(zScore([null, null])).toEqual([null, null]);
  });

  it('std=0（全値同一）なら非NULL値は全て0', () => {
    expect(zScore([5, 5, 5])).toEqual([0, 0, 0]);
  });

  it('std=0（1件のみ非NULL）なら0', () => {
    expect(zScore([null, 7, null])).toEqual([null, 0, null]);
  });

  it('標準的なz-score計算（母集団std）', () => {
    // [1,2,3] mean=2, population variance=(1+0+1)/3=2/3, std=sqrt(2/3)
    const std = Math.sqrt(2 / 3);
    const result = zScore([1, 2, 3]);
    expect(result[0]).toBeCloseTo((1 - 2) / std, 10);
    expect(result[1]).toBeCloseTo(0, 10);
    expect(result[2]).toBeCloseTo((3 - 2) / std, 10);
  });

  it('NULLはNULLのまま', () => {
    const result = zScore([1, null, 3]);
    expect(result[1]).toBeNull();
  });
});

describe('clamp', () => {
  it('±limitにクランプする', () => {
    expect(clamp([-5, -3, 0, 3, 5], 3)).toEqual([-3, -3, 0, 3, 3]);
  });

  it('NULLはそのまま', () => {
    expect(clamp([null, 10], 3)).toEqual([null, 3]);
  });
});

describe('fillNullWithZero', () => {
  it('NULLを既定0で補完', () => {
    expect(fillNullWithZero([1, null, 3])).toEqual([1, 0, 3]);
  });

  it('fillValueを指定可能', () => {
    expect(fillNullWithZero([null], -1)).toEqual([-1]);
  });
});

describe('applyWeight', () => {
  it('全要素に重みを乗算', () => {
    expect(applyWeight([1, 2, 3], 0.5)).toEqual([0.5, 1, 1.5]);
  });
});

describe('transformColumn', () => {
  it('winsorize→z-score→clamp→NULL補完→重みを通しで実行する', () => {
    const column = [1, 2, 3, null, 4, 5];
    const result = transformColumn(column, 1.0);
    expect(result).toHaveLength(6);
    expect(result[3]).toBe(0); // NULLは0埋め
    expect(Number.isFinite(result[0])).toBe(true);
  });

  it('重み0.5がz-score後に乗算される', () => {
    const column = [1, 2, 3];
    const weighted = transformColumn(column, 0.5);
    const unweighted = transformColumn(column, 1.0);
    expect(weighted[0]).toBeCloseTo(unweighted[0] * 0.5, 10);
  });

  it('全NULL列はz-score/clamp/補完を経て全て0になる', () => {
    const result = transformColumn([null, null, null], 1.0);
    expect(result).toEqual([0, 0, 0]);
  });
});

describe('buildVectors', () => {
  it('各行に13次元ベクトルを割り当てる', () => {
    const rows = [row({ local_code: 'A' }), row({ local_code: 'B', per: 30 })];
    const { vectors, coverage, rawDimensions } = buildVectors(rows);
    expect(vectors).toHaveLength(2);
    expect(vectors[0]).toHaveLength(13);
    expect(coverage).toHaveLength(2);
    expect(rawDimensions).toHaveLength(2);
  });

  it('1銘柄のみでもエラーにならない（std=0で全次元0）', () => {
    const rows = [row()];
    const { vectors } = buildVectors(rows);
    expect(vectors[0]).toEqual(new Array(13).fill(0));
  });

  it('coverageは生の派生値の非NULL数と一致する', () => {
    const rows = [row({ per: null, pbr: 1.5, dev_200: null })];
    const { coverage, rawDimensions } = buildVectors(rows);
    expect(coverage[0]).toBe(countCoverageHelper(rawDimensions[0]));
    expect(coverage[0]).toBe(11); // 13次元 - per(null) - dev_200(null)
  });
});

function countCoverageHelper(dims: (number | null)[]): number {
  return dims.filter((v) => v != null).length;
}

describe('formatEmbeddingLiteral', () => {
  it('pgvectorリテラル形式の文字列を生成する', () => {
    expect(formatEmbeddingLiteral([0.123456789, -1, 2])).toBe('[0.123457,-1,2]');
  });

  it('precisionを指定可能', () => {
    expect(formatEmbeddingLiteral([1.23456], 2)).toBe('[1.23]');
  });
});

describe('cosineSimilarity', () => {
  it('同一ベクトルは類似度1', () => {
    expect(cosineSimilarity([1, 2, 3], [1, 2, 3])).toBeCloseTo(1, 10);
  });

  it('直交ベクトルは類似度0', () => {
    expect(cosineSimilarity([1, 0], [0, 1])).toBeCloseTo(0, 10);
  });

  it('逆向きベクトルは類似度-1', () => {
    expect(cosineSimilarity([1, 2], [-1, -2])).toBeCloseTo(-1, 10);
  });

  it('ゼロベクトルは0（0除算回避）', () => {
    expect(cosineSimilarity([0, 0], [1, 1])).toBe(0);
  });

  it('次元不一致はエラー', () => {
    expect(() => cosineSimilarity([1, 2], [1, 2, 3])).toThrow();
  });
});

describe('distributionStats', () => {
  it('非NULL値のmin/max/meanを計算', () => {
    const stats = distributionStats([1, null, 2, 3, null]);
    expect(stats.count).toBe(3);
    expect(stats.min).toBe(1);
    expect(stats.max).toBe(3);
    expect(stats.mean).toBeCloseTo(2, 10);
  });

  it('全NULLはcount=0・その他null', () => {
    const stats = distributionStats([null, null]);
    expect(stats).toEqual({ count: 0, min: null, max: null, mean: null });
  });
});
