/**
 * mof/client.ts のユニットテスト
 */

import { describe, it, expect, vi, afterEach } from 'vitest';
import {
  parseEraDate,
  parseMofJgbCsv,
  tenorForSourceSeriesId,
  releasedAtForJgbDate,
  createMofClient,
  TENOR_COLUMNS,
} from '@/lib/mof/client';

describe('releasedAtForJgbDate', () => {
  it('観測日(indicator_date)ベースのタイムスタンプを返す（取得時刻ではない）', () => {
    expect(releasedAtForJgbDate('2026-07-07')).toBe('2026-07-07T15:00:00+09:00');
  });
});

describe('parseEraDate', () => {
  it('令和を西暦に変換する', () => {
    expect(parseEraDate('R8.7.7')).toBe('2026-07-07');
    expect(parseEraDate('R1.5.1')).toBe('2019-05-01');
  });

  it('平成を西暦に変換する', () => {
    expect(parseEraDate('H31.4.1')).toBe('2019-04-01');
    expect(parseEraDate('H1.1.8')).toBe('1989-01-08');
  });

  it('昭和を西暦に変換する', () => {
    expect(parseEraDate('S64.1.7')).toBe('1989-01-07');
  });

  it('月日は2桁ゼロパディングされる', () => {
    expect(parseEraDate('R8.1.5')).toBe('2026-01-05');
  });

  it('不正な形式は null を返す', () => {
    expect(parseEraDate('2026-07-07')).toBeNull();
    expect(parseEraDate('')).toBeNull();
    expect(parseEraDate('X8.7.7')).toBeNull();
  });
});

describe('tenorForSourceSeriesId', () => {
  it('登録済みの year_id を年限ラベルに変換する', () => {
    expect(tenorForSourceSeriesId('jgbcm_20y')).toBe('20年');
    expect(tenorForSourceSeriesId('jgbcm_30y')).toBe('30年');
  });

  it('未知の source_series_id は null', () => {
    expect(tenorForSourceSeriesId('unknown')).toBeNull();
  });
});

describe('parseMofJgbCsv', () => {
  function buildCsv(rows: string[]): Uint8Array {
    const header1 = '国債金利情報';
    const header2 = `基準日,${TENOR_COLUMNS.join(',')}`;
    const text = [header1, header2, ...rows].join('\n');
    return new TextEncoder().encode(text);
  }

  it('データ行を日付+年限別利回りにパースする', () => {
    // 15列（1年〜40年）分の値。20年=1.500, 30年=2.800 の位置に置く
    const values = TENOR_COLUMNS.map((_, i) => (0.5 + i * 0.1).toFixed(3));
    const row = `R8.7.7,${values.join(',')}`;
    const rows = parseMofJgbCsv(buildCsv([row]));

    expect(rows).toHaveLength(1);
    expect(rows[0].date).toBe('2026-07-07');
    expect(rows[0].tenors['20年']).toBeCloseTo(Number(values[TENOR_COLUMNS.indexOf('20年')]));
    expect(rows[0].tenors['30年']).toBeCloseTo(Number(values[TENOR_COLUMNS.indexOf('30年')]));
  });

  it('未提供年限（-）は null として保持する（0への誤変換をしない）', () => {
    const values = TENOR_COLUMNS.map(() => '-');
    const row = `R8.7.7,${values.join(',')}`;
    const rows = parseMofJgbCsv(buildCsv([row]));

    expect(rows[0].tenors['20年']).toBeNull();
    expect(rows[0].tenors['30年']).toBeNull();
  });

  it('日付が和暦形式でない行は無視する', () => {
    const values = TENOR_COLUMNS.map(() => '1.0');
    const rows = parseMofJgbCsv(buildCsv([`invalid,${values.join(',')}`]));
    expect(rows).toHaveLength(0);
  });

  it('空行は無視する', () => {
    const values = TENOR_COLUMNS.map(() => '1.0');
    const row = `R8.7.7,${values.join(',')}`;
    const rows = parseMofJgbCsv(buildCsv([row, '', '   ']));
    expect(rows).toHaveLength(1);
  });
});

describe('createMofClient', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('CSVを取得してパース結果を返す', async () => {
    const values = TENOR_COLUMNS.map(() => '1.234');
    const csvText = ['タイトル', `基準日,${TENOR_COLUMNS.join(',')}`, `R8.7.7,${values.join(',')}`].join('\n');
    const buf = new TextEncoder().encode(csvText).buffer;

    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        arrayBuffer: async () => buf,
      })
    );

    const client = createMofClient();
    const curve = await client.getJgbCurve();

    expect(curve).toHaveLength(1);
    expect(curve[0].date).toBe('2026-07-07');
  });

  it('HTTPエラー時は例外を投げる', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ok: false, status: 503 })
    );

    const client = createMofClient();
    await expect(client.getJgbCurve()).rejects.toThrow('503');
  });
});
