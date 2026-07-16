/**
 * nikkei-official-client.ts のユニットテスト
 *
 * 公式CSV（Shift_JIS・「データ日付,終値,始値,高値,安値」= 終値が先頭）のパースを検証する。
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

import { parseNikkeiOfficialCsv } from '@/lib/market/nikkei-official-client';

// 実レスポンス相当（ヘッダはShift_JIS化けを想定した非日付行・末尾に注意書き行）
const SAMPLE = [
  '�@�[�^���t,�I�l,�n�l,���l,���l',
  '"2023/01/04","25716.86","25834.93","25840.68","25661.89"',
  '"2023/01/05","25820.80","25825.50","25947.10","25750.46"',
  '"本資料は日経の著作物であり、複製・転載はできません。"',
].join('\r\n');

describe('parseNikkeiOfficialCsv', () => {
  it('終値,始値,高値,安値の列順を DailyBar(close/open/high/low) に正しくマップする', () => {
    const bars = parseNikkeiOfficialCsv(SAMPLE);
    expect(bars).toHaveLength(2);
    expect(bars[0]).toEqual({
      date: '2023-01-04',
      close: 25716.86,
      open: 25834.93,
      high: 25840.68,
      low: 25661.89,
    });
  });

  it('ヘッダ行・注意書き行など日付として解釈できない行を読み飛ばす', () => {
    const bars = parseNikkeiOfficialCsv(SAMPLE);
    expect(bars.map((b) => b.date)).toEqual(['2023-01-04', '2023-01-05']);
  });

  it('closeが値域外（5000未満/200000超）の行は行ごと捨てる', () => {
    const bad = '"2023/01/06","100.00","25000.00","25100.00","24900.00"';
    expect(parseNikkeiOfficialCsv(bad)).toHaveLength(0);
  });

  it('open/high/lowのみ値域外の行はその列だけnull化しcloseは採用する', () => {
    const partial = '"2023/01/06","25000.00","100.00","25100.00","24900.00"';
    const bars = parseNikkeiOfficialCsv(partial);
    expect(bars).toHaveLength(1);
    expect(bars[0].close).toBe(25000);
    expect(bars[0].open).toBeNull();
    expect(bars[0].high).toBe(25100);
    expect(bars[0].low).toBe(24900);
  });

  it('high < low の矛盾行は OHLC を null 化して close のみ採用する', () => {
    const inverted = '"2023/01/06","25000.00","25050.00","24900.00","25100.00"';
    const bars = parseNikkeiOfficialCsv(inverted);
    expect(bars).toHaveLength(1);
    expect(bars[0].close).toBe(25000);
    expect(bars[0].open).toBeNull();
    expect(bars[0].high).toBeNull();
    expect(bars[0].low).toBeNull();
  });

  it('同一日付の重複は後勝ち・結果は日付昇順', () => {
    const dup = [
      '"2023/01/05","25820.80","25825.50","25947.10","25750.46"',
      '"2023/01/04","25716.86","25834.93","25840.68","25661.89"',
      '"2023/01/05","25900.00","25825.50","25947.10","25750.46"',
    ].join('\n');
    const bars = parseNikkeiOfficialCsv(dup);
    expect(bars.map((b) => b.date)).toEqual(['2023-01-04', '2023-01-05']);
    expect(bars[1].close).toBe(25900);
  });

  it('LF改行のみでもパースできる', () => {
    const lf = SAMPLE.replace(/\r\n/g, '\n');
    expect(parseNikkeiOfficialCsv(lf)).toHaveLength(2);
  });
});
