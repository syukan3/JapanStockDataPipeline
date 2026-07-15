/**
 * market/nikkei225jp-client.ts のユニットテスト
 *
 * 実レスポンス（2026-07-06取得）の構造を模したfixtureで列マッピングを固定する。
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const { stableAcquire, mockFetchWithRetry } = vi.hoisted(() => ({
  stableAcquire: vi.fn().mockResolvedValue(undefined),
  mockFetchWithRetry: vi.fn(),
}));

vi.mock('@/lib/jquants/rate-limiter', () => ({
  RateLimiter: class {
    acquire = stableAcquire;
  },
}));

vi.mock('@/lib/utils/retry', () => ({
  fetchWithRetry: mockFetchWithRetry,
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
  parseVarArrayPayload,
  parseNikkei225jpDaily,
  parseNikkei225jpWeekly,
  epochMsToJstDate,
  fetchNikkei225jpDaily,
  fetchNikkei225jpWeekly,
  DAILY2_EXPECTED_COLS,
  DAILYWEEK2_EXPECTED_COLS,
} from '@/lib/market/nikkei225jp-client';

/** 2026-07-06 JST の epoch ms */
const D_20260706 = new Date('2026-07-06T00:00:00+09:00').getTime();
const D_20260703 = new Date('2026-07-03T00:00:00+09:00').getTime();

/** 実データの列構造（35列）: [epoch, 終値, 売買高, '', '', 値上, 値下, 騰落レシオ, 新高, 新安, 計, 日経VI, PER, PBR, 利回り, ...] */
function daily2Row(overrides: Partial<Record<number, unknown>> = {}): unknown[] {
  const row: unknown[] = [
    D_20260706, 69737.69, 2058, '', '', 1142, 384, 116.37, 106, 3, 1526, 37.36, 18.58, 1.96, 1.49,
    '', 2.83, 162.15, 185.24, 6803897, 63.7, 2888615, 27.1, 982703, 9.2, 25.18, 2.95, 1.28, 18.58,
    1.96, 1.49, '', '', '', '',
  ];
  for (const [k, v] of Object.entries(overrides)) row[Number(k)] = v;
  return row;
}

function daily2Payload(rows: unknown[][]): string {
  return `var DAILY = ${JSON.stringify(rows)};`;
}

describe('parseVarArrayPayload', () => {
  it('var 接頭辞と末尾セミコロンを除去してパース', () => {
    const rows = parseVarArrayPayload('var X = [[1,2],[3,4]];');
    expect(rows).toEqual([
      [1, 2],
      [3, 4],
    ]);
  });
  it('空要素（,, と [,）を null に正規化', () => {
    const rows = parseVarArrayPayload('var X = [[1,,2,,,3,],[,1]];');
    expect(rows).toEqual([
      [1, null, 2, null, null, 3, null],
      [null, 1],
    ]);
  });
  it('配列が見つからない場合は throw', () => {
    expect(() => parseVarArrayPayload('<html>404</html>')).toThrow(/no JSON array/);
  });
});

describe('epochMsToJstDate', () => {
  it('JST 00:00 の epoch を同日に変換（UTC変換だと前日になるケース）', () => {
    expect(epochMsToJstDate(D_20260706)).toBe('2026-07-06');
  });
});

describe('parseNikkei225jpDaily', () => {
  it('実データ相当の列マッピング（終値/PER/日経VI/参照列）', () => {
    const rows = parseNikkei225jpDaily(daily2Payload([daily2Row()]));
    expect(rows).toHaveLength(1);
    const r = rows[0];
    expect(r.date).toBe('2026-07-06');
    expect(r.nikkeiClose).toBe(69737.69);
    expect(r.per).toBe(18.58);
    expect(r.nikkeiVi).toBe(37.36);
    expect(r.refAdvDecRatio).toBe(116.37);
    expect(r.refNewHighs).toBe(106);
    expect(r.refNewLows).toBe(3);
    expect(r.refTurnoverMn).toBe(6803897);
  });

  it('列数が変わったら throw（列マッピング破壊の検知）', () => {
    const short = daily2Row().slice(0, DAILY2_EXPECTED_COLS - 1);
    expect(() => parseNikkei225jpDaily(daily2Payload([short]))).toThrow(/column count changed/);
  });

  it('値域チェック逸脱はその列のみ null（他列は保持）', () => {
    const rows = parseNikkei225jpDaily(daily2Payload([daily2Row({ 12: 999, 11: 37.36 })]));
    expect(rows[0].per).toBeNull(); // PER=999 は値域外
    expect(rows[0].nikkeiVi).toBe(37.36);
    expect(rows[0].nikkeiClose).toBe(69737.69);
  });

  it('日経VIの値域(8〜100): 暴落日の高値を許容し、範囲外はnull化', () => {
    // 2024-08-05 の 70.69（旧short比率レンジ max80 内だが新レンジでも許容）
    expect(parseNikkei225jpDaily(daily2Payload([daily2Row({ 11: 70.69 })]))[0].nikkeiVi).toBe(70.69);
    // 2008-10-31 の過去最高 91.45 は旧max(80)では落ちたが新レンジ(max100)で許容
    expect(parseNikkei225jpDaily(daily2Payload([daily2Row({ 11: 91.45 })]))[0].nikkeiVi).toBe(91.45);
    // 100超・8未満は範囲外としてnull化
    expect(parseNikkei225jpDaily(daily2Payload([daily2Row({ 11: 120 })]))[0].nikkeiVi).toBeNull();
    expect(parseNikkei225jpDaily(daily2Payload([daily2Row({ 11: 5 })]))[0].nikkeiVi).toBeNull();
  });

  it('空文字列の値は null として扱う', () => {
    const rows = parseNikkei225jpDaily(daily2Payload([daily2Row({ 11: '', 12: '' })]));
    expect(rows[0].per).toBeNull();
    expect(rows[0].nikkeiVi).toBeNull();
  });
});

describe('parseNikkei225jpWeekly', () => {
  function weeklyRow(epoch: number, marginPl: unknown): unknown[] {
    const row: unknown[] = new Array(DAILYWEEK2_EXPECTED_COLS).fill('');
    row[0] = epoch;
    row[1] = 69737.69;
    row[7] = marginPl;
    return row;
  }

  it('週次列（col[7]）が非空の行のみ返す', () => {
    const payload = `var DAILY = ${JSON.stringify([
      weeklyRow(D_20260703, -3.09),
      weeklyRow(D_20260706, ''),
    ])};`;
    const rows = parseNikkei225jpWeekly(payload);
    expect(rows).toEqual([{ date: '2026-07-03', marginPlRatio: -3.09 }]);
  });

  it('値域外（信用評価損益率が-50未満等）は除外', () => {
    const payload = `var DAILY = ${JSON.stringify([weeklyRow(D_20260703, -99)])};`;
    expect(parseNikkei225jpWeekly(payload)).toEqual([]);
  });

  it('列数が変わったら throw', () => {
    const payload = `var DAILY = ${JSON.stringify([[D_20260703, 1, 2]])};`;
    expect(() => parseNikkei225jpWeekly(payload)).toThrow(/column count changed/);
  });
});

describe('fetch関数', () => {
  beforeEach(() => {
    stableAcquire.mockResolvedValue(undefined);
  });

  it('fetchNikkei225jpDaily: レート制限を待ってから取得しパースする', async () => {
    mockFetchWithRetry.mockResolvedValue({
      text: () => Promise.resolve(daily2Payload([daily2Row()])),
    });
    const rows = await fetchNikkei225jpDaily();
    expect(stableAcquire).toHaveBeenCalled();
    expect(rows).toHaveLength(1);
    const [url, init] = mockFetchWithRetry.mock.calls[0];
    expect(url).toContain('daily2.json');
    expect(init.headers['User-Agent']).toContain('Mozilla');
  });

  it('fetchNikkei225jpWeekly: Referer ヘッダを必ず送る（無いと404になる）', async () => {
    const row: unknown[] = new Array(DAILYWEEK2_EXPECTED_COLS).fill('');
    row[0] = D_20260703;
    row[7] = -3.09;
    mockFetchWithRetry.mockResolvedValue({
      text: () => Promise.resolve(`var DAILY = ${JSON.stringify([row])};`),
    });
    const rows = await fetchNikkei225jpWeekly();
    expect(rows).toHaveLength(1);
    const [url, init] = mockFetchWithRetry.mock.calls[0];
    expect(url).toContain('dailyweek2.json');
    expect(init.headers.Referer).toContain('nikkei225jp.com');
  });
});
