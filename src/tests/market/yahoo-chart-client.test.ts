/**
 * market/yahoo-chart-client.ts のユニットテスト
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
  parseYahooChart,
  epochToJstDate,
  jstDateToEpoch,
  fetchNikkeiDailyCloses,
} from '@/lib/market/yahoo-chart-client';

/** 2026-07-06 09:00 JST（Yahoo日足のtimestampは場開始時刻） */
const T_20260706 = Math.floor(new Date('2026-07-06T09:00:00+09:00').getTime() / 1000);
const T_20260703 = Math.floor(new Date('2026-07-03T09:00:00+09:00').getTime() / 1000);

function chartResponse(timestamps: number[], closes: Array<number | null>): unknown {
  return {
    chart: {
      result: [
        {
          timestamp: timestamps,
          indicators: { quote: [{ close: closes }] },
          meta: { currency: 'JPY', instrumentType: 'INDEX' },
        },
      ],
      error: null,
    },
  };
}

describe('epoch変換', () => {
  it('epochToJstDate: 場中タイムスタンプをJST日付へ', () => {
    expect(epochToJstDate(T_20260706)).toBe('2026-07-06');
  });
  it('jstDateToEpoch: JST 00:00 の epoch秒', () => {
    expect(epochToJstDate(jstDateToEpoch('2026-07-06'))).toBe('2026-07-06');
  });
});

describe('parseYahooChart', () => {
  it('timestamp×close を日付昇順の終値配列へ', () => {
    const rows = parseYahooChart(chartResponse([T_20260703, T_20260706], [69744.07, 69737.69]));
    expect(rows).toEqual([
      { date: '2026-07-03', close: 69744.07 },
      { date: '2026-07-06', close: 69737.69 },
    ]);
  });

  it('close が null の日は除外', () => {
    const rows = parseYahooChart(chartResponse([T_20260703, T_20260706], [null, 69737.69]));
    expect(rows).toEqual([{ date: '2026-07-06', close: 69737.69 }]);
  });

  it('result が無い場合は throw（エラー内容を含む）', () => {
    expect(() =>
      parseYahooChart({ chart: { result: null, error: { code: 'Not Found', description: 'x' } } })
    ).toThrow(/Not Found/);
  });

  it('timestamp と close の長さ不一致は throw', () => {
    expect(() => parseYahooChart(chartResponse([T_20260706], [1, 2]))).toThrow(/length mismatch/);
  });

  it('同一JST日付の重複は後勝ちで1本化', () => {
    const rows = parseYahooChart(
      chartResponse([T_20260706, T_20260706 + 3600], [69000, 69737.69])
    );
    expect(rows).toEqual([{ date: '2026-07-06', close: 69737.69 }]);
  });
});

describe('fetchNikkeiDailyCloses', () => {
  beforeEach(() => {
    stableAcquire.mockResolvedValue(undefined);
  });

  it('期間を指定して取得し、範囲外の日付を除外する', async () => {
    mockFetchWithRetry.mockResolvedValue({
      json: () =>
        Promise.resolve(chartResponse([T_20260703, T_20260706], [69744.07, 69737.69])),
    });
    const rows = await fetchNikkeiDailyCloses('2026-07-06', '2026-07-06');
    expect(stableAcquire).toHaveBeenCalled();
    expect(rows).toEqual([{ date: '2026-07-06', close: 69737.69 }]);
    const [url, init] = mockFetchWithRetry.mock.calls[0];
    expect(url).toContain('query2.finance.yahoo.com');
    expect(url).toContain('%5EN225');
    expect(url).toContain('interval=1d');
    expect(init.headers['User-Agent']).toContain('Mozilla');
  });
});
