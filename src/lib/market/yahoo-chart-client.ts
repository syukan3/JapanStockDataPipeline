/**
 * Yahoo Finance chart API クライアント（日経平均 ^N225 日足取得）
 *
 * @description
 * J-Quants Light は日経225指数を配信しない（TOPIXのみ）ため、
 * 日経平均の日次終値は Yahoo Finance chart API から取得する。
 * - query2 ホストを使用（query1 はレート制限が厳しい実測結果）
 * - ブラウザ相当の User-Agent を送る（無UAはブロックされる）
 * - 日次バッチで1リクエスト想定の低頻度アクセス
 */

import { fetchWithRetry } from '../utils/retry';
import { RateLimiter } from '../jquants/rate-limiter';
import { createLogger } from '../utils/logger';

const logger = createLogger({ module: 'yahoo-chart-client' });

const CHART_URL = 'https://query2.finance.yahoo.com/v8/finance/chart/%5EN225';

export const BROWSER_USER_AGENT =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36';

/** Yahoo向けレート制限（保守的に 10 req/min・最小間隔2s） */
let rateLimiter: RateLimiter | null = null;
function getRateLimiter(): RateLimiter {
  if (!rateLimiter) {
    rateLimiter = new RateLimiter({ requestsPerMinute: 10, minIntervalMs: 2000 });
  }
  return rateLimiter;
}

export interface DailyClose {
  /** JST の日付 (YYYY-MM-DD) */
  date: string;
  close: number;
}

/** epoch秒 → JST日付文字列 (YYYY-MM-DD) */
export function epochToJstDate(epochSeconds: number): string {
  const formatter = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return formatter.format(new Date(epochSeconds * 1000));
}

/** YYYY-MM-DD (JST) → epoch秒（JST 00:00 時点） */
export function jstDateToEpoch(date: string): number {
  return Math.floor(new Date(`${date}T00:00:00+09:00`).getTime() / 1000);
}

interface YahooChartResponse {
  chart?: {
    result?: Array<{
      timestamp?: number[];
      indicators?: { quote?: Array<{ close?: Array<number | null> }> };
      meta?: { currency?: string; instrumentType?: string };
    }>;
    error?: { code?: string; description?: string } | null;
  };
}

/**
 * Yahoo chart レスポンスをパースして日次終値の配列に変換（純関数・テスト対象）
 *
 * @throws レスポンス構造が期待と異なる場合（列マッピング破壊の検知）
 */
export function parseYahooChart(json: unknown): DailyClose[] {
  const body = json as YahooChartResponse;
  const result = body?.chart?.result?.[0];
  if (!result) {
    const err = body?.chart?.error;
    throw new Error(
      `Yahoo chart: unexpected response shape${err ? ` (${err.code}: ${err.description})` : ''}`
    );
  }
  const timestamps = result.timestamp ?? [];
  const closes = result.indicators?.quote?.[0]?.close ?? [];
  if (timestamps.length !== closes.length) {
    throw new Error(
      `Yahoo chart: timestamp/close length mismatch (${timestamps.length} vs ${closes.length})`
    );
  }
  const out: DailyClose[] = [];
  for (let i = 0; i < timestamps.length; i++) {
    const close = closes[i];
    if (close == null || !Number.isFinite(close)) continue;
    out.push({ date: epochToJstDate(timestamps[i]), close: Number(close.toFixed(2)) });
  }
  // 同一JST日付が複数来た場合は後勝ち（場中スナップショットが混ざるケース）
  const byDate = new Map<string, DailyClose>();
  for (const row of out) byDate.set(row.date, row);
  return Array.from(byDate.values()).sort((a, b) => a.date.localeCompare(b.date));
}

/**
 * 日経平均の日次終値を取得
 *
 * @param from YYYY-MM-DD（この日を含む）
 * @param to   YYYY-MM-DD（この日を含む）
 */
export async function fetchNikkeiDailyCloses(from: string, to: string): Promise<DailyClose[]> {
  await getRateLimiter().acquire();
  const period1 = jstDateToEpoch(from);
  // to の翌日 00:00 JST まで（toの日中バーを含めるため）
  const period2 = jstDateToEpoch(to) + 24 * 3600;
  const url = `${CHART_URL}?period1=${period1}&period2=${period2}&interval=1d`;
  logger.info('Fetching Yahoo chart', { from, to });
  const res = await fetchWithRetry(
    url,
    { headers: { 'User-Agent': BROWSER_USER_AGENT, Accept: 'application/json' } },
    { maxRetries: 3, baseDelayMs: 2000 }
  );
  const json = (await res.json()) as unknown;
  const rows = parseYahooChart(json).filter((r) => r.date >= from && r.date <= to);
  logger.info('Yahoo chart fetched', { rows: rows.length });
  return rows;
}
