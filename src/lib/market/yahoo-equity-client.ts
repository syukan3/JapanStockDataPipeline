/**
 * Yahoo Finance chart API クライアント（東証の個別銘柄・ETF の日足と分割イベント）
 *
 * @description
 * J-Quants OFF 中に、保有・ウォッチ銘柄と固定ETFの日足を取る。
 * 設計正本: ../../../../docs/PLANS-jquants-off-switch-2026-10.md §4.2
 *
 * 値の意味（2026-10-01 実測）:
 * - `indicators.quote` の OHLC は**取得時点で分割調整済み・配当未調整**。2026-09-29 に分割した
 *   1925（1→2）・1904（1→4）で J-Quants の adj_close と5日すべて一致した。
 * - `indicators.adjclose` は分割＋配当調整で別物（使わない）。
 * - `events.splits` に ex-date と比率（numerator/denominator）が入る。
 *
 * 取得経路: 既定は query2 へ直接。YAHOO_PROXY_BASE_URL と CRON_SECRET が揃っていれば
 * 東京リージョン固定の自前ルート（/api/proxy/yahoo-chart）経由（nikkei225jp と同じ作り）。
 */

import { fetchWithRetry } from '../utils/retry';
import { RateLimiter } from '../jquants/rate-limiter';
import { createLogger } from '../utils/logger';
import { BROWSER_USER_AGENT, epochToJstDate, jstDateToEpoch } from './yahoo-chart-client';

const logger = createLogger({ module: 'yahoo-equity-client' });

const CHART_BASE = 'https://query2.finance.yahoo.com/v8/finance/chart/';

/** 東証銘柄の Yahoo シンボル（例: 5016.T・200A.T） */
export const YAHOO_TSE_SYMBOL_RE = /^[0-9]{3}[0-9A-Z]\.T$/;

/** 日足（調整値＝Yahoo の quote そのもの） */
export interface YahooEquityBar {
  /** JST の日付 */
  date: string;
  adjOpen: number | null;
  adjHigh: number | null;
  adjLow: number | null;
  adjClose: number;
  adjVolume: number | null;
}

/** 分割・併合イベント（1株→ratio株） */
export interface YahooSplitEvent {
  exDate: string;
  ratio: number;
}

export interface YahooEquityChart {
  bars: YahooEquityBar[];
  splits: YahooSplitEvent[];
}

/**
 * J-Quants の5文字コード → Yahoo シンボル。5文字目が 0 でないコード（優先株等）は対応なし（null）
 */
export function toYahooSymbol(localCode: string): string | null {
  if (!/^[0-9]{3}[0-9A-Z][0-9]$/.test(localCode)) return null;
  if (!localCode.endsWith('0')) return null;
  return `${localCode.slice(0, 4)}.T`;
}

/** 少し緩めのレート制限（1日1回・十数シンボル） */
let rateLimiter: RateLimiter | null = null;
function getRateLimiter(): RateLimiter {
  if (!rateLimiter) {
    rateLimiter = new RateLimiter({ requestsPerMinute: 20, minIntervalMs: 1500 });
  }
  return rateLimiter;
}

interface ChartJson {
  chart?: {
    result?: Array<{
      timestamp?: number[];
      indicators?: {
        quote?: Array<{
          open?: Array<number | null>;
          high?: Array<number | null>;
          low?: Array<number | null>;
          close?: Array<number | null>;
          volume?: Array<number | null>;
        }>;
      };
      events?: {
        splits?: Record<string, { date?: number; numerator?: number; denominator?: number }>;
      };
    }>;
    error?: { code?: string; description?: string } | null;
  };
}

function num(arr: Array<number | null> | undefined, i: number, digits: number | null): number | null {
  const v = arr?.[i];
  if (v == null || !Number.isFinite(v)) return null;
  return digits == null ? Math.round(v) : Number(v.toFixed(digits));
}

/**
 * chart レスポンスを日足と分割イベントに変換する（純関数・テスト対象）
 *
 * 価格は小数2桁に丸める（Yahoo は 424.79998779296875 のような float を返す）。
 * close が null の日は除外。同じ JST 日付が複数あれば後勝ち。
 */
export function parseYahooEquityChart(json: unknown): YahooEquityChart {
  const body = json as ChartJson;
  const result = body?.chart?.result?.[0];
  if (!result) {
    const err = body?.chart?.error;
    throw new Error(`Yahoo chart: unexpected response shape${err ? ` (${err.code}: ${err.description})` : ''}`);
  }
  const ts = result.timestamp ?? [];
  const q = result.indicators?.quote?.[0];
  const closes = q?.close ?? [];
  if (ts.length !== closes.length) {
    throw new Error(`Yahoo chart: timestamp/close length mismatch (${ts.length} vs ${closes.length})`);
  }

  const byDate = new Map<string, YahooEquityBar>();
  for (let i = 0; i < ts.length; i++) {
    const close = num(closes, i, 2);
    if (close == null) continue;
    const date = epochToJstDate(ts[i]);
    byDate.set(date, {
      date,
      adjOpen: num(q?.open, i, 2),
      adjHigh: num(q?.high, i, 2),
      adjLow: num(q?.low, i, 2),
      adjClose: close,
      adjVolume: num(q?.volume, i, null),
    });
  }

  const splits: YahooSplitEvent[] = [];
  for (const ev of Object.values(result.events?.splits ?? {})) {
    if (ev?.date == null || !ev.numerator || !ev.denominator) continue;
    const ratio = ev.numerator / ev.denominator;
    if (!Number.isFinite(ratio) || ratio <= 0 || ratio === 1) continue;
    splits.push({ exDate: epochToJstDate(ev.date), ratio });
  }

  return {
    bars: Array.from(byDate.values()).sort((a, b) => a.date.localeCompare(b.date)),
    splits: splits.sort((a, b) => a.exDate.localeCompare(b.exDate)),
  };
}

function resolveTarget(symbol: string, query: string): { url: string; headers: Record<string, string> } {
  const base = process.env.YAHOO_PROXY_BASE_URL?.trim().replace(/\/+$/, '');
  const secret = process.env.CRON_SECRET?.trim();
  if (base && secret) {
    return {
      url: `${base}/api/proxy/yahoo-chart?symbol=${encodeURIComponent(symbol)}&${query}`,
      headers: { Authorization: `Bearer ${secret}`, Accept: 'application/json' },
    };
  }
  return {
    url: `${CHART_BASE}${encodeURIComponent(symbol)}?${query}`,
    headers: { 'User-Agent': BROWSER_USER_AGENT, Accept: 'application/json' },
  };
}

/** chart API のクエリ（period は JST 日付。to の日中バーを含めるため翌日 00:00 まで） */
export function buildChartQuery(from: string, to: string): string {
  const period1 = jstDateToEpoch(from);
  const period2 = jstDateToEpoch(to) + 24 * 3600;
  return `period1=${period1}&period2=${period2}&interval=1d&events=split`;
}

/**
 * 1銘柄の日足と分割イベントを取得する
 *
 * @param symbol Yahoo シンボル（YAHOO_TSE_SYMBOL_RE に合うもののみ）
 * @param from YYYY-MM-DD（含む）
 * @param to   YYYY-MM-DD（含む）
 */
export async function fetchYahooEquityChart(symbol: string, from: string, to: string): Promise<YahooEquityChart> {
  if (!YAHOO_TSE_SYMBOL_RE.test(symbol)) {
    throw new Error(`Yahoo シンボルが不正です: ${symbol}`);
  }
  await getRateLimiter().acquire();
  const { url, headers } = resolveTarget(symbol, buildChartQuery(from, to));
  const res = await fetchWithRetry(url, { headers }, { maxRetries: 3, baseDelayMs: 2000 });
  const parsed = parseYahooEquityChart(await res.json());
  const bars = parsed.bars.filter((b) => b.date >= from && b.date <= to);
  logger.debug('Yahoo equity chart fetched', { symbol, rows: bars.length, splits: parsed.splits.length });
  return { bars, splits: parsed.splits.filter((s) => s.exDate >= from && s.exDate <= to) };
}
