/**
 * Yahoo Finance chart 取得プロキシ API Route（東京リージョン固定）
 *
 * @description J-Quants OFF 中の代替株価（scripts/cron/sync-alt-prices.ts）用。
 * GitHub Actions ランナー（米国）から Yahoo へ直接取りに行くと 429 が続くことがある
 * （2026-07 に日経OHLCの一次ソースを切り替えた経緯）。その場合に YAHOO_PROXY_BASE_URL を
 * 設定すると、この東京リージョンのルート経由で取得する。設計正本: docs/PLANS-jquants-off-switch-2026-10.md §4.2
 *
 * セキュリティ（nikkei225jp プロキシと同じ方針）:
 * - 取得先は東証銘柄の日足 chart API だけ。symbol は `^[0-9]{3}[0-9A-Z]\.T$`、period は整数、
 *   interval=1d・events=split は固定。任意 URL を受け取らない（SSRF・オープンプロキシ化を防ぐ）。
 * - CRON_SECRET による Bearer 認証必須。
 *
 * GET /api/proxy/yahoo-chart?symbol=5016.T&period1=<epoch>&period2=<epoch>
 * Headers: Authorization: Bearer <CRON_SECRET>
 */

import { NextResponse } from 'next/server';
import { requireCronAuth } from '@/lib/cron/auth';
import { BROWSER_USER_AGENT } from '@/lib/market/yahoo-chart-client';
import { YAHOO_TSE_SYMBOL_RE } from '@/lib/market/yahoo-equity-client';
import { createLogger } from '@/lib/utils/logger';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const preferredRegion = 'hnd1';
export const maxDuration = 15;

const logger = createLogger({ module: 'route/proxy-yahoo-chart' });

const UPSTREAM_BASE = 'https://query2.finance.yahoo.com/v8/finance/chart/';
const UPSTREAM_TIMEOUT_MS = 10_000;
const EPOCH_RE = /^[0-9]{9,11}$/;

export async function GET(request: Request): Promise<Response> {
  const authError = requireCronAuth(request);
  if (authError) {
    return authError;
  }

  const params = new URL(request.url).searchParams;
  const symbol = params.get('symbol');
  const period1 = params.get('period1');
  const period2 = params.get('period2');
  if (!symbol || !YAHOO_TSE_SYMBOL_RE.test(symbol)) {
    return NextResponse.json({ error: 'symbol must match ^[0-9]{3}[0-9A-Z]\\.T$' }, { status: 400 });
  }
  if (!period1 || !period2 || !EPOCH_RE.test(period1) || !EPOCH_RE.test(period2) || Number(period1) >= Number(period2)) {
    return NextResponse.json({ error: 'period1/period2 must be epoch seconds (period1 < period2)' }, { status: 400 });
  }

  const upstreamUrl = `${UPSTREAM_BASE}${encodeURIComponent(symbol)}?period1=${period1}&period2=${period2}&interval=1d&events=split`;

  let res: Response;
  try {
    res = await fetch(upstreamUrl, {
      headers: { 'User-Agent': BROWSER_USER_AGENT, Accept: 'application/json' },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
      cache: 'no-store',
    });
  } catch (error) {
    logger.error('Upstream fetch failed', { symbol, error });
    return NextResponse.json({ error: 'Upstream fetch failed', symbol }, { status: 502 });
  }

  if (!res.ok) {
    // 上流のステータスをそのまま返す（呼び出し側のリトライ判定を直接取得時と同じにする）
    logger.warn('Upstream returned non-OK', { symbol, status: res.status });
    return NextResponse.json({ error: 'Upstream returned non-OK', symbol, status: res.status }, { status: res.status });
  }

  const body = await res.text();
  return new Response(body, {
    status: 200,
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' },
  });
}
