/**
 * app/api/proxy/yahoo-chart/route.ts のテスト
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  requireCronAuth: vi.fn(),
}));

vi.mock('@/lib/cron/auth', () => ({
  requireCronAuth: mocks.requireCronAuth,
}));

vi.mock('@/lib/utils/logger', () => ({
  createLogger: vi.fn(() => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() })),
}));

import { GET } from '@/app/api/proxy/yahoo-chart/route';

const fetchMock = vi.fn();

function makeRequest(query: string): Request {
  return new Request(`https://example.com/api/proxy/yahoo-chart${query}`, {
    headers: { Authorization: 'Bearer test-secret' },
  });
}

describe('GET /api/proxy/yahoo-chart', () => {
  beforeEach(() => {
    mocks.requireCronAuth.mockReturnValue(null);
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('認証に失敗した場合は上流を叩かない', async () => {
    mocks.requireCronAuth.mockReturnValue(new Response(null, { status: 401 }));
    const res = await GET(makeRequest('?symbol=5016.T&period1=1788188400&period2=1790780400'));
    expect(res.status).toBe(401);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it.each([
    ['?symbol=%5EN225&period1=1788188400&period2=1790780400', '東証以外のシンボル'],
    ['?symbol=AAPL&period1=1788188400&period2=1790780400', '米国株'],
    ['?symbol=5016.T&period1=abc&period2=1790780400', 'period が数値でない'],
    ['?symbol=5016.T&period1=1790780400&period2=1788188400', 'period1 >= period2'],
    ['?symbol=5016.T%2F..%2Fx&period1=1788188400&period2=1790780400', 'パスの持ち込み'],
  ])('%s（%s）は 400 で拒否し上流を叩かない', async (query) => {
    const res = await GET(makeRequest(query));
    expect(res.status).toBe(400);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('固定のクエリで上流を叩き、本文をそのまま返す', async () => {
    fetchMock.mockResolvedValue(new Response('{"chart":{}}', { status: 200 }));
    const res = await GET(makeRequest('?symbol=200A.T&period1=1788188400&period2=1790780400'));
    expect(res.status).toBe(200);
    expect(await res.text()).toBe('{"chart":{}}');
    expect(fetchMock.mock.calls[0][0]).toBe(
      'https://query2.finance.yahoo.com/v8/finance/chart/200A.T?period1=1788188400&period2=1790780400&interval=1d&events=split'
    );
  });

  it('上流の非 2xx はステータスをそのまま返す（429 のリトライ判定を直接取得と揃える）', async () => {
    fetchMock.mockResolvedValue(new Response('', { status: 429 }));
    const res = await GET(makeRequest('?symbol=1306.T&period1=1788188400&period2=1790780400'));
    expect(res.status).toBe(429);
  });
});
