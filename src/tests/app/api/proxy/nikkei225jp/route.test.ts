/**
 * app/api/proxy/nikkei225jp/route.ts のテスト
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  requireCronAuth: vi.fn(),
}));

vi.mock('@/lib/cron/auth', () => ({
  requireCronAuth: mocks.requireCronAuth,
}));

vi.mock('@/lib/utils/logger', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
}));

import { GET } from '@/app/api/proxy/nikkei225jp/route';

function makeRequest(query: string): Request {
  return new Request(`https://example.com/api/proxy/nikkei225jp${query}`, {
    headers: { Authorization: 'Bearer test-secret' },
  });
}

const fetchMock = vi.fn();

describe('GET /api/proxy/nikkei225jp', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.requireCronAuth.mockReturnValue(null);
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('CRON_SECRET認証に失敗した場合は上流を叩かずエラーを返す', async () => {
    mocks.requireCronAuth.mockReturnValue(new Response(null, { status: 401 }));

    const response = await GET(makeRequest('?file=daily2'));

    expect(response.status).toBe(401);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it.each([
    ['?file=evil', '許可外の値'],
    ['', 'file未指定'],
    ['?file=https://attacker.example.com/x', '任意URLの持ち込み'],
  ])('%s (%s) は400で拒否し上流を叩かない', async (query) => {
    const response = await GET(makeRequest(query));

    expect(response.status).toBe(400);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('daily2 は許可URLをReferer付きで取得し本文をそのまま返す', async () => {
    fetchMock.mockResolvedValue(new Response('var DAILY = [[1,2]];', { status: 200 }));

    const response = await GET(makeRequest('?file=daily2'));

    expect(response.status).toBe(200);
    expect(await response.text()).toBe('var DAILY = [[1,2]];');
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('https://nikkei225jp.com/_data/_nfsDATA/DAY/daily2.json');
    const headers = init.headers as Record<string, string>;
    expect(headers.Referer).toBe('https://nikkei225jp.com/data/sinyou.php');
    expect(headers['User-Agent']).toContain('Mozilla/5.0');
  });

  it('dailyweek2 は週次側の許可URLを取得する', async () => {
    fetchMock.mockResolvedValue(new Response('var DAILYWEEK = [];', { status: 200 }));

    await GET(makeRequest('?file=dailyweek2'));

    expect(fetchMock.mock.calls[0][0]).toBe(
      'https://nikkei225jp.com/_data/_nfsDATA/DAY/dailyweek2.json'
    );
  });

  // 上流の403を200に丸めると呼び出し側(fetchWithRetry)のリトライ判定が
  // 直接取得時と変わってしまうため、ステータスはそのまま通す。
  it('上流が403なら403をそのまま返す', async () => {
    fetchMock.mockResolvedValue(new Response('forbidden', { status: 403 }));

    const response = await GET(makeRequest('?file=dailyweek2'));

    expect(response.status).toBe(403);
  });

  it('上流への接続自体が失敗した場合は502を返す', async () => {
    fetchMock.mockRejectedValue(new Error('timeout'));

    const response = await GET(makeRequest('?file=daily2'));

    expect(response.status).toBe(502);
  });
});
