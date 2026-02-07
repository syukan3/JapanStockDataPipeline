import { describe, it, expect, vi, beforeEach } from 'vitest';
import { KabuStationClient } from '../../lib/yutai/kabu-station-client';

// fetch をモック
const mockFetch = vi.fn();
vi.stubGlobal('fetch', mockFetch);

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

describe('KabuStationClient', () => {
  let client: KabuStationClient;

  beforeEach(() => {
    client = new KabuStationClient({
      apiPassword: 'test-password',
      baseUrl: 'http://localhost:18080/kabusapi',
      timeout: 5000,
    });
    mockFetch.mockReset();
  });

  describe('authenticate', () => {
    it('正常認証 → トークン取得', async () => {
      mockFetch.mockResolvedValueOnce(
        jsonResponse({ ResultCode: 0, Token: 'test-token-123' }),
      );

      const token = await client.authenticate();
      expect(token).toBe('test-token-123');
      expect(mockFetch).toHaveBeenCalledWith(
        'http://localhost:18080/kabusapi/token',
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({ APIPassword: 'test-password' }),
        }),
      );
    });

    it('認証失敗（HTTPエラー）→ 例外', async () => {
      mockFetch.mockResolvedValueOnce(new Response('', { status: 500 }));

      await expect(client.authenticate()).rejects.toThrow('Authentication failed: HTTP 500');
    });

    it('認証失敗（ResultCode!=0）→ 例外', async () => {
      mockFetch.mockResolvedValueOnce(
        jsonResponse({ ResultCode: 4, Token: '' }),
      );

      await expect(client.authenticate()).rejects.toThrow('ResultCode=4');
    });
  });

  describe('getMarginPremium', () => {
    const mockPremiumResponse = {
      Symbol: '2914',
      GeneralMargin: {
        MarginPremiumType: 1,
        MarginPremium: 5.5,
        UpperMarginPremium: null,
        LowerMarginPremium: null,
        TickMarginPremium: null,
      },
      SystemMargin: {
        MarginPremiumType: 0,
        MarginPremium: null,
        UpperMarginPremium: null,
        LowerMarginPremium: null,
        TickMarginPremium: null,
      },
    };

    it('正常取得', async () => {
      // 認証
      mockFetch.mockResolvedValueOnce(
        jsonResponse({ ResultCode: 0, Token: 'test-token' }),
      );
      // マージンプレミアム
      mockFetch.mockResolvedValueOnce(jsonResponse(mockPremiumResponse));

      const result = await client.getMarginPremium('2914');
      expect(result.Symbol).toBe('2914');
      expect(result.GeneralMargin.MarginPremium).toBe(5.5);
    });

    it('401エラー → 再認証してリトライ', async () => {
      // 初回認証
      mockFetch.mockResolvedValueOnce(
        jsonResponse({ ResultCode: 0, Token: 'old-token' }),
      );
      // 初回リクエスト → 401
      mockFetch.mockResolvedValueOnce(new Response('', { status: 401 }));
      // 再認証
      mockFetch.mockResolvedValueOnce(
        jsonResponse({ ResultCode: 0, Token: 'new-token' }),
      );
      // リトライ成功
      mockFetch.mockResolvedValueOnce(jsonResponse(mockPremiumResponse));

      const result = await client.getMarginPremium('2914');
      expect(result.Symbol).toBe('2914');
      expect(mockFetch).toHaveBeenCalledTimes(4);
    });

    it('非401エラー → 例外', async () => {
      mockFetch.mockResolvedValueOnce(
        jsonResponse({ ResultCode: 0, Token: 'test-token' }),
      );
      mockFetch.mockResolvedValueOnce(new Response('', { status: 500 }));

      await expect(client.getMarginPremium('2914')).rejects.toThrow('HTTP 500');
    });
  });

  describe('getMarginPremiumBatch', () => {
    it('複数銘柄をバッチ取得', async () => {
      // 認証
      mockFetch.mockResolvedValueOnce(
        jsonResponse({ ResultCode: 0, Token: 'test-token' }),
      );
      // 1件目
      mockFetch.mockResolvedValueOnce(
        jsonResponse({
          Symbol: '2914',
          GeneralMargin: { MarginPremiumType: 1, MarginPremium: 5.5, UpperMarginPremium: null, LowerMarginPremium: null, TickMarginPremium: null },
          SystemMargin: { MarginPremiumType: 0, MarginPremium: null, UpperMarginPremium: null, LowerMarginPremium: null, TickMarginPremium: null },
        }),
      );
      // 2件目
      mockFetch.mockResolvedValueOnce(
        jsonResponse({
          Symbol: '8591',
          GeneralMargin: { MarginPremiumType: 1, MarginPremium: 3.0, UpperMarginPremium: null, LowerMarginPremium: null, TickMarginPremium: null },
          SystemMargin: { MarginPremiumType: 0, MarginPremium: null, UpperMarginPremium: null, LowerMarginPremium: null, TickMarginPremium: null },
        }),
      );

      const results = await client.getMarginPremiumBatch(['2914', '8591']);
      expect(results.size).toBe(2);
    });

    it('一部失敗でも他は成功', async () => {
      mockFetch.mockResolvedValueOnce(
        jsonResponse({ ResultCode: 0, Token: 'test-token' }),
      );
      // 1件目成功
      mockFetch.mockResolvedValueOnce(
        jsonResponse({
          Symbol: '2914',
          GeneralMargin: { MarginPremiumType: 1, MarginPremium: 5.5, UpperMarginPremium: null, LowerMarginPremium: null, TickMarginPremium: null },
          SystemMargin: { MarginPremiumType: 0, MarginPremium: null, UpperMarginPremium: null, LowerMarginPremium: null, TickMarginPremium: null },
        }),
      );
      // 2件目失敗
      mockFetch.mockResolvedValueOnce(new Response('', { status: 500 }));

      const results = await client.getMarginPremiumBatch(['2914', '9999']);
      expect(results.size).toBe(1);
      expect(results.has('2914')).toBe(true);
    });
  });
});
