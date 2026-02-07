/**
 * kabu STATION REST API クライアント
 *
 * @description Windows ローカル PC 上の kabu STATION アプリ経由で
 * eスマート証券の一般信用売り在庫情報を取得する
 *
 * - ベースURL: http://localhost:18080/kabusapi（本番）
 * - 認証: POST /token でトークン取得 → X-API-KEY ヘッダー
 * - レート制限: 約10 req/sec → 8 req/sec で制御
 */

import { createLogger } from '../utils/logger';
import type {
  KabuStationClientOptions,
  TokenResponse,
  MarginPremiumResponse,
} from './kabu-station-types';

const logger = createLogger({ module: 'kabu-station-client' });

const DEFAULT_BASE_URL = 'http://localhost:18080/kabusapi';
const DEFAULT_TIMEOUT = 10_000;
const RATE_LIMIT_INTERVAL_MS = 125; // 8 req/sec

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class KabuStationClient {
  private readonly baseUrl: string;
  private readonly apiPassword: string;
  private readonly timeout: number;
  private token: string | null = null;

  constructor(options: KabuStationClientOptions) {
    this.apiPassword = options.apiPassword;
    this.baseUrl = options.baseUrl ?? DEFAULT_BASE_URL;
    this.timeout = options.timeout ?? DEFAULT_TIMEOUT;
  }

  /**
   * 認証トークンを取得
   */
  async authenticate(): Promise<string> {
    logger.info('Authenticating with kabu STATION API');

    const response = await fetch(`${this.baseUrl}/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ APIPassword: this.apiPassword }),
      signal: AbortSignal.timeout(this.timeout),
    });

    if (!response.ok) {
      throw new Error(`Authentication failed: HTTP ${response.status}`);
    }

    const data = (await response.json()) as TokenResponse;
    if (data.ResultCode !== 0) {
      throw new Error(`Authentication failed: ResultCode=${data.ResultCode}`);
    }

    this.token = data.Token;
    logger.info('Authentication successful');
    return data.Token;
  }

  /**
   * トークンが有効か確認し、なければ取得
   */
  private async ensureToken(): Promise<string> {
    if (!this.token) {
      await this.authenticate();
    }
    return this.token!;
  }

  /**
   * 単一銘柄の信用プレミアム情報を取得
   */
  async getMarginPremium(symbol: string): Promise<MarginPremiumResponse> {
    const token = await this.ensureToken();

    const response = await fetch(
      `${this.baseUrl}/margin/marginpremium/${encodeURIComponent(symbol)}`,
      {
        headers: { 'X-API-KEY': token },
        signal: AbortSignal.timeout(this.timeout),
      },
    );

    if (!response.ok) {
      // トークン失効の場合、再認証して1回リトライ
      if (response.status === 401) {
        logger.info('Token expired, re-authenticating');
        await this.authenticate();
        const retryResponse = await fetch(
          `${this.baseUrl}/margin/marginpremium/${encodeURIComponent(symbol)}`,
          {
            headers: { 'X-API-KEY': this.token! },
            signal: AbortSignal.timeout(this.timeout),
          },
        );
        if (!retryResponse.ok) {
          throw new Error(`MarginPremium failed after re-auth: HTTP ${retryResponse.status}`);
        }
        return (await retryResponse.json()) as MarginPremiumResponse;
      }
      throw new Error(`MarginPremium failed: HTTP ${response.status} for ${symbol}`);
    }

    return (await response.json()) as MarginPremiumResponse;
  }

  /**
   * 複数銘柄の信用プレミアム情報をバッチ取得（レート制限付き）
   */
  async getMarginPremiumBatch(
    symbols: string[],
  ): Promise<Map<string, MarginPremiumResponse>> {
    const results = new Map<string, MarginPremiumResponse>();

    for (let i = 0; i < symbols.length; i++) {
      const symbol = symbols[i];
      try {
        const premium = await this.getMarginPremium(symbol);
        results.set(symbol, premium);
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        logger.warn('Failed to get margin premium', { symbol, error: msg });
      }

      // レート制限
      if (i < symbols.length - 1) {
        await sleep(RATE_LIMIT_INTERVAL_MS);
      }

      // 進捗ログ（100件ごと）
      if ((i + 1) % 100 === 0) {
        logger.info('Batch progress', { processed: i + 1, total: symbols.length });
      }
    }

    return results;
  }
}
