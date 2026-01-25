/**
 * J-Quants API V2 クライアント
 *
 * @description API キー認証、レート制限、ページネーション、リトライ対応
 * @see https://jpx-jquants.com/en/spec
 *
 * V2では:
 * - すべてのエンドポイントが "data" キーでデータを返す
 * - 認証は x-api-key ヘッダーを使用
 */

import { getJQuantsRateLimiter } from './rate-limiter';
import { fetchWithRetry, RetryableError, NonRetryableError } from '../utils/retry';
import { createLogger, type LogContext } from '../utils/logger';
import type {
  TradingCalendarResponse,
  EquityMasterResponse,
  EquityBarsDailyResponse,
  TopixBarsDailyResponse,
  FinancialSummaryResponse,
  EarningsCalendarResponse,
  InvestorTypeTradingResponse,
  TradingCalendarItem,
  EquityMasterItem,
  EquityBarDailyItem,
  TopixBarDailyItem,
  FinancialSummaryItem,
  EarningsCalendarItem,
  InvestorTypeTradingItem,
} from './types';

const BASE_URL = 'https://api.jquants.com/v2';

export interface JQuantsClientOptions {
  /** API キー（省略時は環境変数 JQUANTS_API_KEY を使用） */
  apiKey?: string;
  /** リクエストタイムアウト（ミリ秒、デフォルト: 30000） */
  timeoutMs?: number;
  /** ロガーコンテキスト */
  logContext?: LogContext;
}

export interface RequestOptions {
  /** クエリパラメータ */
  params?: Record<string, string | number | undefined>;
  /** ページネーションキー */
  paginationKey?: string;
}

/**
 * J-Quants API クライアント
 */
export class JQuantsClient {
  private readonly apiKey: string;
  private readonly timeoutMs: number;
  private readonly logger: ReturnType<typeof createLogger>;

  constructor(options?: JQuantsClientOptions) {
    const apiKey = options?.apiKey ?? process.env.JQUANTS_API_KEY;
    if (!apiKey) {
      throw new Error('J-Quants API key is required. Set JQUANTS_API_KEY environment variable.');
    }

    this.apiKey = apiKey;
    this.timeoutMs = options?.timeoutMs ?? 30000;
    this.logger = createLogger(options?.logContext ?? {});
  }

  /**
   * APIリクエストを実行
   */
  private async request<T>(
    endpoint: string,
    options?: RequestOptions
  ): Promise<T> {
    // レート制限を適用
    const rateLimiter = getJQuantsRateLimiter();
    await rateLimiter.acquire();

    // URLを構築
    const url = new URL(`${BASE_URL}${endpoint}`);

    // クエリパラメータを追加
    if (options?.params) {
      for (const [key, value] of Object.entries(options.params)) {
        if (value !== undefined && value !== null && value !== '') {
          url.searchParams.append(key, String(value));
        }
      }
    }

    // ページネーションキーを追加
    if (options?.paginationKey) {
      url.searchParams.append('pagination_key', options.paginationKey);
    }

    this.logger.debug('API request', {
      endpoint,
      params: options?.params,
      hasPaginationKey: !!options?.paginationKey,
    });

    try {
      const response = await fetchWithRetry(
        url.toString(),
        {
          method: 'GET',
          headers: {
            'x-api-key': this.apiKey,
            'Accept': 'application/json',
          },
          signal: AbortSignal.timeout(this.timeoutMs),
        },
        {
          maxRetries: 5,
          baseDelayMs: 500,
          maxDelayMs: 32000,
          onRetry: (attempt, error, delayMs) => {
            this.logger.warn('API request retry', {
              endpoint,
              attempt,
              delayMs,
              error,
            });
          },
        }
      );

      const data = await response.json();

      this.logger.debug('API response', {
        endpoint,
        hasPaginationKey: !!data.pagination_key,
      });

      return data as T;
    } catch (error) {
      if (error instanceof NonRetryableError) {
        this.logger.error('API request failed (non-retryable)', {
          endpoint,
          statusCode: error.statusCode,
          error,
        });
      } else if (error instanceof RetryableError) {
        this.logger.error('API request failed after retries', {
          endpoint,
          statusCode: error.statusCode,
          error,
        });
      } else {
        this.logger.error('API request failed', {
          endpoint,
          error,
        });
      }
      throw error;
    }
  }

  /**
   * ページネーション付きリクエストを全ページ取得
   *
   * @param endpoint APIエンドポイント
   * @param dataKey レスポンスからデータを取り出すキー（V2では常に "data"）
   * @param options リクエストオプション
   * @yields 各ページのデータ配列
   */
  async *requestPaginated<T, R extends { pagination_key?: string }>(
    endpoint: string,
    dataKey: keyof R,
    options?: RequestOptions
  ): AsyncGenerator<T[], void, unknown> {
    let paginationKey: string | undefined = options?.paginationKey;
    let pageCount = 0;
    const maxPages = 1000; // 安全弁

    do {
      const response = await this.request<R>(endpoint, {
        ...options,
        paginationKey,
      });

      // リクエストごとにインクリメント（空データでも無限ループを防止）
      pageCount++;

      const data = response[dataKey] as T[];
      if (data && data.length > 0) {
        yield data;
      }

      paginationKey = response.pagination_key;

      if (pageCount >= maxPages) {
        this.logger.warn('Max pages reached', { endpoint, pageCount, maxPages });
        break;
      }
    } while (paginationKey);

    this.logger.info('Pagination complete', { endpoint, pageCount });
  }

  /**
   * ページネーション付きリクエストを全ページ取得して配列として返す
   */
  async fetchAllPages<T, R extends { pagination_key?: string }>(
    endpoint: string,
    dataKey: keyof R,
    options?: RequestOptions
  ): Promise<T[]> {
    const allData: T[] = [];

    for await (const pageData of this.requestPaginated<T, R>(endpoint, dataKey, options)) {
      allData.push(...pageData);
    }

    return allData;
  }

  // ============================================
  // 各エンドポイント用メソッド
  // V2: すべてのエンドポイントで "data" キーを使用
  // ============================================

  /**
   * 取引カレンダー取得
   * V2 エンドポイント: GET /v2/markets/calendar
   */
  async getTradingCalendar(params?: { from?: string; to?: string }): Promise<TradingCalendarResponse> {
    return this.request<TradingCalendarResponse>(
      '/markets/calendar',
      { params }
    );
  }

  /**
   * 上場銘柄マスタ取得
   * V2 エンドポイント: GET /v2/equities/master
   */
  async getEquityMaster(params?: { code?: string; date?: string }): Promise<EquityMasterResponse> {
    return this.request<EquityMasterResponse>(
      '/equities/master',
      { params }
    );
  }

  /**
   * 株価四本値（日足）取得（ページネーション対応）
   * V2 エンドポイント: GET /v2/equities/bars/daily
   */
  async *getEquityBarsDailyPaginated(
    params: { code?: string; date?: string; from?: string; to?: string }
  ): AsyncGenerator<EquityBarDailyItem[], void, unknown> {
    yield* this.requestPaginated<EquityBarDailyItem, EquityBarsDailyResponse>(
      '/equities/bars/daily',
      'data',
      { params }
    );
  }

  /**
   * 株価四本値（日足）全件取得
   */
  async getEquityBarsDaily(
    params: { code?: string; date?: string; from?: string; to?: string }
  ): Promise<EquityBarDailyItem[]> {
    return this.fetchAllPages<EquityBarDailyItem, EquityBarsDailyResponse>(
      '/equities/bars/daily',
      'data',
      { params }
    );
  }

  /**
   * TOPIX取得
   * V2 エンドポイント: GET /v2/indices/bars/daily/topix
   */
  async getTopixBarsDaily(params?: { from?: string; to?: string }): Promise<TopixBarsDailyResponse> {
    return this.request<TopixBarsDailyResponse>(
      '/indices/bars/daily/topix',
      { params }
    );
  }

  /**
   * 財務サマリー取得（ページネーション対応）
   * V2 エンドポイント: GET /v2/fins/summary
   */
  async *getFinancialSummaryPaginated(
    params: { code?: string; date?: string }
  ): AsyncGenerator<FinancialSummaryItem[], void, unknown> {
    yield* this.requestPaginated<FinancialSummaryItem, FinancialSummaryResponse>(
      '/fins/summary',
      'data',
      { params }
    );
  }

  /**
   * 財務サマリー全件取得
   */
  async getFinancialSummary(
    params: { code?: string; date?: string }
  ): Promise<FinancialSummaryItem[]> {
    return this.fetchAllPages<FinancialSummaryItem, FinancialSummaryResponse>(
      '/fins/summary',
      'data',
      { params }
    );
  }

  /**
   * 決算発表予定取得
   * V2 エンドポイント: GET /v2/equities/earnings-calendar
   */
  async getEarningsCalendar(): Promise<EarningsCalendarResponse> {
    return this.request<EarningsCalendarResponse>(
      '/equities/earnings-calendar'
    );
  }

  /**
   * 投資部門別売買状況取得
   * V2 エンドポイント: GET /v2/equities/investor-types
   */
  async getInvestorTypes(
    params?: { from?: string; to?: string; section?: string }
  ): Promise<InvestorTypeTradingResponse> {
    return this.request<InvestorTypeTradingResponse>(
      '/equities/investor-types',
      { params }
    );
  }
}

/**
 * デフォルトクライアントインスタンスを作成
 */
export function createJQuantsClient(options?: JQuantsClientOptions): JQuantsClient {
  return new JQuantsClient(options);
}

// 型のre-export
export type {
  TradingCalendarItem,
  EquityMasterItem,
  EquityBarDailyItem,
  TopixBarDailyItem,
  FinancialSummaryItem,
  EarningsCalendarItem,
  InvestorTypeTradingItem,
};
