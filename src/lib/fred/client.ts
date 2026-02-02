/**
 * FRED API クライアント
 *
 * @description レート制限、リトライ、ログ対応
 * @see https://fred.stlouisfed.org/docs/api/fred/
 */

import { RateLimiter } from '../jquants/rate-limiter';
import { fetchWithRetry, NonRetryableError } from '../utils/retry';
import { createLogger, type LogContext } from '../utils/logger';
import type {
  FredObservationsResponse,
  ParsedFredObservation,
} from './types';

const BASE_URL = 'https://api.stlouisfed.org/fred';

/** FRED欠損値マーカー */
const MISSING_VALUE = '.';

export interface FredClientOptions {
  /** API キー（省略時は環境変数 FRED_API_KEY を使用） */
  apiKey?: string;
  /** リクエストタイムアウト（ミリ秒、デフォルト: 30000） */
  timeoutMs?: number;
  /** ロガーコンテキスト */
  logContext?: LogContext;
}

/**
 * FRED API クライアント
 */
export class FredClient {
  private readonly apiKey: string;
  private readonly timeoutMs: number;
  private readonly logger: ReturnType<typeof createLogger>;

  constructor(options?: FredClientOptions) {
    const apiKey = options?.apiKey ?? process.env.FRED_API_KEY;
    if (!apiKey) {
      throw new Error('FRED API key is required. Set FRED_API_KEY environment variable.');
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
    params?: Record<string, string | number | undefined>
  ): Promise<T> {
    // レート制限を適用
    const rateLimiter = getFredRateLimiter();
    await rateLimiter.acquire();

    // URLを構築
    const url = new URL(`${BASE_URL}${endpoint}`);

    // api_key をクエリパラメータで渡す
    url.searchParams.append('api_key', this.apiKey);
    url.searchParams.append('file_type', 'json');

    // クエリパラメータを追加
    if (params) {
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== null && value !== '') {
          url.searchParams.append(key, String(value));
        }
      }
    }

    this.logger.debug('FRED API request', {
      endpoint,
      params,
    });

    try {
      const response = await fetchWithRetry(
        url.toString(),
        {
          method: 'GET',
          headers: {
            'Accept': 'application/json',
          },
          signal: AbortSignal.timeout(this.timeoutMs),
        },
        {
          maxRetries: 5,
          baseDelayMs: 500,
          maxDelayMs: 32000,
          onRetry: (attempt, error, delayMs) => {
            this.logger.warn('FRED API request retry', {
              endpoint,
              attempt,
              delayMs,
              error,
            });
          },
        }
      );

      const data = await response.json();

      this.logger.debug('FRED API response', {
        endpoint,
      });

      return data as T;
    } catch (error) {
      if (error instanceof NonRetryableError) {
        this.logger.error('FRED API request failed (non-retryable)', {
          endpoint,
          statusCode: error.statusCode,
          error,
        });
      } else {
        this.logger.error('FRED API request failed', {
          endpoint,
          error,
        });
      }
      throw error;
    }
  }

  /**
   * 系列の観測値を取得
   *
   * @param seriesId FRED series ID (例: 'VIXCLS')
   * @param observationStart 取得開始日 (YYYY-MM-DD)
   * @param observationEnd 取得終了日 (YYYY-MM-DD)
   * @returns パース済み観測値（欠損値 "." はスキップ済み）
   */
  async getSeriesObservations(
    seriesId: string,
    observationStart?: string,
    observationEnd?: string
  ): Promise<{ observations: ParsedFredObservation[]; skippedCount: number }> {
    const response = await this.request<FredObservationsResponse>(
      '/series/observations',
      {
        series_id: seriesId,
        observation_start: observationStart,
        observation_end: observationEnd,
      }
    );

    let skippedCount = 0;
    const observations: ParsedFredObservation[] = [];

    for (const obs of response.observations) {
      if (obs.value === MISSING_VALUE) {
        skippedCount++;
        continue;
      }

      const numValue = Number(obs.value);
      if (isNaN(numValue)) {
        skippedCount++;
        this.logger.warn('FRED observation has non-numeric value', {
          seriesId,
          date: obs.date,
          value: obs.value,
        });
        continue;
      }

      observations.push({
        date: obs.date,
        value: numValue,
        releasedAt: `${obs.realtime_start}T00:00:00Z`,
      });
    }

    this.logger.info('FRED observations fetched', {
      seriesId,
      total: response.observations.length,
      valid: observations.length,
      skipped: skippedCount,
    });

    return { observations, skippedCount };
  }
}

// ============================================
// シングルトン レートリミッター
// ============================================

let fredRateLimiter: RateLimiter | null = null;

/**
 * FRED API 用のレートリミッターを取得
 * 公式制限: 120リクエスト/60秒
 */
export function getFredRateLimiter(): RateLimiter {
  if (!fredRateLimiter) {
    fredRateLimiter = new RateLimiter({
      requestsPerMinute: 120,
      minIntervalMs: 500,
    });
  }
  return fredRateLimiter;
}

/**
 * デフォルトクライアントインスタンスを作成
 */
export function createFredClient(options?: FredClientOptions): FredClient {
  return new FredClient(options);
}
