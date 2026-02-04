/**
 * e-Stat API クライアント
 *
 * @description getStatsData エンドポイントでの統計データ取得
 * @see https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0
 */

import { RateLimiter } from '../jquants/rate-limiter';
import { fetchWithRetry, NonRetryableError } from '../utils/retry';
import { createLogger, type LogContext } from '../utils/logger';
import type {
  EStatApiResponse,
  EStatValue,
  ParsedEStatObservation,
} from './types';

const BASE_URL = 'https://api.e-stat.go.jp/rest/3.0/app';

/** e-Stat 欠損値マーカー */
const MISSING_VALUES = new Set(['-', '...', '***', 'x', 'X', '']);

export interface EStatClientOptions {
  /** API キー（省略時は環境変数 ESTAT_API_KEY を使用） */
  appId?: string;
  /** リクエストタイムアウト（ミリ秒、デフォルト: 30000） */
  timeoutMs?: number;
  /** ロガーコンテキスト */
  logContext?: LogContext;
}

/**
 * e-Stat API クライアント
 */
export class EStatClient {
  private readonly appId: string;
  private readonly timeoutMs: number;
  private readonly logger: ReturnType<typeof createLogger>;

  constructor(options?: EStatClientOptions) {
    const appId = options?.appId ?? process.env.ESTAT_API_KEY;
    if (!appId) {
      throw new Error('e-Stat API key is required. Set ESTAT_API_KEY environment variable.');
    }

    this.appId = appId;
    this.timeoutMs = options?.timeoutMs ?? 30000;
    this.logger = createLogger(options?.logContext ?? {});
  }

  /**
   * APIリクエストを実行
   */
  private async request(
    endpoint: string,
    params?: Record<string, string | number | undefined>
  ): Promise<EStatApiResponse> {
    // レート制限を適用
    const rateLimiter = getEStatRateLimiter();
    await rateLimiter.acquire();

    // URLを構築
    const url = new URL(`${BASE_URL}${endpoint}`);
    url.searchParams.append('appId', this.appId);

    // クエリパラメータを追加
    if (params) {
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== null && value !== '') {
          url.searchParams.append(key, String(value));
        }
      }
    }

    this.logger.debug('e-Stat API request', {
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
          baseDelayMs: 1000,
          maxDelayMs: 32000,
          onRetry: (attempt, error, delayMs) => {
            this.logger.warn('e-Stat API request retry', {
              endpoint,
              attempt,
              delayMs,
              error,
            });
          },
        }
      );

      const data = (await response.json()) as EStatApiResponse;

      // e-Stat APIはHTTP 200でもエラーを返すことがある
      const status = data.GET_STATS_DATA?.RESULT?.STATUS;
      if (status !== 0 && status !== undefined) {
        const errorMsg = data.GET_STATS_DATA?.RESULT?.ERROR_MSG ?? 'Unknown e-Stat error';
        this.logger.error('e-Stat API returned error status', {
          endpoint,
          status,
          errorMsg,
        });
        throw new NonRetryableError(`e-Stat API error: ${errorMsg}`, status);
      }

      this.logger.debug('e-Stat API response', { endpoint });

      return data;
    } catch (error) {
      if (error instanceof NonRetryableError) {
        this.logger.error('e-Stat API request failed (non-retryable)', {
          endpoint,
          statusCode: error.statusCode,
          error,
        });
      } else {
        this.logger.error('e-Stat API request failed', {
          endpoint,
          error,
        });
      }
      throw error;
    }
  }

  /**
   * 統計データを取得
   *
   * @param statsDataId 統計表ID
   * @param sourceFilter レスポンスから該当系列を抽出するフィルタ条件
   * @returns パース済み観測値
   */
  async getStatsData(
    statsDataId: string,
    sourceFilter?: Record<string, string> | null
  ): Promise<{ observations: ParsedEStatObservation[]; skippedCount: number }> {
    const response = await this.request('/json/getStatsData', {
      statsDataId,
      lang: 'J',
    });

    const values = response.GET_STATS_DATA?.STATISTICAL_DATA?.DATA_INF?.VALUE;
    if (!values || values.length === 0) {
      this.logger.warn('e-Stat returned no data', { statsDataId });
      return { observations: [], skippedCount: 0 };
    }

    // source_filter に基づくフィルタリング
    // CLASS_INF から code→name マッピングを構築して名前ベースのフィルタをコード値に変換
    const classInfo = response.GET_STATS_DATA?.STATISTICAL_DATA?.CLASS_INF;
    const filteredValues = sourceFilter
      ? this.filterValues(values, sourceFilter, classInfo ?? undefined)
      : values;

    let skippedCount = 0;
    // 同じ日付のデータは後勝ち（最新値）でマージ
    const observationMap = new Map<string, ParsedEStatObservation>();

    for (const val of filteredValues) {
      const rawValue = val.$;

      // 欠損値チェック
      if (MISSING_VALUES.has(rawValue)) {
        skippedCount++;
        continue;
      }

      const numValue = Number(rawValue);
      if (isNaN(numValue)) {
        skippedCount++;
        this.logger.warn('e-Stat observation has non-numeric value', {
          statsDataId,
          time: val['@time'],
          value: rawValue,
        });
        continue;
      }

      const date = this.parseTimeCode(val['@time']);
      if (!date) {
        skippedCount++;
        continue;
      }

      // 同じ日付は上書き（後勝ち）
      observationMap.set(date, {
        date,
        value: numValue,
      });
    }

    const observations = Array.from(observationMap.values());

    this.logger.info('e-Stat observations fetched', {
      statsDataId,
      total: values.length,
      filtered: filteredValues.length,
      valid: observations.length,
      skipped: skippedCount,
    });

    return { observations, skippedCount };
  }

  /**
   * source_filter に基づいて VALUE 配列をフィルタ
   *
   * source_filter のキーは CLASS_OBJ の @id に対応する属性名（例: "cat01"）、
   * 値はフィルタ対象の CLASS @name（日本語名）。
   *
   * VALUE の @catXX 属性にはコード値（例: "01"）が入るため、
   * CLASS_INF から name→code の逆引きを行い、コード値で照合する。
   */
  private filterValues(
    values: EStatValue[],
    sourceFilter: Record<string, string>,
    classInfo?: { CLASS_OBJ: Array<{ '@id': string; '@name': string; CLASS: Array<{ '@code': string; '@name': string }> | { '@code': string; '@name': string } }> }
  ): EStatValue[] {
    // CLASS_INF から name→code マッピングを構築
    const nameToCodeMap = new Map<string, Map<string, string>>(); // key: catId, value: Map<name, code>
    if (classInfo?.CLASS_OBJ) {
      for (const classObj of classInfo.CLASS_OBJ) {
        const catId = classObj['@id']; // e.g. "cat01"
        const classes = Array.isArray(classObj.CLASS) ? classObj.CLASS : [classObj.CLASS];
        const nameMap = new Map<string, string>();
        for (const cls of classes) {
          nameMap.set(cls['@name'], cls['@code']);
        }
        nameToCodeMap.set(catId, nameMap);
      }
    }

    // sourceFilter の name をコード値に変換
    const codeFilter = new Map<string, string[]>(); // key: @catXX, value: [code values]
    for (const [filterKey, filterName] of Object.entries(sourceFilter)) {
      const catId = filterKey.startsWith('@') ? filterKey.slice(1) : filterKey;
      const attrKey = `@${catId}`;
      const nameMap = nameToCodeMap.get(catId);
      if (nameMap) {
        // 部分一致で該当する全コードを収集
        const matchingCodes: string[] = [];
        for (const [name, code] of nameMap) {
          if (name === filterName || name.includes(filterName)) {
            matchingCodes.push(code);
          }
        }
        if (matchingCodes.length > 0) {
          codeFilter.set(attrKey, matchingCodes);
        } else {
          // 名前一致なし: フィルタ名をそのままコード値として試す（フォールバック）
          this.logger.warn('No CLASS_INF match for filter, using raw value', { catId, filterName });
          codeFilter.set(attrKey, [filterName]);
        }
      } else {
        // CLASS_INF にカテゴリがない場合はフォールバック
        codeFilter.set(attrKey, [filterName]);
      }
    }

    return values.filter((val) => {
      for (const [attrKey, validCodes] of codeFilter) {
        const attrValue = val[attrKey];
        if (attrValue === undefined) return false;
        if (!validCodes.includes(attrValue)) return false;
      }
      return true;
    });
  }

  /**
   * e-Stat の時間コードを YYYY-MM-DD に変換
   *
   * @param timeCode 例: "2024000101" (年月), "2024001200" (年月)
   * @returns YYYY-MM-DD 形式（月末日）。パース不能な場合は null
   */
  private parseTimeCode(timeCode: string): string | null {
    // 一般的なパターン: "2024001200" → 2024年12月
    // パターン1: YYYYMMdd 的なもの
    // パターン2: "2024年01月" 的な文字列

    // 数字のみのパターン
    const numMatch = timeCode.match(/^(\d{4})(\d{2})(\d{2})(\d{2})$/);
    if (numMatch) {
      const year = parseInt(numMatch[1], 10);
      const month = parseInt(numMatch[3], 10); // 位置は統計による
      if (month >= 1 && month <= 12) {
        return this.lastDayOfMonth(year, month);
      }
      // 別の位置を試す
      const month2 = parseInt(numMatch[2], 10);
      if (month2 >= 1 && month2 <= 12) {
        return this.lastDayOfMonth(year, month2);
      }
    }

    // "YYYY00MM00" パターン
    const altMatch = timeCode.match(/^(\d{4})00(\d{2})00$/);
    if (altMatch) {
      const year = parseInt(altMatch[1], 10);
      const month = parseInt(altMatch[2], 10);
      if (month >= 1 && month <= 12) {
        return this.lastDayOfMonth(year, month);
      }
    }

    // "YYYYMM" パターン
    const shortMatch = timeCode.match(/^(\d{4})(\d{2})$/);
    if (shortMatch) {
      const year = parseInt(shortMatch[1], 10);
      const month = parseInt(shortMatch[2], 10);
      if (month >= 1 && month <= 12) {
        return this.lastDayOfMonth(year, month);
      }
    }

    this.logger.warn('Unable to parse e-Stat time code', { timeCode });
    return null;
  }

  /**
   * 月末日を返す
   */
  private lastDayOfMonth(year: number, month: number): string {
    // 翌月1日の前日 = 当月末日
    const lastDay = new Date(year, month, 0).getDate();
    const mm = String(month).padStart(2, '0');
    const dd = String(lastDay).padStart(2, '0');
    return `${year}-${mm}-${dd}`;
  }
}

// ============================================
// シングルトン レートリミッター
// ============================================

let estatRateLimiter: RateLimiter | null = null;

/**
 * e-Stat API 用のレートリミッターを取得
 * 公式制限は明記されていないが、保守的に設定
 */
export function getEStatRateLimiter(): RateLimiter {
  if (!estatRateLimiter) {
    estatRateLimiter = new RateLimiter({
      requestsPerMinute: 30,
      minIntervalMs: 2000,
    });
  }
  return estatRateLimiter;
}

/**
 * デフォルトクライアントインスタンスを作成
 */
export function createEStatClient(options?: EStatClientOptions): EStatClient {
  return new EStatClient(options);
}
