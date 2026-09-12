/**
 * 指数バックオフリトライユーティリティ
 *
 * @description 429/5xx エラー時に指数バックオフでリトライ
 */

export interface RetryOptions {
  /** 最大リトライ回数（デフォルト: 5） */
  maxRetries?: number;
  /** 基本遅延時間（ミリ秒、デフォルト: 500） */
  baseDelayMs?: number;
  /** 最大遅延時間（ミリ秒、デフォルト: 32000） */
  maxDelayMs?: number;
  /** ジッター幅（ミリ秒、デフォルト: 100） */
  jitterMs?: number;
  /** リトライ対象のステータスコード */
  retryStatusCodes?: number[];
  /** リトライ時のコールバック */
  onRetry?: (attempt: number, error: Error, delayMs: number) => void;
}

/**
 * リトライ可能なエラーかどうかを判定
 */
export class RetryableError extends Error {
  constructor(
    message: string,
    public readonly statusCode?: number,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'RetryableError';
  }
}

/**
 * リトライ不可能なエラー（即座に失敗）
 */
export class NonRetryableError extends Error {
  constructor(
    message: string,
    public readonly statusCode?: number,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'NonRetryableError';
  }
}

/**
 * 指定時間スリープ
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * ジッター付きの遅延時間を計算
 */
function calculateDelay(
  attempt: number,
  baseDelayMs: number,
  maxDelayMs: number,
  jitterMs: number
): number {
  // 指数バックオフ: baseDelay * 2^attempt
  const exponentialDelay = baseDelayMs * Math.pow(2, attempt);
  // 最大遅延でキャップ
  const cappedDelay = Math.min(exponentialDelay, maxDelayMs);
  // ランダムジッター追加
  const jitter = Math.random() * jitterMs;
  return cappedDelay + jitter;
}

/**
 * 指数バックオフリトライでラップされた関数を実行
 *
 * @example
 * ```typescript
 * const result = await withRetry(
 *   async () => {
 *     const response = await fetch(url);
 *     if (!response.ok) {
 *       throw new RetryableError('Request failed', response.status);
 *     }
 *     return response.json();
 *   },
 *   { maxRetries: 3, baseDelayMs: 1000 }
 * );
 * ```
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  options?: RetryOptions
): Promise<T> {
  const {
    maxRetries = 5,
    baseDelayMs = 500,
    maxDelayMs = 32000,
    jitterMs = 100,
    retryStatusCodes = [429, 500, 502, 503, 504],
    onRetry,
  } = options ?? {};

  let lastError: Error | undefined;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;

      // NonRetryableError は即座に失敗
      if (error instanceof NonRetryableError) {
        throw error;
      }

      // 最後の試行で失敗した場合は例外を投げる
      if (attempt === maxRetries) {
        throw lastError;
      }

      // リトライ可能かチェック
      const isRetryable =
        error instanceof RetryableError ||
        (error instanceof Error &&
          'statusCode' in error &&
          retryStatusCodes.includes((error as { statusCode: number }).statusCode));

      if (!isRetryable && !(error instanceof Error && error.message.includes('fetch'))) {
        // ネットワークエラー以外の非リトライ可能エラー
        throw error;
      }

      // 遅延時間を計算してスリープ
      const delayMs = calculateDelay(attempt, baseDelayMs, maxDelayMs, jitterMs);

      if (onRetry) {
        onRetry(attempt + 1, lastError, delayMs);
      }

      await sleep(delayMs);
    }
  }

  // ここには到達しないはずだが、TypeScript用
  throw lastError ?? new Error('Unknown error during retry');
}

/**
 * fetch をリトライ付きでラップ
 *
 * @example
 * ```typescript
 * const response = await fetchWithRetry('https://api.example.com/data', {
 *   method: 'GET',
 *   headers: { 'Authorization': 'Bearer xxx' },
 * });
 * ```
 */
export async function fetchWithRetry(
  url: string,
  init?: RequestInit,
  retryOptions?: RetryOptions
): Promise<Response> {
  return withRetry(
    async () => {
      const response = await fetch(url, init);

      if (!response.ok) {
        const statusCode = response.status;
        const retryStatusCodes = retryOptions?.retryStatusCodes ?? [429, 500, 502, 503, 504];

        if (retryStatusCodes.includes(statusCode)) {
          throw new RetryableError(
            `HTTP ${statusCode}: ${response.statusText}`,
            statusCode
          );
        }

        throw new NonRetryableError(
          `HTTP ${statusCode}: ${response.statusText}`,
          statusCode
        );
      }

      return response;
    },
    retryOptions
  );
}

/**
 * PostgREST（supabase-js）応答の最小形
 *
 * @description supabase-js はネットワーク障害・5xx でも例外を投げず
 * `{ error, status }` を返すため、戻り値を見てリトライ判定する
 */
export interface PostgrestLikeResult<T = unknown> {
  data?: T | null;
  error: { message: string } | null;
  status?: number;
}

/**
 * PostgREST で一時的障害とみなす 5xx 以外の HTTP ステータス
 *
 * @description 0 は fetch 自体の失敗（supabase-js が status=0 で返す）。
 * 5xx は 520 等のプロキシ由来を含め既定で全てリトライ対象（{@link isTransientPostgrestStatus}）
 */
export const TRANSIENT_POSTGREST_STATUS_CODES = [0, 408, 429];

/**
 * PostgREST 応答のステータスが一時的障害かどうか（既定判定）
 *
 * @description 5xx 全域 + fetch 失敗(0) + 408/429 を一時的とみなす
 */
export function isTransientPostgrestStatus(status: number): boolean {
  return TRANSIENT_POSTGREST_STATUS_CODES.includes(status) || (status >= 500 && status < 600);
}

/**
 * Supabase(PostgREST) クエリを指数バックオフでリトライ実行する
 *
 * @param run クエリビルダーを毎回組み立てて返す関数（同じビルダーを使い回さないこと）
 * @param options リトライ設定（retryStatusCodes を渡すとそのリストだけを対象にする。
 * 既定は {@link isTransientPostgrestStatus} = 5xx 全域 + 0/408/429）
 * @returns 最後に得られた PostgREST 応答（リトライ上限に達した場合はエラーを含んだまま返す）
 *
 * @example
 * ```typescript
 * const { data, error } = await withPostgrestRetry(() =>
 *   supabase.from('macro_series_metadata').select('*')
 * );
 * ```
 */
export async function withPostgrestRetry<T extends PostgrestLikeResult>(
  run: () => PromiseLike<T>,
  options?: RetryOptions
): Promise<T> {
  const {
    maxRetries = 3,
    baseDelayMs = 1000,
    maxDelayMs = 8000,
    jitterMs = 100,
    retryStatusCodes,
    onRetry,
  } = options ?? {};

  // 明示指定があればそのリストのみ、無指定なら 5xx 全域 + 0/408/429
  const isTransient = (status: number): boolean =>
    retryStatusCodes ? retryStatusCodes.includes(status) : isTransientPostgrestStatus(status);

  let result = await run();

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    if (!result.error) {
      return result;
    }

    // status が無い応答（= 業務エラー相当）はリトライしない
    if (result.status === undefined || !isTransient(result.status)) {
      return result;
    }

    const delayMs = calculateDelay(attempt, baseDelayMs, maxDelayMs, jitterMs);

    if (onRetry) {
      onRetry(attempt + 1, new Error(result.error.message), delayMs);
    }

    await sleep(delayMs);
    result = await run();
  }

  return result;
}
