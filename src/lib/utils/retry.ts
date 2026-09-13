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
 * PostgREST で一時的障害とみなす HTTP ステータス
 *
 * @description 0 は fetch 自体の失敗（supabase-js が status=0 で返す）。
 * 500 は含めない: PostgREST は SQL 例外（statement timeout・CHECK 違反など）も 500 で返すため、
 * 投げ直しても同じ結果になるうえ、握り潰してはいけない不具合を隠してしまう。
 * 520-524 は Cloudflare 系プロキシがオリジン異常時に返すもので 502/504 と同じ扱いでよい。
 */
export const TRANSIENT_POSTGREST_STATUS_CODES = [
  0, 408, 429, 502, 503, 504, 520, 521, 522, 523, 524,
];

/**
 * PostgREST 応答のステータスが一時的障害かどうか（既定判定）
 */
export function isTransientPostgrestStatus(status: number): boolean {
  return TRANSIENT_POSTGREST_STATUS_CODES.includes(status);
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
export interface PostgrestRetryOptions extends RetryOptions {
  /**
   * 再試行してよいかを呼び出し側が判断するためのガード。
   *
   * クレーム取得のように「1回目が実はコミットしていた」場合に投げ直すと意味が変わる
   * 操作で使う。DB の現在状態を読み、前回が無効だったと確認できたときだけ true を返すこと。
   * 省略時は常に再試行する（冪等な操作向け）。
   */
  isRetrySafe?: () => Promise<boolean>;
}

export async function withPostgrestRetry<T extends PostgrestLikeResult>(
  run: () => PromiseLike<T>,
  options?: PostgrestRetryOptions
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

    if (options?.isRetrySafe && !(await options.isRetrySafe())) {
      // 状態が進んでいる（＝前回が効いていた可能性がある）ので投げ直さない
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

/** メソッドを取り出す（Request オブジェクト渡しにも対応） */
function methodOf(input: RequestInfo | URL, init?: RequestInit): string {
  if (init?.method) return init.method.toUpperCase();
  if (typeof input === 'object' && input !== null && 'method' in input) {
    return String((input as Request).method).toUpperCase();
  }
  return 'GET';
}

/** Prefer ヘッダを取り出す（PostgREST の upsert 判定に使う） */
function preferOf(input: RequestInfo | URL, init?: RequestInit): string {
  const raw =
    init?.headers ??
    (typeof input === 'object' && input !== null && 'headers' in input
      ? (input as Request).headers
      : undefined);
  if (!raw) return '';
  try {
    return new Headers(raw as HeadersInit).get('prefer') ?? '';
  } catch {
    return '';
  }
}

/**
 * 投げ直しても結果が変わらないリクエストか。
 *
 * - GET / HEAD: 読み取り
 * - PATCH: PostgREST の update はリテラル値の代入なので二重に届いても同じ状態になる
 * - POST + `Prefer: resolution=merge-duplicates`（= upsert）: onConflict で吸収される
 *
 * 素の POST（insert・RPC）と DELETE は、レスポンスを取りこぼしただけでサーバ側は
 * コミット済みという場合に意味が変わるため対象外。冪等と分かっているものだけ
 * 呼び出し側で {@link withPostgrestRetry} を使う。
 * ストリーム本文は投げ直せないため、本文が文字列か空のときに限る。
 */
function isIdempotentRequest(
  input: RequestInfo | URL,
  init: RequestInit | undefined,
  retryWrites: boolean
): boolean {
  const method = methodOf(input, init);
  if (method === 'GET' || method === 'HEAD') return true;
  if (!retryWrites) return false;

  // 本文が再送できない形（ストリーム等）なら投げ直さない。
  // Request オブジェクトに本文を持たせて渡された場合、1回目で消費済みなので同様に除外する
  const initBody = init?.body;
  if (initBody != null && typeof initBody !== 'string') return false;
  if (
    initBody == null &&
    typeof input === 'object' &&
    input !== null &&
    'body' in input &&
    (input as Request).body != null
  ) {
    return false;
  }

  if (method === 'PATCH') return true;
  if (method === 'POST') return /resolution=(merge|ignore)-duplicates/i.test(preferOf(input, init));
  return false;
}

/**
 * 経路側（undici / ゲートウェイ）の瞬断と分かる例外メッセージ。
 *
 * これ以外の例外（AbortError、URL 不正など）は投げ直しても同じなので対象外。
 */
const TRANSIENT_FETCH_MESSAGE =
  /fetch failed|socket hang up|other side closed|terminated|connect timeout|network|ECONNRESET|ECONNREFUSED|ETIMEDOUT|EAI_AGAIN|ENOTFOUND|EPIPE|UND_ERR/i;

export interface RetryingFetchOptions {
  /** 総試行回数（初回を含む）。既定 3 */
  maxAttempts?: number;
  /** バックオフの基準待ち時間(ms)。既定 200 */
  baseDelayMs?: number;
  /**
   * 冪等な書き込み（PATCH / upsert）も投げ直すか。既定 false（読み取りのみ）。
   *
   * 「保存後の状態が同じ」でも、UPDATE トリガーが監査行を作るようなテーブルでは
   * 二重に副作用が起きる。true にしてよいのは、UPDATE の副作用が updated_at の
   * 更新に留まると確認済みのスキーマだけ（jquants_core / jquants_ingest / analytics /
   * scouter は結果テーブルのみで、監査トリガーを持つのは portfolio スキーマ）。
   */
  retryIdempotentWrites?: boolean;
  /** 差し替え用（テスト） */
  fetchImpl?: typeof fetch;
}

/**
 * 冪等なリクエストだけを一過性エラー時に投げ直す fetch を作る。
 *
 * @description Supabase クライアントに渡すと、全 Cron の読み取りが
 * ゲートウェイの瞬断（504 等）を自動で乗り越えるようになる。
 * `retryIdempotentWrites` を立てたときだけ upsert/update も対象に含める。
 */
export function createRetryingFetch(options: RetryingFetchOptions = {}): typeof fetch {
  const maxAttempts = Math.max(1, options.maxAttempts ?? 3);
  const baseDelayMs = options.baseDelayMs ?? 200;
  const fetchImpl = options.fetchImpl ?? fetch;
  const retryWrites = options.retryIdempotentWrites ?? false;

  return async function retryingFetch(
    input: RequestInfo | URL,
    init?: RequestInit
  ): Promise<Response> {
    const idempotent = isIdempotentRequest(input, init, retryWrites);

    for (let attempt = 1; ; attempt++) {
      let message: string;

      try {
        const response = await fetchImpl(input, init);
        if (!idempotent || attempt === maxAttempts || !isTransientPostgrestStatus(response.status)) {
          return response;
        }
        // 破棄するレスポンスのボディは読み切って接続を解放する
        await response.arrayBuffer().catch(() => undefined);
        message = `HTTP ${response.status}`;
      } catch (e) {
        message = e instanceof Error ? e.message : String(e);
        // 中断（AbortSignal）は投げ直さない。同じ signal では即座に失敗するだけ
        const aborted =
          (e instanceof Error && e.name === 'AbortError') || Boolean(init?.signal?.aborted);
        if (!idempotent || aborted || attempt === maxAttempts) throw e;
        if (!TRANSIENT_FETCH_MESSAGE.test(message)) throw e;
      }

      const delayMs = calculateDelay(attempt - 1, baseDelayMs, 8000, 100);
      console.warn(
        `[supabase-fetch] transient error (attempt ${attempt}/${maxAttempts}): ${message} — retrying`
      );
      await sleep(delayMs);
    }
  };
}
