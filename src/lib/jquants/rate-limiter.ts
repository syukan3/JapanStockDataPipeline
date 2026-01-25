/**
 * J-Quants API レート制限
 *
 * @description Light プラン: 60 req/min (1 req/秒)
 * トークンバケットアルゴリズムで制御
 */

export interface RateLimiterOptions {
  /** 1分あたりの最大リクエスト数（デフォルト: 60） */
  requestsPerMinute?: number;
  /** 最小リクエスト間隔（ミリ秒、デフォルト: 1000） */
  minIntervalMs?: number;
}

/**
 * トークンバケット方式のレート制限
 */
export class RateLimiter {
  private readonly requestsPerMinute: number;
  private readonly minIntervalMs: number;
  private readonly bucketCapacity: number;
  private tokens: number;
  private lastRefillTime: number;
  private lastRequestTime: number;

  constructor(options?: RateLimiterOptions) {
    this.requestsPerMinute = options?.requestsPerMinute ?? 60;
    this.minIntervalMs = options?.minIntervalMs ?? 1000;
    this.bucketCapacity = this.requestsPerMinute;
    this.tokens = this.bucketCapacity;
    this.lastRefillTime = Date.now();
    this.lastRequestTime = 0;
  }

  /**
   * トークンを補充
   */
  private refill(): void {
    const now = Date.now();
    const elapsedMs = now - this.lastRefillTime;
    // 1分あたり requestsPerMinute トークンを補充
    const tokensToAdd = (elapsedMs / 60000) * this.requestsPerMinute;
    this.tokens = Math.min(this.bucketCapacity, this.tokens + tokensToAdd);
    this.lastRefillTime = now;
  }

  /**
   * 次のリクエストまでの待機時間（ミリ秒）を計算
   */
  private getWaitTime(): number {
    this.refill();

    // トークンがある場合
    if (this.tokens >= 1) {
      // 最小間隔のチェック
      const timeSinceLastRequest = Date.now() - this.lastRequestTime;
      if (timeSinceLastRequest < this.minIntervalMs) {
        return this.minIntervalMs - timeSinceLastRequest;
      }
      return 0;
    }

    // トークンがない場合、1トークン補充されるまでの時間を計算
    const msPerToken = 60000 / this.requestsPerMinute;
    const tokensNeeded = 1 - this.tokens;
    return Math.ceil(tokensNeeded * msPerToken);
  }

  /**
   * トークンを消費（リクエスト実行前に呼び出す）
   * 必要に応じて待機する
   */
  async acquire(): Promise<void> {
    const waitTime = this.getWaitTime();

    if (waitTime > 0) {
      await new Promise((resolve) => setTimeout(resolve, waitTime));
      // 待機後に再度補充
      this.refill();
    }

    this.tokens -= 1;
    this.lastRequestTime = Date.now();
  }

  /**
   * 現在利用可能なトークン数
   */
  get availableTokens(): number {
    this.refill();
    return Math.floor(this.tokens);
  }

  /**
   * レート制限をリセット
   */
  reset(): void {
    this.tokens = this.bucketCapacity;
    this.lastRefillTime = Date.now();
    this.lastRequestTime = 0;
  }
}

/**
 * シングルトンインスタンス（J-Quants API用）
 */
let jquantsRateLimiter: RateLimiter | null = null;

/**
 * J-Quants API用のレートリミッターを取得
 *
 * NOTE: Vercel Functionsではコールドスタート時に新しいインスタンスが作成されるため、
 * 複数のFunction間でレート制限状態が共有されません。
 * 本システムではGitHub Actionsから逐次的にAPIを呼び出す設計のため問題ありませんが、
 * 複数の同時リクエストがある場合は、Redis (Upstash) などの
 * 外部ストアを使用したレート制限を検討してください。
 *
 * @see https://vercel.com/docs/functions/serverless-functions#cold-starts
 */
export function getJQuantsRateLimiter(): RateLimiter {
  if (!jquantsRateLimiter) {
    jquantsRateLimiter = new RateLimiter({
      requestsPerMinute: 60,  // Light プラン
      minIntervalMs: 1000,    // 1秒間隔
    });
  }
  return jquantsRateLimiter;
}

/**
 * レートリミッターをリセット（テスト用）
 */
export function resetJQuantsRateLimiter(): void {
  jquantsRateLimiter?.reset();
}
