import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  RateLimiter,
  getJQuantsRateLimiter,
  resetJQuantsRateLimiter,
} from '@/lib/jquants/rate-limiter';

describe('rate-limiter.ts', () => {
  describe('RateLimiter', () => {
    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2024-01-15T00:00:00.000Z'));
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    describe('constructor', () => {
      it('デフォルト設定で60トークンを持つ', () => {
        const limiter = new RateLimiter();
        expect(limiter.availableTokens).toBe(60);
      });

      it('カスタム設定が反映される', () => {
        const limiter = new RateLimiter({ requestsPerMinute: 30 });
        expect(limiter.availableTokens).toBe(30);
      });
    });

    describe('acquire', () => {
      it('トークンがある場合は即時取得できる', async () => {
        const limiter = new RateLimiter({
          requestsPerMinute: 60,
          minIntervalMs: 0,
        });

        const startTime = Date.now();
        await limiter.acquire();
        const elapsed = Date.now() - startTime;

        expect(elapsed).toBe(0);
        expect(limiter.availableTokens).toBe(59);
      });

      it('最小間隔を遵守する', async () => {
        const limiter = new RateLimiter({
          requestsPerMinute: 60,
          minIntervalMs: 1000,
        });

        // 1回目の取得
        await limiter.acquire();
        expect(limiter.availableTokens).toBe(59);

        // 2回目の取得（1秒待機が必要）
        const acquirePromise = limiter.acquire();

        // 1秒進める
        await vi.advanceTimersByTimeAsync(1000);

        await acquirePromise;
        // 1秒経過でトークンが1つ補充されている（60/分 = 1/秒）ので59のまま
        // 59 + 1（補充） - 1（消費） = 59
        expect(limiter.availableTokens).toBe(59);
      });

      it('トークン枯渇時は補充まで待機する', async () => {
        const limiter = new RateLimiter({
          requestsPerMinute: 2,
          minIntervalMs: 0,
        });

        // 2トークンを消費
        await limiter.acquire();
        await limiter.acquire();
        expect(limiter.availableTokens).toBe(0);

        // 3回目の取得（トークン補充待ち）
        const acquirePromise = limiter.acquire();

        // 30秒進める（1トークン補充）
        await vi.advanceTimersByTimeAsync(30000);

        await acquirePromise;
        // 補充されたトークンを消費
        expect(limiter.availableTokens).toBe(0);
      });

      it('連続取得でトークンが正しく消費される', async () => {
        const limiter = new RateLimiter({
          requestsPerMinute: 60,
          minIntervalMs: 0,
        });

        await limiter.acquire();
        await limiter.acquire();
        await limiter.acquire();

        expect(limiter.availableTokens).toBe(57);
      });
    });

    describe('availableTokens', () => {
      it('時間経過でトークンが補充される', async () => {
        const limiter = new RateLimiter({
          requestsPerMinute: 60,
          minIntervalMs: 0,
        });

        // 60トークンすべて消費
        for (let i = 0; i < 60; i++) {
          await limiter.acquire();
        }
        expect(limiter.availableTokens).toBe(0);

        // 30秒進める → 30トークン補充
        await vi.advanceTimersByTimeAsync(30000);
        expect(limiter.availableTokens).toBe(30);
      });

      it('最大容量を超えない', async () => {
        const limiter = new RateLimiter({
          requestsPerMinute: 60,
          minIntervalMs: 0,
        });

        // 初期状態: 60トークン
        expect(limiter.availableTokens).toBe(60);

        // 1分経過しても60を超えない
        await vi.advanceTimersByTimeAsync(60000);
        expect(limiter.availableTokens).toBe(60);
      });
    });

    describe('reset', () => {
      it('トークンを最大値にリセットする', async () => {
        const limiter = new RateLimiter({
          requestsPerMinute: 60,
          minIntervalMs: 0,
        });

        // トークンを消費
        await limiter.acquire();
        await limiter.acquire();
        expect(limiter.availableTokens).toBe(58);

        // リセット
        limiter.reset();
        expect(limiter.availableTokens).toBe(60);
      });
    });

    describe('並行呼び出し', () => {
      it('並行acquireでトークンが正しく消費される', async () => {
        const limiter = new RateLimiter({
          requestsPerMinute: 60,
          minIntervalMs: 0,
        });

        // 5つの並行リクエスト
        const promises = [
          limiter.acquire(),
          limiter.acquire(),
          limiter.acquire(),
          limiter.acquire(),
          limiter.acquire(),
        ];

        await Promise.all(promises);

        // 5トークン消費されているはず
        expect(limiter.availableTokens).toBe(55);
      });
    });
  });

  describe('getJQuantsRateLimiter', () => {
    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2024-01-15T00:00:00.000Z'));
      // シングルトンをリセット
      resetJQuantsRateLimiter();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('シングルトンインスタンスを返す', () => {
      const limiter1 = getJQuantsRateLimiter();
      const limiter2 = getJQuantsRateLimiter();
      expect(limiter1).toBe(limiter2);
    });

    it('Light設定（60/分, 1秒間隔）が適用されている', async () => {
      const limiter = getJQuantsRateLimiter();
      expect(limiter.availableTokens).toBe(60);

      // 最小間隔1秒の確認
      await limiter.acquire();
      expect(limiter.availableTokens).toBe(59);

      const acquirePromise = limiter.acquire();
      await vi.advanceTimersByTimeAsync(1000);
      await acquirePromise;

      // 1秒経過でトークンが1つ補充されている（60/分 = 1/秒）
      // 59 + 1（補充） - 1（消費） = 59
      expect(limiter.availableTokens).toBe(59);
    });
  });

  describe('resetJQuantsRateLimiter', () => {
    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2024-01-15T00:00:00.000Z'));
      // シングルトンをリセット
      resetJQuantsRateLimiter();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('トークンが復元される', async () => {
      const limiter = getJQuantsRateLimiter();

      // トークンを消費（minIntervalMs=0 ではないのでawait必要）
      await limiter.acquire();
      expect(limiter.availableTokens).toBe(59);

      // リセット
      resetJQuantsRateLimiter();

      expect(limiter.availableTokens).toBe(60);
    });
  });
});
