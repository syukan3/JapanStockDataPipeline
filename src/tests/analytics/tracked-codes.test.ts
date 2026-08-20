/**
 * analytics/tracked-codes.ts のユニットテスト
 *
 * 保有導出は JapanStockScouter src/lib/fetch-holdings.ts と同一挙動であることを固定する:
 * ソフトデリート済みポートフォリオの除外 / buy-sell の純数量 > 0 / 複数口座の合算。
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockCreateAdminClient } = vi.hoisted(() => ({
  mockCreateAdminClient: vi.fn(),
}));

vi.mock('@/lib/supabase/admin', () => ({
  createAdminClient: mockCreateAdminClient,
}));

import {
  getTrackedLocalCodes,
  fetchHeldLocalCodes,
  fetchWatchlistLocalCodes,
} from '@/lib/analytics/tracked-codes';

// ---------------------------------------------------------------------------
// portfolio クライアントのモック（テーブルごとにページ単位の結果を返す）
// ---------------------------------------------------------------------------

interface PageResult {
  data: unknown[] | null;
  error: { message: string } | null;
}

interface MockClient {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  from: any;
  /** テーブル別のチェーン呼び出し記録（[method, ...args]） */
  calls: Record<string, unknown[][]>;
}

function createPortfolioClient(pagesByTable: Record<string, PageResult[]>): MockClient {
  const calls: Record<string, unknown[][]> = {};
  const pageIndex: Record<string, number> = {};

  const from = vi.fn((table: string) => {
    calls[table] ??= [];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const chain: any = {};
    for (const method of ['select', 'is', 'in', 'eq', 'order', 'not']) {
      chain[method] = vi.fn((...args: unknown[]) => {
        calls[table].push([method, ...args]);
        return chain;
      });
    }
    chain.range = vi.fn(async (...args: unknown[]) => {
      calls[table].push(['range', ...args]);
      const pages = pagesByTable[table] ?? [{ data: [], error: null }];
      const index = pageIndex[table] ?? 0;
      pageIndex[table] = index + 1;
      return pages[Math.min(index, pages.length - 1)];
    });
    return chain;
  });

  return { from, calls };
}

const okPage = (data: unknown[]): PageResult => ({ data, error: null });

beforeEach(() => {
  mockCreateAdminClient.mockReset();
});

// ---------------------------------------------------------------------------
// fetchHeldLocalCodes
// ---------------------------------------------------------------------------

describe('fetchHeldLocalCodes', () => {
  it('buy - sell の純数量 > 0 の銘柄のみ返す', async () => {
    const client = createPortfolioClient({
      portfolios: [okPage([{ id: 'p1' }])],
      transactions: [
        okPage([
          { local_code: '72030', trade_type: 'buy', quantity: 100 },
          { local_code: '72030', trade_type: 'sell', quantity: 40 },
          // 全部売却（純数量0）は保有外
          { local_code: '99840', trade_type: 'buy', quantity: 50 },
          { local_code: '99840', trade_type: 'sell', quantity: 50 },
          // 売り越し（信用売り等のデータ不整合）も保有外
          { local_code: '68570', trade_type: 'sell', quantity: 10 },
        ]),
      ],
    });

    await expect(fetchHeldLocalCodes(client)).resolves.toEqual(['72030']);
  });

  it('ソフトデリート済みポートフォリオを除外し、残ったIDだけで取引を絞る', async () => {
    const client = createPortfolioClient({
      portfolios: [okPage([{ id: 'p1' }, { id: 'p2' }])],
      transactions: [okPage([{ local_code: '72030', trade_type: 'buy', quantity: 10 }])],
    });

    await fetchHeldLocalCodes(client);

    expect(client.calls.portfolios).toContainEqual(['is', 'deleted_at', null]);
    expect(client.calls.transactions).toContainEqual(['in', 'portfolio_id', ['p1', 'p2']]);
  });

  it('複数ポートフォリオの同一銘柄は合算する', async () => {
    const client = createPortfolioClient({
      portfolios: [okPage([{ id: 'p1' }, { id: 'p2' }])],
      transactions: [
        okPage([
          { local_code: '72030', trade_type: 'buy', quantity: 100 }, // p1
          { local_code: '72030', trade_type: 'sell', quantity: 100 }, // p2 で全部売却 → 合算0
          { local_code: '285A0', trade_type: 'buy', quantity: 1 },
        ]),
      ],
    });

    await expect(fetchHeldLocalCodes(client)).resolves.toEqual(['285A0']);
  });

  it('未削除ポートフォリオが無ければ取引を読まずに空配列', async () => {
    const client = createPortfolioClient({
      portfolios: [okPage([])],
    });

    await expect(fetchHeldLocalCodes(client)).resolves.toEqual([]);
    expect(client.from).toHaveBeenCalledTimes(1);
    expect(client.calls.transactions).toBeUndefined();
  });

  it('quantity が文字列でも数値化する', async () => {
    const client = createPortfolioClient({
      portfolios: [okPage([{ id: 'p1' }])],
      transactions: [
        okPage([
          { local_code: '72030', trade_type: 'buy', quantity: '100' },
          { local_code: '72030', trade_type: 'sell', quantity: '30' },
        ]),
      ],
    });

    await expect(fetchHeldLocalCodes(client)).resolves.toEqual(['72030']);
  });

  it('1000件を超える取引はページングして全件読む', async () => {
    const firstPage = Array.from({ length: 1000 }, () => ({
      local_code: '72030',
      trade_type: 'buy' as const,
      quantity: 1,
    }));
    const client = createPortfolioClient({
      portfolios: [okPage([{ id: 'p1' }])],
      transactions: [okPage(firstPage), okPage([{ local_code: '285A0', trade_type: 'buy', quantity: 5 }])],
    });

    await expect(fetchHeldLocalCodes(client)).resolves.toEqual(['72030', '285A0']);
    const ranges = client.calls.transactions.filter(([method]) => method === 'range');
    expect(ranges).toEqual([
      ['range', 0, 999],
      ['range', 1000, 1999],
    ]);
  });

  it('取得エラーはテーブル名付きで throw', async () => {
    const client = createPortfolioClient({
      portfolios: [{ data: null, error: { message: 'permission denied' } }],
    });

    await expect(fetchHeldLocalCodes(client)).rejects.toThrow(
      'portfolio.portfolios の取得に失敗しました: permission denied'
    );
  });
});

// ---------------------------------------------------------------------------
// fetchWatchlistLocalCodes
// ---------------------------------------------------------------------------

describe('fetchWatchlistLocalCodes', () => {
  it('重複を排除して返す', async () => {
    const client = createPortfolioClient({
      watchlist_items: [okPage([{ local_code: '13060' }, { local_code: '13060' }, { local_code: '16150' }])],
    });

    await expect(fetchWatchlistLocalCodes(client)).resolves.toEqual(['13060', '16150']);
  });

  it('取得エラーはテーブル名付きで throw', async () => {
    const client = createPortfolioClient({
      watchlist_items: [{ data: null, error: { message: 'boom' } }],
    });

    await expect(fetchWatchlistLocalCodes(client)).rejects.toThrow(
      'portfolio.watchlist_items の取得に失敗しました: boom'
    );
  });
});

// ---------------------------------------------------------------------------
// getTrackedLocalCodes
// ---------------------------------------------------------------------------

describe('getTrackedLocalCodes', () => {
  it('保有とウォッチの和集合を重複排除・昇順で返す', async () => {
    const client = createPortfolioClient({
      portfolios: [okPage([{ id: 'p1' }])],
      transactions: [
        okPage([
          { local_code: '72030', trade_type: 'buy', quantity: 100 },
          { local_code: '52020', trade_type: 'buy', quantity: 100 },
        ]),
      ],
      // 72030 は保有とウォッチの両方にある
      watchlist_items: [okPage([{ local_code: '72030' }, { local_code: '13060' }])],
    });

    await expect(getTrackedLocalCodes(client)).resolves.toEqual(['13060', '52020', '72030']);
  });

  it('保有0件でもウォッチ銘柄は追跡対象になる', async () => {
    const client = createPortfolioClient({
      portfolios: [okPage([])],
      watchlist_items: [okPage([{ local_code: '16150' }])],
    });

    await expect(getTrackedLocalCodes(client)).resolves.toEqual(['16150']);
  });

  it('クライアント未指定なら portfolio スキーマの service_role クライアントを使う', async () => {
    const client = createPortfolioClient({
      portfolios: [okPage([])],
      watchlist_items: [okPage([{ local_code: '16150' }])],
    });
    mockCreateAdminClient.mockReturnValue(client);

    await expect(getTrackedLocalCodes()).resolves.toEqual(['16150']);
    expect(mockCreateAdminClient).toHaveBeenCalledWith('portfolio');
  });
});
