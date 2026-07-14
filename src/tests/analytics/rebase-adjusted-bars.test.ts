/**
 * analytics/rebase-adjusted-bars.ts のユニットテスト
 *
 * - parseRebaseArgs: モード決定・排他制約・日付/コードの検証
 * - detectEventsInWindow / detectEventsAll: lookback窓・フィルタ・ページング・session重複の正規化
 * - uniqueCodes / rebaseCodes: 重複排除・RPC呼び出し契約・エラー伝播
 */

import { describe, it, expect, vi } from 'vitest';

import {
  parseRebaseArgs,
  getLatestTradeDate,
  detectEventsInWindow,
  detectEventsAll,
  uniqueCodes,
  rebaseCodes,
  subtractDays,
  DETECT_LOOKBACK_DAYS,
  type AdjustmentEvent,
} from '@/lib/analytics/rebase-adjusted-bars';

// ---------------------------------------------------------------------------
// supabase チェーンモック（thenable。await で結果を解決する）
// ---------------------------------------------------------------------------

interface ChainResult {
  data: unknown;
  error: { message: string } | null;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function createChain(result: ChainResult): any {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const chain: any = {};
  const methods = ['select', 'eq', 'neq', 'gte', 'lte', 'not', 'order', 'range', 'limit', 'single'];
  for (const m of methods) {
    chain[m] = vi.fn().mockReturnValue(chain);
  }
  chain.then = (resolve: (v: ChainResult) => void) => resolve(result);
  return chain;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function createCore(chains: any[], rpcResults: ChainResult[] = []): any {
  let fromCall = 0;
  let rpcCall = 0;
  return {
    from: vi.fn(() => chains[Math.min(fromCall++, chains.length - 1)]),
    rpc: vi.fn(async () => rpcResults[Math.min(rpcCall++, rpcResults.length - 1)]),
  };
}

// ---------------------------------------------------------------------------
// parseRebaseArgs
// ---------------------------------------------------------------------------

describe('parseRebaseArgs', () => {
  it('引数なしは検知モード（date未指定・dryRunなし）', () => {
    expect(parseRebaseArgs([])).toEqual({ mode: 'detect', date: undefined, dryRun: false });
  });

  it('--date は検知モードの窓終端日になる', () => {
    expect(parseRebaseArgs(['--date=2026-06-29'])).toEqual({
      mode: 'detect',
      date: '2026-06-29',
      dryRun: false,
    });
  });

  it('--date の形式不正は throw', () => {
    expect(() => parseRebaseArgs(['--date=2026/06/29'])).toThrow('YYYY-MM-DD');
    expect(() => parseRebaseArgs(['--date=2026-13-01'])).toThrow('YYYY-MM-DD');
  });

  it('--code はカンマ区切り・trim・重複排除して codes モードになる', () => {
    expect(parseRebaseArgs(['--code=31100, 72360,31100'])).toEqual({
      mode: 'codes',
      codes: ['31100', '72360'],
      dryRun: false,
    });
  });

  it('--code が空なら throw', () => {
    expect(() => parseRebaseArgs(['--code='])).toThrow('銘柄コード');
    expect(() => parseRebaseArgs(['--code=,,'])).toThrow('銘柄コード');
  });

  it('--all は all モードになる', () => {
    expect(parseRebaseArgs(['--all'])).toEqual({ mode: 'all', dryRun: false });
  });

  it('--dry-run は各モードと併用できる', () => {
    expect(parseRebaseArgs(['--dry-run'])).toEqual({ mode: 'detect', date: undefined, dryRun: true });
    expect(parseRebaseArgs(['--all', '--dry-run'])).toEqual({ mode: 'all', dryRun: true });
    expect(parseRebaseArgs(['--code=31100', '--dry-run'])).toEqual({
      mode: 'codes',
      codes: ['31100'],
      dryRun: true,
    });
  });

  it('--code と --all の併用は throw', () => {
    expect(() => parseRebaseArgs(['--code=31100', '--all'])).toThrow('併用できません');
  });

  it('--date と --code / --all の併用は throw', () => {
    expect(() => parseRebaseArgs(['--date=2026-06-29', '--all'])).toThrow('検知モード専用');
    expect(() => parseRebaseArgs(['--date=2026-06-29', '--code=31100'])).toThrow('検知モード専用');
  });

  it('不明な引数は throw', () => {
    expect(() => parseRebaseArgs(['--unknown'])).toThrow('不明な引数');
  });
});

// ---------------------------------------------------------------------------
// getLatestTradeDate
// ---------------------------------------------------------------------------

describe('getLatestTradeDate', () => {
  it('最新 trade_date を返す', async () => {
    const chain = createChain({ data: { trade_date: '2026-07-14' }, error: null });
    const core = createCore([chain]);
    await expect(getLatestTradeDate(core)).resolves.toBe('2026-07-14');
    expect(chain.order).toHaveBeenCalledWith('trade_date', { ascending: false });
    expect(chain.limit).toHaveBeenCalledWith(1);
  });

  it('行が無ければ throw', async () => {
    const core = createCore([createChain({ data: null, error: { message: 'no rows' } })]);
    await expect(getLatestTradeDate(core)).rejects.toThrow('Failed to get latest trade_date');
  });
});

// ---------------------------------------------------------------------------
// subtractDays / detectEventsInWindow
// ---------------------------------------------------------------------------

describe('subtractDays', () => {
  it('暦日で減算し月・年境界をまたげる', () => {
    expect(subtractDays('2026-07-14', 7)).toBe('2026-07-07');
    expect(subtractDays('2026-07-03', 7)).toBe('2026-06-26');
    expect(subtractDays('2026-01-02', 7)).toBe('2025-12-26');
  });
});

describe('detectEventsInWindow', () => {
  it('終端日から DETECT_LOOKBACK_DAYS 遡る窓で factor≠1 を検知し数値化して返す（numeric文字列対応）', async () => {
    const chain = createChain({
      data: [
        { local_code: '31100', trade_date: '2026-06-29', adjustment_factor: '0.2' },
        { local_code: '72360', trade_date: '2026-06-29', adjustment_factor: 0.1 },
      ],
      error: null,
    });
    const core = createCore([chain]);
    const events = await detectEventsInWindow(core, '2026-06-29');
    expect(events).toEqual([
      { local_code: '31100', trade_date: '2026-06-29', adjustment_factor: 0.2 },
      { local_code: '72360', trade_date: '2026-06-29', adjustment_factor: 0.1 },
    ]);
    expect(chain.gte).toHaveBeenCalledWith(
      'trade_date',
      subtractDays('2026-06-29', DETECT_LOOKBACK_DAYS)
    );
    expect(chain.lte).toHaveBeenCalledWith('trade_date', '2026-06-29');
    expect(chain.not).toHaveBeenCalledWith('adjustment_factor', 'is', null);
    expect(chain.neq).toHaveBeenCalledWith('adjustment_factor', 1);
  });

  it('catch-up 相当: 窓内の複数日にまたがるイベントを全て検知する', async () => {
    // 障害復旧後の forward-fill で 6-26〜6-30 が一括投入されたケース。
    // 最新日(6-30)にイベントが無くても、窓内の 6-26 / 6-29 の権利落ちを取りこぼさない。
    const chain = createChain({
      data: [
        { local_code: '11110', trade_date: '2026-06-26', adjustment_factor: 0.5 },
        { local_code: '31100', trade_date: '2026-06-29', adjustment_factor: 0.2 },
        { local_code: '72360', trade_date: '2026-06-29', adjustment_factor: 0.1 },
      ],
      error: null,
    });
    const core = createCore([chain]);
    const events = await detectEventsInWindow(core, '2026-06-30');
    expect(events.map((e) => e.local_code)).toEqual(['11110', '31100', '72360']);
    expect(chain.gte).toHaveBeenCalledWith('trade_date', '2026-06-23');
    expect(chain.lte).toHaveBeenCalledWith('trade_date', '2026-06-30');
  });

  it('lookbackDays を明示指定できる', async () => {
    const chain = createChain({ data: [], error: null });
    const core = createCore([chain]);
    await detectEventsInWindow(core, '2026-06-30', 3);
    expect(chain.gte).toHaveBeenCalledWith('trade_date', '2026-06-27');
  });

  it('同一 (local_code, trade_date) の複数 session 行は1イベントに正規化する', async () => {
    const chain = createChain({
      data: [
        { local_code: '31100', trade_date: '2026-06-29', adjustment_factor: 0.2 },
        { local_code: '31100', trade_date: '2026-06-29', adjustment_factor: 0.2 },
      ],
      error: null,
    });
    const core = createCore([chain]);
    const events = await detectEventsInWindow(core, '2026-06-29');
    expect(events).toHaveLength(1);
  });

  it('検知0件は空配列', async () => {
    const core = createCore([createChain({ data: [], error: null })]);
    await expect(detectEventsInWindow(core, '2026-07-14')).resolves.toEqual([]);
  });

  it('クエリエラーは throw', async () => {
    const core = createCore([createChain({ data: null, error: { message: 'timeout' } })]);
    await expect(detectEventsInWindow(core, '2026-07-14')).rejects.toThrow(
      'Failed to detect adjustment events in 2026-07-07..2026-07-14: timeout'
    );
  });
});

// ---------------------------------------------------------------------------
// detectEventsAll
// ---------------------------------------------------------------------------

describe('detectEventsAll', () => {
  it('1000行ずつページングし全ページを結合する', async () => {
    const page1 = Array.from({ length: 1000 }, (_, i) => ({
      local_code: String(10000 + i),
      trade_date: '2026-01-05',
      adjustment_factor: 0.5,
    }));
    const page2 = [{ local_code: '99990', trade_date: '2026-06-29', adjustment_factor: 0.2 }];
    const chain1 = createChain({ data: page1, error: null });
    const chain2 = createChain({ data: page2, error: null });
    const core = createCore([chain1, chain2]);

    const events = await detectEventsAll(core);
    expect(events).toHaveLength(1001);
    expect(chain1.range).toHaveBeenCalledWith(0, 999);
    expect(chain2.range).toHaveBeenCalledWith(1000, 1999);
    expect(core.from).toHaveBeenCalledTimes(2);
  });

  it('クエリエラーは throw', async () => {
    const core = createCore([createChain({ data: null, error: { message: 'boom' } })]);
    await expect(detectEventsAll(core)).rejects.toThrow('Failed to detect adjustment events (all): boom');
  });
});

// ---------------------------------------------------------------------------
// uniqueCodes / rebaseCodes
// ---------------------------------------------------------------------------

describe('uniqueCodes', () => {
  it('複数イベント銘柄を重複排除し出現順を維持する', () => {
    const events: AdjustmentEvent[] = [
      { local_code: '31100', trade_date: '2025-03-01', adjustment_factor: 0.5 },
      { local_code: '72360', trade_date: '2026-06-29', adjustment_factor: 0.1 },
      { local_code: '31100', trade_date: '2026-06-29', adjustment_factor: 0.2 },
    ];
    expect(uniqueCodes(events)).toEqual(['31100', '72360']);
  });
});

describe('rebaseCodes', () => {
  it('銘柄ごとに RPC を呼び、更新行数を集約する', async () => {
    const core = createCore([], [
      { data: 370, error: null },
      { data: '365', error: null }, // integer が文字列で返っても数値化
    ]);
    const results = await rebaseCodes(core, ['31100', '72360']);
    expect(results).toEqual([
      { local_code: '31100', updated_rows: 370 },
      { local_code: '72360', updated_rows: 365 },
    ]);
    expect(core.rpc).toHaveBeenNthCalledWith(1, 'rebase_adjusted_bars', { p_local_code: '31100' });
    expect(core.rpc).toHaveBeenNthCalledWith(2, 'rebase_adjusted_bars', { p_local_code: '72360' });
  });

  it('RPC エラーは対象銘柄を含めて throw', async () => {
    const core = createCore([], [{ data: null, error: { message: 'permission denied' } }]);
    await expect(rebaseCodes(core, ['31100'])).rejects.toThrow(
      'rebase_adjusted_bars failed for 31100: permission denied'
    );
  });
});
