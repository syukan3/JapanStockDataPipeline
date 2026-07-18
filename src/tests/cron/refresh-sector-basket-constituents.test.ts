/**
 * refresh-sector-basket-constituents（業種自動導出バスケットの構成銘柄 日次差分更新）のテスト
 *
 * 重点: 00106未適用の防御的スキップ / 差分反映（追加upsert・クローズupdate）/
 *       equity_master 側0件の全クローズ防止 / dry-run。
 * Supabase クライアントは thenable/記録付きオブジェクトでモックする。
 */
import { describe, it, expect, vi } from 'vitest';
import {
  isConstituentSourceMissing,
  listSectorAutoBaskets,
  refreshBasketConstituents,
} from '../../../scripts/cron/refresh-sector-basket-constituents';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyClient = any;

// ============================================================
// isConstituentSourceMissing（00106未適用の検知）
// ============================================================

describe('isConstituentSourceMissing', () => {
  it('Postgres undefined_column(42703) は true', () => {
    expect(isConstituentSourceMissing({ code: '42703' })).toBe(true);
  });

  it('メッセージに constituent_source + does not exist を含めば true', () => {
    expect(
      isConstituentSourceMissing({
        message: 'column basket_definitions.constituent_source does not exist',
      })
    ).toBe(true);
  });

  it('無関係なエラー・null は false', () => {
    expect(isConstituentSourceMissing({ code: 'PGRST116' })).toBe(false);
    expect(isConstituentSourceMissing({ message: 'some other error' })).toBe(false);
    expect(isConstituentSourceMissing(null)).toBe(false);
  });
});

// ============================================================
// listSectorAutoBaskets
// ============================================================

function makeDefsClient(result: { data: unknown; error: unknown }): AnyClient {
  const chain: Record<string, ReturnType<typeof vi.fn>> = {};
  chain.select = vi.fn().mockReturnValue(chain);
  chain.eq = vi.fn().mockReturnValue(chain);
  chain.order = vi.fn().mockResolvedValue(result);
  return { from: vi.fn().mockReturnValue(chain) };
}

describe('listSectorAutoBaskets', () => {
  it('constituent_source 列が無い(42703)なら migration_not_applied を返す', async () => {
    const client = makeDefsClient({ data: null, error: { code: '42703' } });
    expect(await listSectorAutoBaskets(client)).toBe('migration_not_applied');
  });

  it('通常時は sector33_auto バスケットの一覧を返す', async () => {
    const client = makeDefsClient({
      data: [{ basket_id: 'topix33-banks-1615', sector33_filter: '銀行業' }],
      error: null,
    });
    const result = await listSectorAutoBaskets(client);
    expect(result).toEqual([{ basket_id: 'topix33-banks-1615', sector33_filter: '銀行業' }]);
  });

  it('列存在下の他エラーは例外にする', async () => {
    const client = makeDefsClient({ data: null, error: { code: 'XX000', message: 'boom' } });
    await expect(listSectorAutoBaskets(client)).rejects.toThrow(/boom/);
  });
});

// ============================================================
// refreshBasketConstituents（差分反映）
// ============================================================

/** equity_master(is_current) の該当銘柄を返す core モック */
function makeCore(currentCodes: string[]): AnyClient {
  const chain: Record<string, ReturnType<typeof vi.fn>> = {};
  for (const m of ['select', 'eq', 'order']) chain[m] = vi.fn().mockReturnValue(chain);
  chain.range = vi.fn().mockResolvedValue({
    data: currentCodes.map((c) => ({ local_code: c })),
    error: null,
  });
  return { from: vi.fn().mockReturnValue(chain) };
}

/** basket_constituents の select(list)/upsert(add)/update(close) を記録する analytics モック */
function makeAnalytics(existingCodes: string[]) {
  const recorded: { upsertRows: unknown[] | null; closedCodes: string[] | null } = {
    upsertRows: null,
    closedCodes: null,
  };

  const listChain: Record<string, ReturnType<typeof vi.fn>> = {};
  for (const m of ['select', 'eq', 'is', 'order']) listChain[m] = vi.fn().mockReturnValue(listChain);
  listChain.range = vi.fn().mockResolvedValue({
    data: existingCodes.map((c) => ({ local_code: c })),
    error: null,
  });

  const updateChain: Record<string, ReturnType<typeof vi.fn>> = {};
  updateChain.eq = vi.fn().mockReturnValue(updateChain);
  updateChain.is = vi.fn().mockReturnValue(updateChain);
  updateChain.in = vi.fn().mockImplementation((_col: string, codes: string[]) => {
    recorded.closedCodes = codes;
    return Promise.resolve({ error: null });
  });

  const tableApi = {
    select: listChain.select,
    upsert: vi.fn().mockImplementation((rows: unknown[]) => {
      recorded.upsertRows = rows;
      return Promise.resolve({ error: null });
    }),
    update: vi.fn().mockReturnValue(updateChain),
  };

  return { client: { from: vi.fn().mockReturnValue(tableApi) } as AnyClient, recorded };
}

const basket = { basket_id: 'topix33-banks-1615', sector33_filter: '銀行業' };

describe('refreshBasketConstituents', () => {
  it('新規は upsert（weight_factor=1・official_weight=null・valid_from=当日）、消滅は valid_to close', async () => {
    const core = makeCore(['A', 'B', 'C']);
    const { client, recorded } = makeAnalytics(['B', 'C', 'D']);
    const result = await refreshBasketConstituents(core, client, basket, '2026-07-18', false);

    expect(result).toMatchObject({ added: 1, closed: 1, currentCount: 3, existingCount: 3 });
    expect(recorded.upsertRows).toEqual([
      {
        basket_id: 'topix33-banks-1615',
        local_code: 'A',
        weight_factor: 1,
        official_weight: null,
        is_semicon_main: true,
        valid_from: '2026-07-18',
        valid_to: null,
      },
    ]);
    expect(recorded.closedCodes).toEqual(['D']);
  });

  it('変化なしは upsert/update を呼ばず unchanged を返す', async () => {
    const core = makeCore(['A', 'B']);
    const { client, recorded } = makeAnalytics(['B', 'A']);
    const result = await refreshBasketConstituents(core, client, basket, '2026-07-18', false);
    expect(result).toMatchObject({ unchanged: true, added: 0, closed: 0 });
    expect(recorded.upsertRows).toBeNull();
    expect(recorded.closedCodes).toBeNull();
  });

  it('equity_master 側が0件なら全クローズを防ぐため安全側でスキップ', async () => {
    const core = makeCore([]);
    const { client, recorded } = makeAnalytics(['A', 'B']);
    const result = await refreshBasketConstituents(core, client, basket, '2026-07-18', false);
    expect(result).toMatchObject({ skipped: true, reason: 'no_current_constituents' });
    expect(recorded.closedCodes).toBeNull();
  });

  it('dry-run は差分を計算するが書き込まない', async () => {
    const core = makeCore(['A', 'B', 'C']);
    const { client, recorded } = makeAnalytics(['B']);
    const result = await refreshBasketConstituents(core, client, basket, '2026-07-18', true);
    expect(result).toMatchObject({ dryRun: true, wouldAdd: ['A', 'C'], wouldClose: [] });
    expect(recorded.upsertRows).toBeNull();
    expect(recorded.closedCodes).toBeNull();
  });

  it('sector33_filter 未設定はスキップ', async () => {
    const core = makeCore(['A']);
    const { client } = makeAnalytics([]);
    const result = await refreshBasketConstituents(
      core,
      client,
      { basket_id: 'x', sector33_filter: null },
      '2026-07-18',
      false
    );
    expect(result).toMatchObject({ skipped: true, reason: 'no_sector33_filter' });
  });
});
