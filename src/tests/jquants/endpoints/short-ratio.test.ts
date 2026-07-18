/** short-ratio エンドポイントの変換・ウィンドウ計算・sync・契約前ガードを検証する。 */

import { describe, it, expect, vi } from 'vitest';
import type { JQuantsClient } from '@/lib/jquants/client';
import type { ShortRatioItem } from '@/lib/jquants/types';
import { NonRetryableError } from '@/lib/utils/retry';
import {
  toShortSellingSectorRecord,
  resolveShortRatioWindow,
  fetchShortRatio,
  syncShortRatio,
} from '@/lib/jquants/endpoints/short-ratio';

function item(date: string, s33: string, patch: Partial<ShortRatioItem> = {}): ShortRatioItem {
  return {
    Date: date,
    S33: s33,
    SellExShortVa: 1_000_000,
    ShrtWithResVa: 400_000,
    ShrtNoResVa: 100_000,
    ...patch,
  };
}

/** batchUpsert が呼ぶ from().upsert() を捕捉するモック */
function mockSupabase() {
  const upsert = vi.fn((chunk: unknown[], _opts?: unknown) =>
    Promise.resolve({ error: null, count: chunk.length })
  );
  const from = vi.fn().mockReturnValue({ upsert });
  return { client: { from } as never, from, upsert };
}

describe('toShortSellingSectorRecord', () => {
  it('APIフィールドをDB列名へマップする', () => {
    expect(toShortSellingSectorRecord(item('2026-07-15', '0050'))).toEqual({
      as_of_date: '2026-07-15',
      sector33_code: '0050',
      selling_ex_short_value: 1_000_000,
      short_with_restrictions_value: 400_000,
      short_without_restrictions_value: 100_000,
    });
  });

  it('欠損金額（undefined）は null に正規化する', () => {
    const record = toShortSellingSectorRecord({
      Date: '2026-07-15',
      S33: '0050',
      SellExShortVa: 500_000,
    });
    expect(record.short_with_restrictions_value).toBeNull();
    expect(record.short_without_restrictions_value).toBeNull();
    expect(record.selling_ex_short_value).toBe(500_000);
  });
});

describe('resolveShortRatioWindow', () => {
  const baseDate = new Date('2026-07-15T09:00:00+09:00');

  it('デフォルトは baseDate から14日遡る', () => {
    expect(resolveShortRatioWindow({ baseDate })).toEqual({
      from: '2026-07-01',
      to: '2026-07-15',
    });
  });

  it('windowDays を上書きできる', () => {
    expect(resolveShortRatioWindow({ baseDate, windowDays: 3 })).toEqual({
      from: '2026-07-12',
      to: '2026-07-15',
    });
  });

  it('from/to 明示が最優先（windowDays を無視）', () => {
    expect(
      resolveShortRatioWindow({ baseDate, from: '2020-01-01', to: '2020-12-31', windowDays: 3 })
    ).toEqual({ from: '2020-01-01', to: '2020-12-31' });
  });
});

describe('syncShortRatio', () => {
  it('取得行を変換して batchUpsert へ渡す', async () => {
    const client = {
      getShortRatio: vi
        .fn()
        .mockResolvedValue([item('2026-07-15', '0050'), item('2026-07-15', '1050')]),
    } as unknown as JQuantsClient;
    const supa = mockSupabase();

    const result = await syncShortRatio({
      client,
      supabase: supa.client,
      from: '2026-07-01',
      to: '2026-07-15',
    });

    expect(client.getShortRatio).toHaveBeenCalledWith({ from: '2026-07-01', to: '2026-07-15' });
    expect(supa.from).toHaveBeenCalledWith('short_selling_sector');
    expect(supa.upsert).toHaveBeenCalledTimes(1);
    const [chunk, opts] = supa.upsert.mock.calls[0];
    expect(opts).toMatchObject({ onConflict: 'as_of_date,sector33_code' });
    expect(chunk).toEqual([
      expect.objectContaining({ as_of_date: '2026-07-15', sector33_code: '0050' }),
      expect.objectContaining({ as_of_date: '2026-07-15', sector33_code: '1050' }),
    ]);
    expect(result).toMatchObject({ fetched: 2, inserted: 2, from: '2026-07-01', to: '2026-07-15' });
  });

  it('0件なら upsert を呼ばない', async () => {
    const client = { getShortRatio: vi.fn().mockResolvedValue([]) } as unknown as JQuantsClient;
    const supa = mockSupabase();
    const result = await syncShortRatio({ client, supabase: supa.client, from: 'a', to: 'b' });
    expect(supa.upsert).not.toHaveBeenCalled();
    expect(result).toMatchObject({ fetched: 0, inserted: 0 });
  });
});

describe('契約前ガード（fetchShortRatio）', () => {
  it('401/403 は Standard 未契約ヒント付きで再throwする', async () => {
    const client = {
      getShortRatio: vi.fn().mockRejectedValue(new NonRetryableError('HTTP 403: Forbidden', 403)),
    } as unknown as JQuantsClient;
    await expect(fetchShortRatio(client, { from: 'a', to: 'b' })).rejects.toThrow(
      'J-Quants Standard 未契約の可能性'
    );
  });

  it('401 も同様にヒント付きで再throwする', async () => {
    const client = {
      getShortRatio: vi.fn().mockRejectedValue(new NonRetryableError('HTTP 401', 401)),
    } as unknown as JQuantsClient;
    await expect(fetchShortRatio(client, {})).rejects.toThrow('未契約');
  });

  it('401/403 以外のエラーはそのまま伝播する', async () => {
    const client = {
      getShortRatio: vi.fn().mockRejectedValue(new Error('boom')),
    } as unknown as JQuantsClient;
    await expect(fetchShortRatio(client, {})).rejects.toThrow('boom');
    await expect(fetchShortRatio(client, {})).rejects.not.toThrow('未契約');
  });
});
