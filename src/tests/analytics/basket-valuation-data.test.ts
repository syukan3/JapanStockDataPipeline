/**
 * basket-valuation-data（共有DBローダー・行パーサ）のテスト
 *
 * seed（バックフィル）と cron（日次refresh）が同一の取得・型ゆらぎ吸収を通ることを保証する。
 */
import { describe, it, expect, vi } from 'vitest';
import {
  toNumberOrNull,
  parseBasketConstituentRow,
  toNumericDisclosure,
  fetchDisclosuresGrouped,
  buildPitByCode,
} from '@/lib/analytics/basket-valuation-data';
import type { RawDisclosure } from '@/lib/analytics/basket-valuation';

describe('toNumberOrNull', () => {
  it('null/undefined は null', () => {
    expect(toNumberOrNull(null)).toBeNull();
    expect(toNumberOrNull(undefined)).toBeNull();
  });

  it('数値文字列(PostgRESTのnumeric)を数値に変換する', () => {
    expect(toNumberOrNull('123.45')).toBe(123.45);
  });

  it('数値はそのまま、数値化不能/NaN/Infinity は null', () => {
    expect(toNumberOrNull(42)).toBe(42);
    expect(toNumberOrNull('abc')).toBeNull();
    expect(toNumberOrNull(NaN)).toBeNull();
    expect(toNumberOrNull(Infinity)).toBeNull();
  });
});

describe('parseBasketConstituentRow', () => {
  it('numeric文字列を数値化し、weight_factor欠損は0にフォールバックする', () => {
    expect(
      parseBasketConstituentRow({
        local_code: '80350',
        weight_factor: '1.03980000',
        official_weight: '15.000',
      })
    ).toEqual({ local_code: '80350', weight_factor: 1.0398, official_weight: 15 });
    expect(
      parseBasketConstituentRow({ local_code: 'X', weight_factor: null, official_weight: null })
    ).toEqual({ local_code: 'X', weight_factor: 0, official_weight: null });
  });
});

describe('toNumericDisclosure', () => {
  it('数値列を一括で number|null に統一する', () => {
    const raw = {
      disclosed_date: '2026-04-30',
      disclosed_time: '15:00',
      period_type: 'FY',
      sales: '1000',
      net_income: '100',
      eps: '128.5',
      bps: null,
      dividend_annual: '60',
      forecast_eps: null,
      next_forecast_eps: '130',
      shares_outstanding_fy: '471632733',
      fiscal_year_end: '2026-03-31',
    } as unknown as RawDisclosure;
    const parsed = toNumericDisclosure(raw);
    expect(parsed.sales).toBe(1000);
    expect(parsed.eps).toBe(128.5);
    expect(parsed.bps).toBeNull();
    expect(parsed.next_forecast_eps).toBe(130);
    expect(parsed.shares_outstanding_fy).toBe(471632733);
  });
});

// ============================================================
// fetchDisclosuresGrouped（Supabaseチェーンはthenableでモック）
// ============================================================

interface CoreMock {
  from: ReturnType<typeof vi.fn>;
  chain: Record<string, ReturnType<typeof vi.fn>>;
}

function createCoreMock(pages: { data: unknown[] | null; error: { message: string } | null }[]): CoreMock {
  let call = 0;
  const chain: Record<string, ReturnType<typeof vi.fn>> = {};
  for (const m of ['select', 'in', 'lte', 'order']) {
    chain[m] = vi.fn().mockReturnValue(chain);
  }
  chain.range = vi.fn().mockImplementation(() =>
    Promise.resolve(pages[call++] ?? { data: [], error: null })
  );
  return { from: vi.fn().mockReturnValue(chain), chain };
}

const disclosureRow = (code: string, over: Record<string, unknown> = {}) => ({
  local_code: code,
  disclosed_date: '2026-04-30',
  disclosed_time: '15:00',
  period_type: 'FY',
  sales: '1000',
  net_income: null,
  eps: '100',
  bps: null,
  dividend_annual: null,
  forecast_eps: null,
  next_forecast_eps: null,
  shares_outstanding_fy: '10',
  fiscal_year_end: '2026-03-31',
  ...over,
});

describe('fetchDisclosuresGrouped', () => {
  it('銘柄別にグループ化し、要求コード全キーを（空でも）持ち、numericを数値化する', async () => {
    const core = createCoreMock([
      { data: [disclosureRow('80350'), disclosureRow('68570')], error: null },
    ]);
    const result = await fetchDisclosuresGrouped(core, ['80350', '68570', '61460']);
    expect(result.get('80350')).toHaveLength(1);
    expect(result.get('80350')![0].eps).toBe(100);
    expect(result.get('68570')).toHaveLength(1);
    expect(result.get('61460')).toEqual([]);
  });

  it('1000行のページはフルページとみなし次ページを取得する', async () => {
    const fullPage = Array.from({ length: 1000 }, () => disclosureRow('80350'));
    const core = createCoreMock([
      { data: fullPage, error: null },
      { data: [disclosureRow('80350')], error: null },
    ]);
    const result = await fetchDisclosuresGrouped(core, ['80350']);
    expect(result.get('80350')).toHaveLength(1001);
    expect(core.chain.range).toHaveBeenCalledTimes(2);
    expect(core.chain.range).toHaveBeenNthCalledWith(2, 1000, 1999);
  });

  it('toDate 指定時のみ disclosed_date <= toDate の絞り込みを適用する', async () => {
    const withDate = createCoreMock([{ data: [], error: null }]);
    await fetchDisclosuresGrouped(withDate, ['80350'], '2026-07-17');
    expect(withDate.chain.lte).toHaveBeenCalledWith('disclosed_date', '2026-07-17');

    const withoutDate = createCoreMock([{ data: [], error: null }]);
    await fetchDisclosuresGrouped(withoutDate, ['80350']);
    expect(withoutDate.chain.lte).not.toHaveBeenCalled();
  });

  it('コード0件は問い合わせせず空Mapを返す', async () => {
    const core = createCoreMock([]);
    const result = await fetchDisclosuresGrouped(core, []);
    expect(result.size).toBe(0);
    expect(core.from).not.toHaveBeenCalled();
  });

  it('PostgRESTエラーは例外にする', async () => {
    const core = createCoreMock([{ data: null, error: { message: 'boom' } }]);
    await expect(fetchDisclosuresGrouped(core, ['80350'])).rejects.toThrow(/boom/);
  });
});

describe('buildPitByCode', () => {
  it('銘柄ごとにPIT系列(FY/forward)を構築する', () => {
    const grouped = new Map<string, RawDisclosure[]>([
      ['80350', [toNumericDisclosure(disclosureRow('80350', { next_forecast_eps: '130' }) as unknown as RawDisclosure)]],
      ['61460', []],
    ]);
    const pit = buildPitByCode(grouped);
    expect(pit.get('80350')!.fy).toHaveLength(1);
    expect(pit.get('80350')!.forward).toHaveLength(1);
    expect(pit.get('80350')!.forward[0].targetFyEnd).toBe('2027-03-31');
    expect(pit.get('61460')).toEqual({ fy: [], forward: [] });
  });
});
