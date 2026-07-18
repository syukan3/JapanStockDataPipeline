/**
 * バスケット割安判定の共有DBローダー・行パーサ
 *
 * @description scripts/seed/basket-valuation.ts（バックフィル）と
 * scripts/cron/refresh-basket-metrics.ts（日次refresh）の両方から使用する。
 * 計算本体は basket-valuation.ts（純関数）に集約し、ここは PostgREST の
 * 取得・型ゆらぎ吸収のみを担当する。
 */

import { buildPitFinancials, type PitFinancials, type RawDisclosure } from './basket-valuation';

// Supabaseクライアントはスキーマ束縛の動的型のため any で受ける（repo慣習）
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = any;

/** PostgREST の numeric（文字列/数値/null混在）を number|null に統一する */
export function toNumberOrNull(v: unknown): number | null {
  if (v == null) return null;
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

// ============================================================
// basket_constituents
// ============================================================

/** analytics.basket_constituents の生行（valid_to is null の現行行のみ想定） */
export interface RawBasketConstituentRow {
  local_code: string;
  weight_factor: unknown;
  official_weight: unknown;
}

export interface BasketConstituent {
  local_code: string;
  weight_factor: number;
  official_weight: number | null;
}

export function parseBasketConstituentRow(raw: RawBasketConstituentRow): BasketConstituent {
  return {
    local_code: raw.local_code,
    weight_factor: toNumberOrNull(raw.weight_factor) ?? 0,
    official_weight: toNumberOrNull(raw.official_weight),
  };
}

// ============================================================
// financial_disclosure（PIT 素材）
// ============================================================

const DISCLOSURE_COLUMNS =
  'local_code, disclosed_date, disclosed_time, period_type, sales, net_income, eps, bps, ' +
  'dividend_annual, forecast_eps, next_forecast_eps, shares_outstanding_fy, fiscal_year_end';

/** PostgREST の numeric が文字列で返る環境差を吸収 */
export function toNumericDisclosure(row: RawDisclosure): RawDisclosure {
  return {
    ...row,
    sales: toNumberOrNull(row.sales),
    net_income: toNumberOrNull(row.net_income),
    eps: toNumberOrNull(row.eps),
    bps: toNumberOrNull(row.bps),
    dividend_annual: toNumberOrNull(row.dividend_annual),
    forecast_eps: toNumberOrNull(row.forecast_eps),
    next_forecast_eps: toNumberOrNull(row.next_forecast_eps),
    shares_outstanding_fy: toNumberOrNull(row.shares_outstanding_fy),
  };
}

/**
 * 構成銘柄の全開示行を disclosed_date 昇順で取得し銘柄別にグループ化する。
 * ページングは disclosure_id を最終タイブレークにして決定的にする。
 *
 * @param toDate 指定時は disclosed_date <= toDate に制限（日次refreshのPIT参照用）
 */
export async function fetchDisclosuresGrouped(
  core: Client,
  codes: string[],
  toDate?: string
): Promise<Map<string, RawDisclosure[]>> {
  const byCode = new Map<string, RawDisclosure[]>();
  for (const code of codes) byCode.set(code, []);
  if (codes.length === 0) return byCode;

  const PAGE_SIZE = 1000;
  for (let page = 0; ; page++) {
    let query = core
      .from('financial_disclosure')
      .select(DISCLOSURE_COLUMNS)
      .in('local_code', codes);
    if (toDate) query = query.lte('disclosed_date', toDate);
    const { data, error } = await query
      .order('disclosed_date', { ascending: true })
      .order('disclosed_time', { ascending: true })
      .order('disclosure_id', { ascending: true })
      .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);
    if (error) throw new Error(`financial_disclosure fetch failed: ${error.message}`);

    const rows = (data ?? []) as (RawDisclosure & { local_code: string })[];
    for (const row of rows) {
      const { local_code: localCode, ...rest } = row;
      byCode.get(localCode)?.push(toNumericDisclosure(rest));
    }
    if (rows.length < PAGE_SIZE) break;
  }
  return byCode;
}

/** 銘柄別の開示行から PIT 参照系列を構築する */
export function buildPitByCode(
  disclosuresByCode: Map<string, RawDisclosure[]>
): Map<string, PitFinancials> {
  const pitByCode = new Map<string, PitFinancials>();
  for (const [code, rows] of disclosuresByCode) {
    pitByCode.set(code, buildPitFinancials(rows));
  }
  return pitByCode;
}
