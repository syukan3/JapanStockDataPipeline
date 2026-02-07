/**
 * 優待クロス取引 共通型定義
 */

/** kabuyutai.com から取得した優待情報 */
export interface YutaiBenefit {
  local_code: string;
  company_name: string;
  min_shares: number;
  benefit_content: string;
  benefit_value: number | null;
  record_month: number;
  record_day: string;
  category: string | null;
}

/** 一般信用売り在庫情報 */
export interface MarginInventory {
  local_code: string;
  broker: string;
  inventory_date: string;
  inventory_qty: number | null;
  is_available: boolean;
  loan_type: string;
  loan_term: string | null;
  premium_fee: number | null;
  source: string;
}

/** Cron E ソース種別 */
export const CRON_E_SOURCES = ['kabuyutai', 'kabu_csv', 'all'] as const;
export type CronESource = (typeof CRON_E_SOURCES)[number];

/** Cron E 結果 */
export interface CronEResult {
  success: boolean;
  source: CronESource;
  benefitsUpserted: number;
  inventoryUpserted: number;
  errors: string[];
}
