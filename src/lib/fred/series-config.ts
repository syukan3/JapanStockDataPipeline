/**
 * FRED 取得対象系列の定義
 *
 * @description 14系列の FRED series_id とメタ情報
 */

export interface FredSeriesConfig {
  /** FRED series_id（canonical ID としても使用） */
  seriesId: string;
  /** カテゴリ */
  category: string;
  /** 地域 */
  region: string;
  /** 更新頻度 */
  frequency: 'daily' | 'weekly' | 'monthly';
  /** 指標名（英語） */
  nameEn: string;
  /** 指標名（日本語） */
  nameJa: string;
}

/**
 * FRED 14系列の定義
 */
export const FRED_SERIES: readonly FredSeriesConfig[] = [
  // 景気
  { seriesId: 'NAPM',              category: 'business_cycle', region: 'us',      frequency: 'monthly', nameEn: 'ISM Manufacturing PMI',           nameJa: 'ISM製造業PMI' },
  { seriesId: 'UNRATE',            category: 'business_cycle', region: 'us',      frequency: 'monthly', nameEn: 'Unemployment Rate',               nameJa: '失業率' },
  // 金融
  { seriesId: 'FEDFUNDS',          category: 'financial',      region: 'us',      frequency: 'monthly', nameEn: 'Federal Funds Rate',              nameJa: 'FF金利' },
  { seriesId: 'NFCI',              category: 'financial',      region: 'us',      frequency: 'weekly',  nameEn: 'Chicago Fed NFCI',                nameJa: 'シカゴ連銀金融環境指数' },
  // インフレ
  { seriesId: 'CPIAUCSL',          category: 'inflation',      region: 'us',      frequency: 'monthly', nameEn: 'CPI All Urban Consumers',         nameJa: 'CPI（都市部全消費者）' },
  { seriesId: 'PCEPILFE',          category: 'inflation',      region: 'us',      frequency: 'monthly', nameEn: 'Core PCE Price Index',            nameJa: 'コアPCE' },
  { seriesId: 'T10YIE',            category: 'inflation',      region: 'us',      frequency: 'daily',   nameEn: '10-Year Breakeven Inflation',     nameJa: '10年BEI' },
  // クレジット
  { seriesId: 'BAMLH0A0HYM2',     category: 'credit',         region: 'us',      frequency: 'daily',   nameEn: 'HY OAS Spread',                  nameJa: 'HYスプレッド（OAS）' },
  { seriesId: 'BAMLC0A4CBBB',     category: 'credit',         region: 'us',      frequency: 'daily',   nameEn: 'IG BBB OAS Spread',               nameJa: 'IGスプレッド（BBB OAS）' },
  // 市場
  { seriesId: 'VIXCLS',            category: 'market',         region: 'us',      frequency: 'daily',   nameEn: 'CBOE VIX',                        nameJa: 'VIX' },
  // 金利
  { seriesId: 'T10Y2Y',            category: 'interest_rate',  region: 'us',      frequency: 'daily',   nameEn: '10Y-2Y Treasury Spread',          nameJa: '10Y-2Yスプレッド' },
  // 日本関連
  { seriesId: 'IRSTCI01JPM156N',   category: 'interest_rate',  region: 'jp',      frequency: 'monthly', nameEn: 'BOJ Policy Rate (via FRED)',      nameJa: '日銀政策金利（FRED経由）' },
  { seriesId: 'IRLTLT01JPM156N',   category: 'interest_rate',  region: 'jp',      frequency: 'monthly', nameEn: 'JGB 10Y Yield (via FRED)',        nameJa: 'JGB10年利回り（FRED経由）' },
  // 為替
  { seriesId: 'DEXJPUS',           category: 'fx',             region: 'linkage', frequency: 'daily',   nameEn: 'USD/JPY Exchange Rate',           nameJa: 'USD/JPY' },
] as const;

/**
 * 月次系列かどうかを判定（vintage再取得対象）
 */
export function isMonthlyOrLower(frequency: string): boolean {
  return frequency === 'monthly' || frequency === 'quarterly';
}
