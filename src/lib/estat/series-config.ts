/**
 * e-Stat 取得対象系列の定義
 *
 * @description 2系列の e-Stat 統計表ID とメタ情報
 */

export interface EStatSeriesConfig {
  /** canonical series_id（'estat_' プレフィックス） */
  seriesId: string;
  /** e-Stat statsDataId */
  statsDataId: string;
  /** レスポンスから特定系列を抽出するフィルタ条件 */
  sourceFilter: Record<string, string>;
  /** カテゴリ */
  category: string;
  /** 地域 */
  region: string;
  /** 更新頻度 */
  frequency: 'monthly';
  /** 指標名（英語） */
  nameEn: string;
  /** 指標名（日本語） */
  nameJa: string;
}

/**
 * e-Stat 2系列の定義
 */
export const ESTAT_SERIES: readonly EStatSeriesConfig[] = [
  {
    seriesId: 'estat_ci_leading',
    statsDataId: '0003473620',
    sourceFilter: { cat01: 'CI', cat02: '先行指数' },
    category: 'business_cycle',
    region: 'jp',
    frequency: 'monthly',
    nameEn: 'Composite Index - Leading',
    nameJa: '景気動向指数CI先行',
  },
  {
    seriesId: 'estat_core_cpi',
    statsDataId: '0003421913',
    sourceFilter: { cat01: '生鮮食品を除く総合' },
    category: 'inflation',
    region: 'jp',
    frequency: 'monthly',
    nameEn: 'Core CPI (ex fresh food)',
    nameJa: '消費者物価指数コアCPI',
  },
] as const;
