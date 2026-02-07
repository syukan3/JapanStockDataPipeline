/**
 * kabu STATION REST API レスポンス型定義
 */

/** POST /token レスポンス */
export interface TokenResponse {
  ResultCode: number;
  Token: string;
}

/** GET /margin/marginpremium/{symbol} レスポンス */
export interface MarginPremiumResponse {
  Symbol: string;
  GeneralMargin: MarginDetail;
  SystemMargin: MarginDetail;
}

export interface MarginDetail {
  /** 0: なし, 1: 品貸料, 2: 金利 */
  MarginPremiumType: number;
  /** プレミアム料（円/日） */
  MarginPremium: number | null;
  /** プレミアム料上限 */
  UpperMarginPremium: number | null;
  /** プレミアム料下限 */
  LowerMarginPremium: number | null;
  /** プレミアム料刻み */
  TickMarginPremium: number | null;
}

/** kabu STATION クライアントオプション */
export interface KabuStationClientOptions {
  apiPassword: string;
  baseUrl?: string;
  timeout?: number;
}
