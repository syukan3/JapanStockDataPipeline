/**
 * FRED API レスポンス型定義
 *
 * @description FRED (Federal Reserve Economic Data) API のレスポンス型
 * @see https://fred.stlouisfed.org/docs/api/fred/
 */

// ============================================
// series/observations エンドポイント
// ============================================

/**
 * 1観測値（1データポイント）
 */
export interface FredObservation {
  /** 観測値の実質有効開始日 (YYYY-MM-DD) */
  realtime_start: string;
  /** 観測値の実質有効終了日 (YYYY-MM-DD) */
  realtime_end: string;
  /** 観測日 (YYYY-MM-DD) */
  date: string;
  /** 観測値（欠損値の場合は "."） */
  value: string;
}

/**
 * GET /fred/series/observations レスポンス
 */
export interface FredObservationsResponse {
  realtime_start: string;
  realtime_end: string;
  observation_start: string;
  observation_end: string;
  units: string;
  output_type: number;
  file_type: string;
  order_by: string;
  sort_order: string;
  count: number;
  offset: number;
  limit: number;
  observations: FredObservation[];
}

// ============================================
// パース済みデータ
// ============================================

/**
 * パース済み観測値（欠損値フィルタ済み、数値変換済み）
 */
export interface ParsedFredObservation {
  /** 観測日 (YYYY-MM-DD) */
  date: string;
  /** 観測値（数値） */
  value: number;
  /** realtime_start をタイムスタンプ化した公表日時 */
  releasedAt: string;
}
