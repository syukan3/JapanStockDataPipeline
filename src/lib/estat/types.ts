/**
 * e-Stat API レスポンス型定義
 *
 * @description e-Stat API v3 の getStatsData レスポンス型
 * @see https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0
 */

// ============================================
// getStatsData レスポンス
// ============================================

/**
 * e-Stat API トップレベルレスポンス
 */
export interface EStatApiResponse {
  GET_STATS_DATA: {
    RESULT: EStatResult;
    PARAMETER: EStatParameter;
    STATISTICAL_DATA: EStatStatisticalData;
  };
}

/**
 * APIステータス
 */
export interface EStatResult {
  STATUS: number;
  ERROR_MSG: string;
  DATE: string;
}

/**
 * リクエストパラメータ（エコーバック）
 */
export interface EStatParameter {
  LANG: string;
  STATS_DATA_ID: string;
  DATA_FORMAT: string;
  [key: string]: unknown;
}

/**
 * 統計データ本体
 */
export interface EStatStatisticalData {
  TABLE_INF: EStatTableInfo;
  CLASS_INF: EStatClassInfo;
  DATA_INF: EStatDataInfo;
}

/**
 * テーブル情報
 */
export interface EStatTableInfo {
  '@id': string;
  STAT_NAME: { '@code': string; $: string };
  GOV_ORG: { '@code': string; $: string };
  STATISTICS_NAME: string;
  TITLE: { '@no': string; $: string } | string;
  [key: string]: unknown;
}

/**
 * 分類情報
 */
export interface EStatClassInfo {
  CLASS_OBJ: EStatClassObj[];
}

/**
 * 分類オブジェクト（カテゴリ軸）
 */
export interface EStatClassObj {
  '@id': string;
  '@name': string;
  CLASS: EStatClass[] | EStatClass;
}

/**
 * 分類要素
 */
export interface EStatClass {
  '@code': string;
  '@name': string;
  '@level'?: string;
  '@parentCode'?: string;
}

/**
 * データ情報
 */
export interface EStatDataInfo {
  NOTE?: EStatNote[];
  VALUE: EStatValue[];
}

/**
 * 注釈
 */
export interface EStatNote {
  '@char': string;
  $: string;
}

/**
 * データ値
 */
export interface EStatValue {
  /** 値（文字列。"-" や "..." は欠損値） */
  $: string;
  /** 時間軸コード */
  '@time': string;
  /** その他の分類軸属性（動的キー） */
  [key: string]: string;
}

// ============================================
// パース済みデータ
// ============================================

/**
 * パース済み e-Stat 観測値
 */
export interface ParsedEStatObservation {
  /** 観測日 (YYYY-MM-DD) — 月次データの場合は月末日 */
  date: string;
  /** 観測値（数値） */
  value: number;
}
