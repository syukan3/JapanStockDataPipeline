/**
 * TOPIX（指数四本値） エンドポイント
 *
 * @description J-Quants API V2 /v2/indices/bars/daily/topix のデータ取得・保存
 * @see https://jpx-jquants.com/en/spec
 */

import { JQuantsClient, createJQuantsClient } from '../client';
import type { TopixBarDailyItem, TopixBarDailyRecord } from '../types';
import { getSupabaseAdmin } from '../../supabase/admin';
import { POSTGREST_ERROR_CODES } from '../../supabase/errors';
import { batchUpsert } from '../../utils/batch';
import { createLogger, type LogContext } from '../../utils/logger';

const TABLE_NAME = 'topix_bar_daily';
const ON_CONFLICT = 'trade_date';

export interface FetchTopixBarsDailyParams {
  /** 取得開始日 (YYYY-MM-DD) */
  from?: string;
  /** 取得終了日 (YYYY-MM-DD) */
  to?: string;
}

export interface SyncTopixBarsDailyResult {
  /** 取得件数 */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** エラー一覧 */
  errors: Error[];
}

/**
 * APIレスポンスをDBレコード形式に変換
 */
export function toTopixBarDailyRecord(item: TopixBarDailyItem): TopixBarDailyRecord {
  return {
    trade_date: item.Date,
    open: item.O,
    high: item.H,
    low: item.L,
    close: item.C,
    // upsert の DO UPDATE では DEFAULT が効かないので明示する（推計行を公式値で上書きしたとき source も戻す）
    source: 'jquants',
  };
}

/**
 * TOPIXを取得
 *
 * @param client J-Quantsクライアント
 * @param params 取得パラメータ
 */
export async function fetchTopixBarsDaily(
  client: JQuantsClient,
  params?: FetchTopixBarsDailyParams
): Promise<TopixBarDailyItem[]> {
  const response = await client.getTopixBarsDaily(params);
  return response.data;
}

/**
 * TOPIXを取得してDBに保存
 *
 * @param params 取得パラメータ
 * @param options オプション
 */
export async function syncTopixBarsDaily(
  params?: FetchTopixBarsDailyParams,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncTopixBarsDailyResult> {
  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });
  const logger = createLogger({ dataset: 'topix_bar_daily', ...options?.logContext });
  const supabase = getSupabaseAdmin();

  const timer = logger.startTimer('Sync TOPIX bars daily');

  try {
    // 1. APIからデータ取得
    logger.info('Fetching TOPIX bars daily', { from: params?.from, to: params?.to });
    const items = await fetchTopixBarsDaily(client, params);

    if (items.length === 0) {
      logger.info('No TOPIX data found');
      timer.end({ fetched: 0, inserted: 0 });
      return { fetched: 0, inserted: 0, errors: [] };
    }

    logger.info('Fetched TOPIX bars daily', { rowCount: items.length });

    // 2. DBレコード形式に変換
    const records = items.map(toTopixBarDailyRecord);

    // 3. DBに保存
    const result = await batchUpsert(
      supabase,
      TABLE_NAME,
      records,
      ON_CONFLICT,
      {
        onBatchComplete: (batchIndex, inserted, total) => {
          logger.debug('Batch complete', { batchIndex, inserted, total });
        },
      }
    );

    timer.end({
      fetched: items.length,
      inserted: result.inserted,
      batchCount: result.batchCount,
    });

    return {
      fetched: items.length,
      inserted: result.inserted,
      errors: result.errors,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/**
 * 指定期間のTOPIXを同期
 *
 * @param from 開始日 (YYYY-MM-DD)
 * @param to 終了日 (YYYY-MM-DD)
 * @param options オプション
 */
export async function syncTopixBarsDailyForRange(
  from: string,
  to: string,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncTopixBarsDailyResult> {
  return syncTopixBarsDaily({ from, to }, options);
}

/**
 * 指定日のTOPIXを同期
 *
 * @param date 日付 (YYYY-MM-DD)
 * @param options オプション
 */
export async function syncTopixBarsDailyForDate(
  date: string,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncTopixBarsDailyResult> {
  return syncTopixBarsDaily({ from: date, to: date }, options);
}

/** TOPIX取得時の基本カラム（raw_json除外） */
export type TopixBarBasicRecord = Pick<
  TopixBarDailyRecord,
  'trade_date' | 'open' | 'high' | 'low' | 'close' | 'ingested_at'
>;

/** 基本カラムのSELECT文字列 */
const BASIC_COLUMNS = 'trade_date,open,high,low,close,ingested_at';

/**
 * DBからTOPIXを取得（単一日付）
 *
 * @param tradeDate 日付 (YYYY-MM-DD)
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getTopixBarFromDB(
  tradeDate: string,
  options?: { includeRawJson?: boolean }
): Promise<TopixBarDailyRecord | TopixBarBasicRecord | null> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('trade_date', tradeDate)
      .single();

    if (error) {
      if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
        return null;
      }
      throw error;
    }
    return data as TopixBarDailyRecord;
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('trade_date', tradeDate)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data as TopixBarBasicRecord;
}

/**
 * DBからTOPIXを取得（期間指定）
 *
 * @param from 開始日 (YYYY-MM-DD)
 * @param to 終了日 (YYYY-MM-DD)
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getTopixBarsFromDB(
  from: string,
  to: string,
  options?: { includeRawJson?: boolean }
): Promise<TopixBarDailyRecord[] | TopixBarBasicRecord[]> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .gte('trade_date', from)
      .lte('trade_date', to)
      .order('trade_date', { ascending: true });

    if (error) {
      throw error;
    }
    return (data ?? []) as TopixBarDailyRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .gte('trade_date', from)
    .lte('trade_date', to)
    .order('trade_date', { ascending: true });

  if (error) {
    throw error;
  }

  return (data ?? []) as TopixBarBasicRecord[];
}

/**
 * DBから最新のTOPIX日付を取得
 */
export async function getLatestTopixBarDateFromDB(): Promise<string | null> {
  const supabase = getSupabaseAdmin();

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select('trade_date')
    .order('trade_date', { ascending: false })
    .limit(1)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data?.trade_date ?? null;
}

/** 最大取得日数 */
const MAX_RECENT_DAYS = 1000;

/**
 * DBから最新N日分のTOPIXを取得
 *
 * @param days 取得日数（1以上、最大1000）
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getRecentTopixBarsFromDB(
  days: number,
  options?: { includeRawJson?: boolean }
): Promise<TopixBarDailyRecord[] | TopixBarBasicRecord[]> {
  // NaN/Infinityチェック
  if (!Number.isFinite(days)) {
    throw new Error('getRecentTopixBarsFromDB: days must be a finite number');
  }
  // 入力値を正の整数に丸め、上限を設定
  const validDays = Math.min(Math.max(1, Math.floor(days)), MAX_RECENT_DAYS);
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .order('trade_date', { ascending: false })
      .limit(validDays);

    if (error) {
      throw error;
    }
    // 日付昇順に並び替えて返す
    return ((data ?? []) as TopixBarDailyRecord[]).reverse();
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .order('trade_date', { ascending: false })
    .limit(validDays);

  if (error) {
    throw error;
  }

  // 日付昇順に並び替えて返す
  return ((data ?? []) as TopixBarBasicRecord[]).reverse();
}
