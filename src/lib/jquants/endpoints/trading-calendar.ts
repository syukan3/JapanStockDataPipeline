/**
 * 取引カレンダー エンドポイント
 *
 * @description J-Quants API V2 /v2/markets/calendar のデータ取得・保存
 * @see https://jpx-jquants.com/en/spec
 */

import { JQuantsClient, createJQuantsClient } from '../client';
import type { TradingCalendarItem, TradingCalendarRecord } from '../types';
import { getSupabaseAdmin } from '../../supabase/admin';
import { POSTGREST_ERROR_CODES } from '../../supabase/errors';
import { batchUpsert } from '../../utils/batch';
import { createLogger, type LogContext } from '../../utils/logger';

const TABLE_NAME = 'trading_calendar';
const ON_CONFLICT = 'calendar_date';

export interface FetchTradingCalendarParams {
  /** 取得開始日 (YYYY-MM-DD) */
  from?: string;
  /** 取得終了日 (YYYY-MM-DD) */
  to?: string;
}

export interface SyncTradingCalendarResult {
  /** 取得件数 */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** エラー一覧 */
  errors: Error[];
}

/**
 * 営業日かどうかを判定
 *
 * @param holDiv 休日区分 (0=非営業日, 1=営業日, 2=半日取引)
 */
export function isBusinessDay(holDiv: string): boolean {
  return holDiv === '1' || holDiv === '2';
}

/**
 * APIレスポンスをDBレコード形式に変換
 */
export function toTradingCalendarRecord(item: TradingCalendarItem): TradingCalendarRecord {
  return {
    calendar_date: item.Date,
    hol_div: item.HolDiv,
    is_business_day: isBusinessDay(item.HolDiv),
    // upsert の DO UPDATE では DEFAULT が効かないので明示する（生成行を公式値で上書きしたとき source も戻す）
    source: 'jquants',
  };
}

/**
 * 取引カレンダーを取得
 *
 * @param client J-Quantsクライアント
 * @param params 取得パラメータ
 */
export async function fetchTradingCalendar(
  client: JQuantsClient,
  params?: FetchTradingCalendarParams
): Promise<TradingCalendarItem[]> {
  const response = await client.getTradingCalendar(params);
  return response.data;
}

/**
 * 取引カレンダーを取得してDBに保存
 *
 * @param params 取得パラメータ
 * @param options オプション
 */
export async function syncTradingCalendar(
  params?: FetchTradingCalendarParams,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncTradingCalendarResult> {
  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });
  const logger = createLogger({ dataset: 'trading_calendar', ...options?.logContext });
  const supabase = getSupabaseAdmin();

  const timer = logger.startTimer('Sync trading calendar');

  try {
    // 1. APIからデータ取得
    logger.info('Fetching trading calendar', { from: params?.from, to: params?.to });
    const items = await fetchTradingCalendar(client, params);

    if (items.length === 0) {
      logger.info('No trading calendar data found');
      timer.end({ fetched: 0, inserted: 0 });
      return { fetched: 0, inserted: 0, errors: [] };
    }

    logger.info('Fetched trading calendar', { rowCount: items.length });

    // 2. DBレコード形式に変換
    const records = items.map(toTradingCalendarRecord);

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

/** rangeDaysの最大値 */
const MAX_RANGE_DAYS = 3650; // 約10年

/**
 * 指定期間の取引カレンダーを同期（±N日）
 *
 * @param baseDate 基準日 (Date オブジェクト)
 * @param rangeDays 前後の日数（デフォルト: 370日、最大: 3650日）
 * @param options オプション
 */
export async function syncTradingCalendarRange(
  baseDate: Date,
  rangeDays: number = 370,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncTradingCalendarResult> {
  // rangeDays の検証
  if (!Number.isFinite(rangeDays)) {
    throw new Error('syncTradingCalendarRange: rangeDays must be a finite number');
  }
  const validRangeDays = Math.min(Math.max(1, Math.floor(rangeDays)), MAX_RANGE_DAYS);

  const from = new Date(baseDate);
  from.setDate(from.getDate() - validRangeDays);

  const to = new Date(baseDate);
  to.setDate(to.getDate() + validRangeDays);

  // Asia/Tokyoタイムゾーンで日付をフォーマット
  const formatDate = (d: Date) => {
    const formatter = new Intl.DateTimeFormat('ja-JP', {
      timeZone: 'Asia/Tokyo',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    });
    const parts = formatter.formatToParts(d);
    const year = parts.find((p) => p.type === 'year')?.value ?? '';
    const month = parts.find((p) => p.type === 'month')?.value ?? '';
    const day = parts.find((p) => p.type === 'day')?.value ?? '';
    return `${year}-${month}-${day}`;
  };

  return syncTradingCalendar(
    {
      from: formatDate(from),
      to: formatDate(to),
    },
    options
  );
}

/** 取引カレンダー取得時の基本カラム（raw_json除外） */
export type TradingCalendarBasicRecord = Pick<
  TradingCalendarRecord,
  'calendar_date' | 'hol_div' | 'is_business_day' | 'ingested_at'
>;

/**
 * DBから取引カレンダーを取得
 *
 * @param calendarDate 日付 (YYYY-MM-DD)
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getTradingCalendarFromDB(
  calendarDate: string,
  options?: { includeRawJson?: boolean }
): Promise<TradingCalendarRecord | TradingCalendarBasicRecord | null> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('calendar_date', calendarDate)
      .single();

    if (error) {
      if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
        return null;
      }
      throw error;
    }
    return data as TradingCalendarRecord;
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select('calendar_date,hol_div,is_business_day,ingested_at')
    .eq('calendar_date', calendarDate)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data as TradingCalendarBasicRecord;
}

/**
 * DBから営業日のみを取得（範囲指定）
 *
 * @param from 開始日 (YYYY-MM-DD)
 * @param to 終了日 (YYYY-MM-DD)
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getBusinessDaysFromDB(
  from: string,
  to: string,
  options?: { includeRawJson?: boolean }
): Promise<TradingCalendarRecord[] | TradingCalendarBasicRecord[]> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .gte('calendar_date', from)
      .lte('calendar_date', to)
      .eq('is_business_day', true)
      .order('calendar_date', { ascending: true });

    if (error) {
      throw error;
    }
    return (data ?? []) as TradingCalendarRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select('calendar_date,hol_div,is_business_day,ingested_at')
    .gte('calendar_date', from)
    .lte('calendar_date', to)
    .eq('is_business_day', true)
    .order('calendar_date', { ascending: true });

  if (error) {
    throw error;
  }

  return (data ?? []) as TradingCalendarBasicRecord[];
}

/**
 * 指定日が営業日かどうかをDBで確認
 *
 * @param calendarDate 日付 (YYYY-MM-DD)
 */
export async function isBusinessDayFromDB(calendarDate: string): Promise<boolean | null> {
  const record = await getTradingCalendarFromDB(calendarDate);
  return record?.is_business_day ?? null;
}
