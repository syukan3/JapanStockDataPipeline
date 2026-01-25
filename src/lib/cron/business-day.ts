/**
 * 営業日判定
 *
 * @description trading_calendar テーブルを使用した営業日判定
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import { createLogger } from '../utils/logger';
import { addDays, getJSTDate } from '../utils/date';

const logger = createLogger({ module: 'business-day' });

/**
 * hol_div から営業日かどうかを判定
 *
 * @param holDiv 休日区分: '0'=非営業日, '1'=営業日, '2'=半日取引
 * @returns 営業日なら true
 */
export function isBusinessDay(holDiv: string): boolean {
  return holDiv === '1' || holDiv === '2';
}

/**
 * 指定日が営業日かどうかをDBから判定
 */
export async function isBusinessDayInDB(
  supabase: SupabaseClient,
  date: string
): Promise<boolean> {
  const { data, error } = await supabase
    .from('trading_calendar')
    .select('hol_div')
    .eq('calendar_date', date)
    .single();

  if (error || !data) {
    logger.warn('Calendar data not found', { date, error });
    return false;
  }

  return isBusinessDay(data.hol_div);
}

/**
 * 前営業日を取得
 *
 * @param supabase Supabase クライアント（jquants_core スキーマ）
 * @param fromDate 基準日（省略時は今日のJST日付）
 * @returns 前営業日（YYYY-MM-DD形式）
 */
export async function getPreviousBusinessDay(
  supabase: SupabaseClient,
  fromDate?: string
): Promise<string | null> {
  const baseDate = fromDate ?? getJSTDate();

  const { data, error } = await supabase
    .from('trading_calendar')
    .select('calendar_date, hol_div')
    .lt('calendar_date', baseDate)
    .order('calendar_date', { ascending: false })
    .limit(10);

  if (error || !data) {
    logger.error('Failed to get previous business day', { fromDate, error });
    return null;
  }

  for (const row of data) {
    if (isBusinessDay(row.hol_div)) {
      return row.calendar_date;
    }
  }

  logger.warn('No previous business day found', { fromDate });
  return null;
}

/**
 * 次営業日を取得
 *
 * @param supabase Supabase クライアント
 * @param fromDate 基準日（省略時は今日のJST日付）
 */
export async function getNextBusinessDay(
  supabase: SupabaseClient,
  fromDate?: string
): Promise<string | null> {
  const baseDate = fromDate ?? getJSTDate();

  const { data, error } = await supabase
    .from('trading_calendar')
    .select('calendar_date, hol_div')
    .gt('calendar_date', baseDate)
    .order('calendar_date', { ascending: true })
    .limit(10);

  if (error || !data) {
    logger.error('Failed to get next business day', { fromDate, error });
    return null;
  }

  for (const row of data) {
    if (isBusinessDay(row.hol_div)) {
      return row.calendar_date;
    }
  }

  logger.warn('No next business day found', { fromDate });
  return null;
}

/**
 * 指定期間の営業日リストを取得
 *
 * @param supabase Supabase クライアント
 * @param fromDate 開始日（含む）
 * @param toDate 終了日（含む）
 */
export async function getBusinessDays(
  supabase: SupabaseClient,
  fromDate: string,
  toDate: string
): Promise<string[]> {
  const { data, error } = await supabase
    .from('trading_calendar')
    .select('calendar_date, hol_div')
    .gte('calendar_date', fromDate)
    .lte('calendar_date', toDate)
    .order('calendar_date', { ascending: true });

  if (error || !data) {
    logger.error('Failed to get business days', { fromDate, toDate, error });
    return [];
  }

  return data
    .filter((row) => isBusinessDay(row.hol_div))
    .map((row) => row.calendar_date);
}

/**
 * N営業日前の日付を取得
 *
 * @param supabase Supabase クライアント
 * @param n 遡る営業日数
 * @param fromDate 基準日
 */
export async function getBusinessDayNDaysAgo(
  supabase: SupabaseClient,
  n: number,
  fromDate?: string
): Promise<string | null> {
  if (n <= 0) {
    return fromDate ?? getJSTDate();
  }

  const baseDate = fromDate ?? getJSTDate();

  // 余裕を持って取得（非営業日が多い期間を考慮）
  const { data, error } = await supabase
    .from('trading_calendar')
    .select('calendar_date, hol_div')
    .lt('calendar_date', baseDate)
    .order('calendar_date', { ascending: false })
    .limit(n * 2 + 10);

  if (error || !data) {
    logger.error('Failed to get business day N days ago', { n, fromDate, error });
    return null;
  }

  const businessDays = data.filter((row) => isBusinessDay(row.hol_div));

  if (businessDays.length < n) {
    logger.warn('Not enough business days found', { n, found: businessDays.length });
    return null;
  }

  return businessDays[n - 1].calendar_date;
}

/**
 * カレンダーの最新日付を取得（データが埋まっている範囲）
 */
export async function getCalendarMaxDate(supabase: SupabaseClient): Promise<string | null> {
  const { data, error } = await supabase
    .from('trading_calendar')
    .select('calendar_date')
    .order('calendar_date', { ascending: false })
    .limit(1)
    .single();

  if (error || !data) {
    logger.error('Failed to get calendar max date', { error });
    return null;
  }

  return data.calendar_date;
}

/**
 * カレンダーの最古日付を取得
 */
export async function getCalendarMinDate(supabase: SupabaseClient): Promise<string | null> {
  const { data, error } = await supabase
    .from('trading_calendar')
    .select('calendar_date')
    .order('calendar_date', { ascending: true })
    .limit(1)
    .single();

  if (error || !data) {
    logger.error('Failed to get calendar min date', { error });
    return null;
  }

  return data.calendar_date;
}

/**
 * カレンダーが十分な範囲をカバーしているかチェック
 *
 * @param supabase Supabase クライアント
 * @param futureDays 未来方向に必要な日数（デフォルト: 370）
 * @param pastDays 過去方向に必要な日数（デフォルト: 370）
 */
export async function checkCalendarCoverage(
  supabase: SupabaseClient,
  futureDays: number = 370,
  pastDays: number = 370
): Promise<{
  ok: boolean;
  minDate: string | null;
  maxDate: string | null;
  requiredMinDate: string;
  requiredMaxDate: string;
}> {
  const today = getJSTDate();
  const requiredMinDate = addDays(today, -pastDays);
  const requiredMaxDate = addDays(today, futureDays);

  // 並列実行で高速化
  const [minDate, maxDate] = await Promise.all([
    getCalendarMinDate(supabase),
    getCalendarMaxDate(supabase),
  ]);

  const ok =
    minDate !== null &&
    maxDate !== null &&
    minDate <= requiredMinDate &&
    maxDate >= requiredMaxDate;

  return {
    ok,
    minDate,
    maxDate,
    requiredMinDate,
    requiredMaxDate,
  };
}
