/**
 * キャッチアップロジック
 *
 * @description 取り込み漏れの営業日を検出して順次処理
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import { createLogger } from '../utils/logger';
import { getJSTDate, addDays } from '../utils/date';
import { getBusinessDays, getPreviousBusinessDay } from './business-day';
import type { JobName } from './job-run';

const logger = createLogger({ module: 'catch-up' });

/** キャッチアップ対象のデータセット */
export type CatchUpDataset =
  | 'calendar'
  | 'equity_bars'
  | 'topix'
  | 'financial'
  | 'equity_master';

export interface CatchUpConfig {
  /** 1回の実行で処理する最大営業日数 */
  maxDays: number;
  /** 遡る最大日数（これより古いデータは取得しない） */
  lookbackDays: number;
}

/**
 * 環境変数からキャッチアップ設定を取得
 */
export function getCatchUpConfig(): CatchUpConfig {
  return {
    maxDays: parseInt(process.env.SYNC_MAX_CATCHUP_DAYS ?? '5', 10),
    lookbackDays: parseInt(process.env.SYNC_LOOKBACK_DAYS ?? '30', 10),
  };
}

/**
 * 未処理の営業日を検出
 *
 * @param supabaseIngest ジョブログ用クライアント（jquants_ingest）
 * @param supabaseCore データ用クライアント（jquants_core）
 * @param jobName ジョブ名
 * @param config キャッチアップ設定
 * @returns 未処理営業日のリスト（古い順）
 */
export async function findMissingBusinessDays(
  supabaseIngest: SupabaseClient,
  supabaseCore: SupabaseClient,
  jobName: JobName,
  config?: CatchUpConfig
): Promise<string[]> {
  const { maxDays, lookbackDays } = config ?? getCatchUpConfig();

  const today = getJSTDate();
  const lookbackStartDate = addDays(today, -lookbackDays);

  // 今日の前営業日を取得（Cron A は前営業日分を処理）
  const previousBusinessDay = await getPreviousBusinessDay(supabaseCore, today);
  if (!previousBusinessDay) {
    logger.warn('Cannot determine previous business day');
    return [];
  }

  // 期間内の営業日を取得
  const businessDays = await getBusinessDays(
    supabaseCore,
    lookbackStartDate,
    previousBusinessDay
  );

  if (businessDays.length === 0) {
    logger.info('No business days in lookback period');
    return [];
  }

  logger.debug('Checking for missing business days', {
    jobName,
    lookbackStartDate,
    previousBusinessDay,
    totalBusinessDays: businessDays.length,
  });

  // 期間内の成功した job_runs を一括取得（N+1クエリ回避）
  const { data: existingRuns, error } = await supabaseIngest
    .from('job_runs')
    .select('target_date')
    .eq('job_name', jobName)
    .eq('status', 'success')
    .in('target_date', businessDays);

  if (error) {
    logger.error('Failed to fetch existing job runs', { jobName, error });
    return [];
  }

  const existingDates = new Set(
    existingRuns?.map((r) => (r as Record<string, string>).target_date) ?? []
  );

  // 未処理の営業日を抽出（最大maxDays件）
  const missingDays = businessDays
    .filter((day) => !existingDates.has(day))
    .slice(0, maxDays);

  if (missingDays.length > 0) {
    logger.info('Found missing business days', {
      jobName,
      count: missingDays.length,
      dates: missingDays,
    });
  }

  return missingDays;
}

/**
 * データテーブルで未処理の日付を検出（job_runs に依存しない方式）
 *
 * @description equity_bar_daily などの実データから欠落を検出
 */
export async function findMissingDatesInTable(
  supabaseCore: SupabaseClient,
  table: string,
  dateColumn: string,
  startDate: string,
  endDate: string
): Promise<string[]> {
  // 期間内の営業日を取得
  const businessDays = await getBusinessDays(supabaseCore, startDate, endDate);

  if (businessDays.length === 0) {
    return [];
  }

  // テーブルに存在する日付を取得
  const { data, error } = await supabaseCore
    .from(table)
    .select(dateColumn)
    .gte(dateColumn, startDate)
    .lte(dateColumn, endDate)
    .order(dateColumn);

  if (error) {
    logger.error('Failed to query table for missing dates', { table, error });
    return [];
  }

  // データから日付を抽出
  const rows = data as unknown as Array<Record<string, string>> | null;
  const existingDates = new Set(
    rows?.map((row) => row[dateColumn]) ?? []
  );

  return businessDays.filter((day) => !existingDates.has(day));
}

/**
 * キャッチアップが必要かどうかを判定
 *
 * @param supabaseIngest ジョブログ用クライアント
 * @param supabaseCore データ用クライアント
 * @param jobName ジョブ名
 */
export async function needsCatchUp(
  supabaseIngest: SupabaseClient,
  supabaseCore: SupabaseClient,
  jobName: JobName
): Promise<boolean> {
  const missingDays = await findMissingBusinessDays(
    supabaseIngest,
    supabaseCore,
    jobName,
    { maxDays: 1, lookbackDays: 7 } // 軽量チェック
  );

  return missingDays.length > 0;
}

/**
 * 最後に成功した処理日を取得
 *
 * @param supabaseIngest ジョブログ用クライアント
 * @param jobName ジョブ名
 */
export async function getLastSuccessfulDate(
  supabaseIngest: SupabaseClient,
  jobName: JobName
): Promise<string | null> {
  const { data, error } = await supabaseIngest
    .from('job_runs')
    .select('target_date')
    .eq('job_name', jobName)
    .eq('status', 'success')
    .not('target_date', 'is', null)
    .order('target_date', { ascending: false })
    .limit(1)
    .single();

  if (error) {
    if (error.code === 'PGRST116') {
      return null;
    }
    logger.error('Failed to get last successful date', { jobName, error });
    return null;
  }

  return data?.target_date ?? null;
}

/**
 * 処理対象日を決定
 *
 * @description キャッチアップ対象があればそれを、なければ前営業日を返す
 */
export async function determineTargetDates(
  supabaseIngest: SupabaseClient,
  supabaseCore: SupabaseClient,
  jobName: JobName,
  config?: CatchUpConfig
): Promise<string[]> {
  const missingDays = await findMissingBusinessDays(
    supabaseIngest,
    supabaseCore,
    jobName,
    config
  );

  if (missingDays.length > 0) {
    return missingDays;
  }

  // キャッチアップ不要の場合、今日の前営業日を返す
  const today = getJSTDate();
  const previousBusinessDay = await getPreviousBusinessDay(supabaseCore, today);

  if (previousBusinessDay) {
    return [previousBusinessDay];
  }

  return [];
}
