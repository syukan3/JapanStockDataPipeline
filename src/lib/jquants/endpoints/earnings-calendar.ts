/**
 * 決算発表予定 エンドポイント
 *
 * @description J-Quants API V2 /v2/equities/earnings-calendar のデータ取得・保存
 * @see https://jpx-jquants.com/en/spec
 *
 * NOTE: このAPIは「翌営業日」の決算発表予定を返す。
 * パラメータなしで呼び出し、返却される日付が翌営業日となる。
 */

import { JQuantsClient, createJQuantsClient } from '../client';
import type { EarningsCalendarItem, EarningsCalendarRecord } from '../types';
import { getSupabaseAdmin } from '../../supabase/admin';
import { POSTGREST_ERROR_CODES } from '../../supabase/errors';
import { createLogger, type LogContext } from '../../utils/logger';

const TABLE_NAME = 'earnings_calendar';

export interface SyncEarningsCalendarResult {
  /** 取得件数 */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** 決算発表日 */
  announcementDate: string | null;
  /** エラー一覧 */
  errors: Error[];
  /** API応答を受け取った時刻 */
  sourceObservedAt: string;
}

interface SyncEarningsCalendarOptions {
  client?: JQuantsClient;
  logContext?: LogContext;
  /** Cron Bが trading_calendar から事前に固定した対象日 */
  expectedAnnouncementDate: string;
  /** job_runs.run_id */
  runId: string;
  /** reclaimごとに更新されるfencing token */
  attemptId: string;
}

/** API観測後の永続化失敗でも、failed coverageへ観測情報を伝える。 */
export class EarningsCalendarSyncError extends Error {
  readonly fetched: number;
  readonly inserted: number;
  readonly sourceObservedAt: string | null;

  constructor(
    message: string,
    details: {
      fetched: number;
      inserted: number;
      sourceObservedAt: string | null;
    },
    options?: ErrorOptions
  ) {
    super(message, options);
    this.name = 'EarningsCalendarSyncError';
    this.fetched = details.fetched;
    this.inserted = details.inserted;
    this.sourceObservedAt = details.sourceObservedAt;
  }
}

/**
 * APIレスポンスをDBレコード形式に変換
 */
export function toEarningsCalendarRecord(item: EarningsCalendarItem): EarningsCalendarRecord {
  return {
    announcement_date: item.Date,
    local_code: item.Code,
    company_name: item.CoName,
    fiscal_year: item.FY,
    fiscal_quarter: item.FQ,
    sector_name: item.SectorNm,
  };
}

/**
 * 決算発表予定を取得
 *
 * @param client J-Quantsクライアント
 */
export async function fetchEarningsCalendar(
  client: JQuantsClient
): Promise<EarningsCalendarItem[]> {
  const response = await client.getEarningsCalendar();
  return response.data;
}

/**
 * 決算発表予定を取得してDBに保存
 *
 * @param options オプション
 */
export async function syncEarningsCalendar(
  options: SyncEarningsCalendarOptions
): Promise<SyncEarningsCalendarResult> {
  if (!options?.expectedAnnouncementDate) {
    throw new Error('expectedAnnouncementDate is required for earnings calendar sync');
  }
  if (!options.runId || !options.attemptId) {
    throw new Error('runId and attemptId are required for fenced earnings calendar sync');
  }

  const client = options.client ?? createJQuantsClient({ logContext: options.logContext });
  const logger = createLogger({ dataset: 'earnings_calendar', ...options.logContext });
  const supabase = getSupabaseAdmin();

  const timer = logger.startTimer('Sync earnings calendar');
  let fetched = 0;
  let sourceObservedAt: string | null = null;

  try {
    // 1. APIからデータ取得
    logger.info('Fetching earnings calendar');
    const items = await fetchEarningsCalendar(client);
    fetched = items.length;
    sourceObservedAt = new Date().toISOString();

    // 決算発表日を取得（全て同じ日付のはずだが検証する）
    const firstDate = items[0]?.Date ?? null;
    const allDatesMatch = items.every((item) => item.Date === firstDate);
    const uniqueDates = [...new Set(items.map((item) => item.Date))];

    if (!allDatesMatch) {
      logger.warn('Earnings calendar contains multiple dates', { uniqueDates });
    }

    const announcementDate = allDatesMatch ? firstDate : null;

    // Cron B 経路は、DBを書き換える前に応答全行の日付を検証する。
    // 検証失敗で不正な日付の行を永続化しない。
    if (items.length > 0) {
      if (!allDatesMatch) {
        throw new Error(
          `Earnings calendar response contains multiple announcement dates: ${uniqueDates.join(', ')}`
        );
      }
      if (announcementDate !== options.expectedAnnouncementDate) {
        throw new Error(
          `Earnings calendar target date mismatch: expected ${options.expectedAnnouncementDate}, got ${announcementDate}`
        );
      }
    }

    logger.info('Fetched earnings calendar', {
      rowCount: items.length,
      announcementDate,
      allDatesMatch,
    });

    // 2. DBレコード形式に変換
    const records = items.map(toEarningsCalendarRecord);

    // 3. 現在のattempt所有権をjob row lockで検証し、対象日全置換、
    // 実件数確認、coverage success公開をDBの1トランザクションで確定する。
    const { data: committedCount, error: commitError } = await supabase.rpc(
      'commit_earnings_calendar_attempt',
      {
        p_target_date: options.expectedAnnouncementDate,
        p_run_id: options.runId,
        p_attempt_id: options.attemptId,
        p_source_observed_at: sourceObservedAt,
        p_records: records,
      }
    );

    if (commitError) {
      throw new Error(
        `Failed to commit earnings calendar for ${options.expectedAnnouncementDate}: ${commitError.message}`
      );
    }

    const inserted = Number(committedCount);
    if (!Number.isSafeInteger(inserted) || inserted < 0) {
      throw new Error(
        `Invalid earnings calendar commit count for ${options.expectedAnnouncementDate}: ${String(committedCount)}`
      );
    }
    if (inserted !== items.length) {
      throw new Error(
        `Earnings calendar row count mismatch for ${options.expectedAnnouncementDate}: expected ${items.length}, got ${inserted}`
      );
    }

    timer.end({
      fetched: items.length,
      inserted,
      announcementDate,
    });

    return {
      fetched: items.length,
      inserted,
      announcementDate,
      errors: [],
      sourceObservedAt,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    if (error instanceof EarningsCalendarSyncError) {
      throw error;
    }
    throw new EarningsCalendarSyncError(
      error instanceof Error ? error.message : String(error),
      { fetched, inserted: 0, sourceObservedAt },
      { cause: error }
    );
  }
}

/**
 * DBから決算発表予定を取得（日付指定）
 *
 * @param announcementDate 決算発表日 (YYYY-MM-DD)
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getEarningsCalendarByDateFromDB(
  announcementDate: string,
  options?: { includeRawJson?: boolean }
): Promise<EarningsCalendarRecord[] | EarningsCalendarBasicRecord[]> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('announcement_date', announcementDate)
      .order('local_code', { ascending: true });

    if (error) {
      throw error;
    }
    return (data ?? []) as EarningsCalendarRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('announcement_date', announcementDate)
    .order('local_code', { ascending: true });

  if (error) {
    throw error;
  }

  return (data ?? []) as EarningsCalendarBasicRecord[];
}

/** 決算発表予定取得時の基本カラム（raw_json除外） */
export type EarningsCalendarBasicRecord = Pick<
  EarningsCalendarRecord,
  | 'announcement_date'
  | 'local_code'
  | 'company_name'
  | 'fiscal_year'
  | 'fiscal_quarter'
  | 'sector_name'
  | 'ingested_at'
>;

/** 基本カラムのSELECT文字列 */
const BASIC_COLUMNS =
  'announcement_date,local_code,company_name,fiscal_year,fiscal_quarter,sector_name,ingested_at';

/** limit の最大値（銘柄別） */
const MAX_CODE_LIMIT = 1000;

/**
 * DBから決算発表予定を取得（銘柄指定）
 *
 * @param localCode 銘柄コード (5桁)
 * @param limit 取得件数（デフォルト: 10、最大: 1000）
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getEarningsCalendarByCodeFromDB(
  localCode: string,
  limit: number = 10,
  options?: { includeRawJson?: boolean }
): Promise<EarningsCalendarRecord[] | EarningsCalendarBasicRecord[]> {
  // limit の検証
  if (!Number.isFinite(limit)) {
    throw new Error('getEarningsCalendarByCodeFromDB: limit must be a finite number');
  }
  const validLimit = Math.min(Math.max(1, Math.floor(limit)), MAX_CODE_LIMIT);

  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('local_code', localCode)
      .order('announcement_date', { ascending: false })
      .limit(validLimit);

    if (error) {
      throw error;
    }
    return (data ?? []) as EarningsCalendarRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('local_code', localCode)
    .order('announcement_date', { ascending: false })
    .limit(validLimit);

  if (error) {
    throw error;
  }

  return (data ?? []) as EarningsCalendarBasicRecord[];
}

/** limit の最大値 */
const MAX_UPCOMING_LIMIT = 10000;

/**
 * DBから今後の決算発表予定を取得
 *
 * @param fromDate 開始日 (YYYY-MM-DD)
 * @param limit 取得件数（デフォルト: 100、最大: 10000）
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getUpcomingEarningsFromDB(
  fromDate: string,
  limit: number = 100,
  options?: { includeRawJson?: boolean }
): Promise<EarningsCalendarRecord[] | EarningsCalendarBasicRecord[]> {
  // limit の検証
  if (!Number.isFinite(limit)) {
    throw new Error('getUpcomingEarningsFromDB: limit must be a finite number');
  }
  const validLimit = Math.min(Math.max(1, Math.floor(limit)), MAX_UPCOMING_LIMIT);

  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .gte('announcement_date', fromDate)
      .order('announcement_date', { ascending: true })
      .order('local_code', { ascending: true })
      .limit(validLimit);

    if (error) {
      throw error;
    }
    return (data ?? []) as EarningsCalendarRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .gte('announcement_date', fromDate)
    .order('announcement_date', { ascending: true })
    .order('local_code', { ascending: true })
    .limit(validLimit);

  if (error) {
    throw error;
  }

  return (data ?? []) as EarningsCalendarBasicRecord[];
}

/**
 * DBから決算発表予定の日付範囲を取得
 */
export async function getEarningsCalendarDateRangeFromDB(): Promise<{
  minDate: string | null;
  maxDate: string | null;
}> {
  const supabase = getSupabaseAdmin();

  // 最小日付と最大日付を並列取得
  const [minResult, maxResult] = await Promise.all([
    supabase
      .from(TABLE_NAME)
      .select('announcement_date')
      .order('announcement_date', { ascending: true })
      .limit(1)
      .single(),
    supabase
      .from(TABLE_NAME)
      .select('announcement_date')
      .order('announcement_date', { ascending: false })
      .limit(1)
      .single(),
  ]);

  if (minResult.error && minResult.error.code !== POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
    throw minResult.error;
  }

  if (maxResult.error && maxResult.error.code !== POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
    throw maxResult.error;
  }

  return {
    minDate: minResult.data?.announcement_date ?? null,
    maxDate: maxResult.data?.announcement_date ?? null,
  };
}

/**
 * DBから指定日の決算発表銘柄数を取得
 *
 * @param announcementDate 決算発表日 (YYYY-MM-DD)
 */
export async function countEarningsByDateFromDB(announcementDate: string): Promise<number> {
  const supabase = getSupabaseAdmin();

  const { count, error } = await supabase
    .from(TABLE_NAME)
    .select('*', { count: 'exact', head: true })
    .eq('announcement_date', announcementDate);

  if (error) {
    throw error;
  }

  return count ?? 0;
}

/**
 * @deprecated raw_jsonは削除されました。レコードから直接フィールドを参照してください。
 */
export function extractEarningsData(record: EarningsCalendarRecord): Partial<EarningsCalendarItem> {
  return {
    Date: record.announcement_date,
    Code: record.local_code,
    CoName: record.company_name,
    FY: record.fiscal_year,
    FQ: record.fiscal_quarter,
    SectorNm: record.sector_name,
  };
}

/**
 * DBに決算発表予定が既に存在するか確認
 *
 * @param announcementDate 決算発表日 (YYYY-MM-DD)
 */
export async function hasEarningsCalendarForDate(announcementDate: string): Promise<boolean> {
  const count = await countEarningsByDateFromDB(announcementDate);
  return count > 0;
}
