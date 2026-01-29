/**
 * 株価四本値（日足） エンドポイント
 *
 * @description J-Quants API V2 /v2/equities/bars/daily のデータ取得・保存
 * @see https://jpx-jquants.com/en/spec
 *
 * NOTE: code または date パラメータが必須。ページネーション対応。
 */

import { JQuantsClient, createJQuantsClient } from '../client';
import type { EquityBarDailyItem, EquityBarDailyRecord } from '../types';
import { getSupabaseAdmin } from '../../supabase/admin';
import { POSTGREST_ERROR_CODES } from '../../supabase/errors';
import { batchUpsert } from '../../utils/batch';
import { createLogger, type LogContext } from '../../utils/logger';

const TABLE_NAME = 'equity_bar_daily';
const ON_CONFLICT = 'local_code,trade_date,session';

/** セッション種別 */
export type SessionType = 'DAY' | 'AM' | 'PM';

export interface FetchEquityBarsDailyParams {
  /** 銘柄コード (5桁) */
  code?: string;
  /** 日付 (YYYY-MM-DD) - 特定日のデータを取得 */
  date?: string;
  /** 取得開始日 (YYYY-MM-DD) */
  from?: string;
  /** 取得終了日 (YYYY-MM-DD) */
  to?: string;
}

export interface SyncEquityBarsDailyResult {
  /** 取得件数 */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** ページ数 */
  pageCount: number;
  /** エラー一覧 */
  errors: Error[];
}

/**
 * APIレスポンスをDBレコード形式に変換（通日データ: session = 'DAY'）
 */
export function toEquityBarDailyRecord(
  item: EquityBarDailyItem,
  session: SessionType = 'DAY'
): EquityBarDailyRecord {
  // セッションに応じたフィールドマッピング
  if (session === 'AM') {
    return {
      trade_date: item.Date,
      local_code: item.Code,
      session: 'AM',
      open: item.MO,
      high: item.MH,
      low: item.ML,
      close: item.MC,
      volume: item.MVo,
      turnover_value: item.MVa,
      adjustment_factor: item.AdjFactor,
      adj_open: item.MAdjO,
      adj_high: item.MAdjH,
      adj_low: item.MAdjL,
      adj_close: item.MAdjC,
      adj_volume: item.MAdjVo,
    };
  }

  if (session === 'PM') {
    return {
      trade_date: item.Date,
      local_code: item.Code,
      session: 'PM',
      open: item.AO,
      high: item.AH,
      low: item.AL,
      close: item.AC,
      volume: item.AVo,
      turnover_value: item.AVa,
      adjustment_factor: item.AdjFactor,
      adj_open: item.AAdjO,
      adj_high: item.AAdjH,
      adj_low: item.AAdjL,
      adj_close: item.AAdjC,
      adj_volume: item.AAdjVo,
    };
  }

  // DAY（終日）
  return {
    trade_date: item.Date,
    local_code: item.Code,
    session: 'DAY',
    open: item.O,
    high: item.H,
    low: item.L,
    close: item.C,
    volume: item.Vo,
    turnover_value: item.Va,
    adjustment_factor: item.AdjFactor,
    adj_open: item.AdjO,
    adj_high: item.AdjH,
    adj_low: item.AdjL,
    adj_close: item.AdjC,
    adj_volume: item.AdjVo,
  };
}

/**
 * APIレスポンスをDBレコード形式に変換（全セッション展開）
 *
 * 1つのAPIレスポンスから DAY, AM, PM の3レコードを生成
 * ただし、対応するデータが存在しないセッションは除外
 */
export function toEquityBarDailyRecords(item: EquityBarDailyItem): EquityBarDailyRecord[] {
  const records: EquityBarDailyRecord[] = [];

  // 終日データがあれば追加（null/undefinedを除外）
  if (item.O != null || item.C != null) {
    records.push(toEquityBarDailyRecord(item, 'DAY'));
  }

  // 前場データがあれば追加（null/undefinedを除外）
  if (item.MO != null || item.MC != null) {
    records.push(toEquityBarDailyRecord(item, 'AM'));
  }

  // 後場データがあれば追加（null/undefinedを除外）
  if (item.AO != null || item.AC != null) {
    records.push(toEquityBarDailyRecord(item, 'PM'));
  }

  return records;
}

/**
 * 株価四本値を取得（ページネーション対応・ジェネレータ）
 *
 * @param client J-Quantsクライアント
 * @param params 取得パラメータ
 */
export async function* fetchEquityBarsDailyPaginated(
  client: JQuantsClient,
  params: FetchEquityBarsDailyParams
): AsyncGenerator<EquityBarDailyItem[], void, unknown> {
  yield* client.getEquityBarsDailyPaginated(params);
}

/**
 * 株価四本値を取得（全件取得）
 *
 * @param client J-Quantsクライアント
 * @param params 取得パラメータ
 */
export async function fetchEquityBarsDaily(
  client: JQuantsClient,
  params: FetchEquityBarsDailyParams
): Promise<EquityBarDailyItem[]> {
  return client.getEquityBarsDaily(params);
}

/**
 * 株価四本値を取得してDBに保存
 *
 * @param params 取得パラメータ
 * @param options オプション
 */
export async function syncEquityBarsDaily(
  params: FetchEquityBarsDailyParams,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
    /** 全セッション（DAY/AM/PM）を展開するか（デフォルト: false = DAYのみ） */
    expandSessions?: boolean;
  }
): Promise<SyncEquityBarsDailyResult> {
  // API仕様: code または date が必須
  if (!params.code && !params.date) {
    throw new Error('syncEquityBarsDaily requires either code or date parameter');
  }

  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });
  const logger = createLogger({ dataset: 'equity_bar_daily', ...options?.logContext });
  const supabase = getSupabaseAdmin();
  const expandSessions = options?.expandSessions ?? false;

  const timer = logger.startTimer('Sync equity bars daily');

  try {
    logger.info('Fetching equity bars daily', { ...params, expandSessions });

    let totalFetched = 0;
    let totalInserted = 0;
    let pageCount = 0;
    const allErrors: Error[] = [];

    // ページネーションで順次処理
    for await (const pageItems of client.getEquityBarsDailyPaginated(params)) {
      pageCount++;
      totalFetched += pageItems.length;

      logger.debug('Processing page', { pageCount, pageItems: pageItems.length });

      // DBレコード形式に変換
      const records: EquityBarDailyRecord[] = expandSessions
        ? pageItems.flatMap(toEquityBarDailyRecords)
        : pageItems.map((item) => toEquityBarDailyRecord(item, 'DAY'));

      // DBに保存
      const result = await batchUpsert(
        supabase,
        TABLE_NAME,
        records,
        ON_CONFLICT,
        {
          onBatchComplete: (batchIndex, inserted, total) => {
            logger.debug('Batch complete', { pageCount, batchIndex, inserted, total });
          },
        }
      );

      totalInserted += result.inserted;
      allErrors.push(...result.errors);

      logger.info('Page processed', {
        pageCount,
        fetched: pageItems.length,
        records: records.length,
        inserted: result.inserted,
      });
    }

    timer.end({
      fetched: totalFetched,
      inserted: totalInserted,
      pageCount,
    });

    return {
      fetched: totalFetched,
      inserted: totalInserted,
      pageCount,
      errors: allErrors,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/** チャンク（1ページ）同期の結果 */
export interface SyncEquityBarsDailySinglePageResult {
  /** 取得件数 */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** 次ページのpagination_key（なければ完了） */
  paginationKey?: string;
  /** エラー一覧 */
  errors: Error[];
}

/**
 * 株価四本値を1ページ分だけ取得してDBに保存
 *
 * Vercel Hobbyの10秒制限に対応するため、1回のAPI呼び出しで1ページのみ処理する。
 * GitHub Actionsからpagination_keyを渡してループ呼び出しすることで全ページを処理。
 */
export async function syncEquityBarsDailySinglePage(
  params: FetchEquityBarsDailyParams,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
    expandSessions?: boolean;
    /** 前回のpagination_key（初回はundefined） */
    paginationKey?: string;
  }
): Promise<SyncEquityBarsDailySinglePageResult> {
  if (!params.code && !params.date) {
    throw new Error('syncEquityBarsDailySinglePage requires either code or date parameter');
  }

  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });
  const logger = createLogger({ dataset: 'equity_bar_daily', ...options?.logContext });
  const supabase = getSupabaseAdmin();
  const expandSessions = options?.expandSessions ?? false;

  const timer = logger.startTimer('Sync equity bars daily (single page)');

  try {
    const { data: pageItems, paginationKey } = await client.getEquityBarsDailySinglePage(
      params,
      options?.paginationKey
    );

    const fetched = pageItems.length;

    // DBレコード形式に変換
    const records: EquityBarDailyRecord[] = expandSessions
      ? pageItems.flatMap(toEquityBarDailyRecords)
      : pageItems.map((item) => toEquityBarDailyRecord(item, 'DAY'));

    // DBに保存
    const result = await batchUpsert(supabase, TABLE_NAME, records, ON_CONFLICT);

    timer.end({ fetched, inserted: result.inserted, hasNextPage: !!paginationKey });

    if (result.errors.length > 0) {
      throw new Error(
        `batchUpsert failed with ${result.errors.length} error(s): ${result.errors[0].message}`
      );
    }

    return {
      fetched,
      inserted: result.inserted,
      paginationKey,
      errors: result.errors,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/**
 * 指定日の全銘柄株価を同期
 *
 * @param date 日付 (YYYY-MM-DD)
 * @param options オプション
 */
export async function syncEquityBarsDailyForDate(
  date: string,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
    expandSessions?: boolean;
  }
): Promise<SyncEquityBarsDailyResult> {
  return syncEquityBarsDaily({ date }, options);
}

/**
 * 指定銘柄の株価を同期（期間指定）
 *
 * @param code 銘柄コード (5桁)
 * @param from 取得開始日 (YYYY-MM-DD)
 * @param to 取得終了日 (YYYY-MM-DD)
 * @param options オプション
 */
export async function syncEquityBarsDailyForCode(
  code: string,
  from: string,
  to: string,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
    expandSessions?: boolean;
  }
): Promise<SyncEquityBarsDailyResult> {
  return syncEquityBarsDaily({ code, from, to }, options);
}

/** 株価取得時の基本カラム（raw_json除外） */
export type EquityBarBasicRecord = Pick<
  EquityBarDailyRecord,
  | 'trade_date'
  | 'local_code'
  | 'session'
  | 'open'
  | 'high'
  | 'low'
  | 'close'
  | 'volume'
  | 'turnover_value'
  | 'adjustment_factor'
  | 'adj_open'
  | 'adj_high'
  | 'adj_low'
  | 'adj_close'
  | 'adj_volume'
  | 'ingested_at'
>;

/** 基本カラムのSELECT文字列 */
const BASIC_COLUMNS =
  'trade_date,local_code,session,open,high,low,close,volume,turnover_value,adjustment_factor,adj_open,adj_high,adj_low,adj_close,adj_volume,ingested_at';

/**
 * DBから株価を取得（単一銘柄・単一日付）
 *
 * @param localCode 銘柄コード (5桁)
 * @param tradeDate 日付 (YYYY-MM-DD)
 * @param session セッション（デフォルト: DAY）
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getEquityBarFromDB(
  localCode: string,
  tradeDate: string,
  session: SessionType = 'DAY',
  options?: { includeRawJson?: boolean }
): Promise<EquityBarDailyRecord | EquityBarBasicRecord | null> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('local_code', localCode)
      .eq('trade_date', tradeDate)
      .eq('session', session)
      .single();

    if (error) {
      if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
        return null;
      }
      throw error;
    }
    return data as EquityBarDailyRecord;
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('local_code', localCode)
    .eq('trade_date', tradeDate)
    .eq('session', session)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data as EquityBarBasicRecord;
}

/**
 * DBから株価を取得（単一銘柄・期間指定）
 *
 * @param localCode 銘柄コード (5桁)
 * @param from 開始日 (YYYY-MM-DD)
 * @param to 終了日 (YYYY-MM-DD)
 * @param session セッション（デフォルト: DAY）
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getEquityBarsFromDB(
  localCode: string,
  from: string,
  to: string,
  session: SessionType = 'DAY',
  options?: { includeRawJson?: boolean }
): Promise<EquityBarDailyRecord[] | EquityBarBasicRecord[]> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('local_code', localCode)
      .gte('trade_date', from)
      .lte('trade_date', to)
      .eq('session', session)
      .order('trade_date', { ascending: true });

    if (error) {
      throw error;
    }
    return (data ?? []) as EquityBarDailyRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('local_code', localCode)
    .gte('trade_date', from)
    .lte('trade_date', to)
    .eq('session', session)
    .order('trade_date', { ascending: true });

  if (error) {
    throw error;
  }

  return (data ?? []) as EquityBarBasicRecord[];
}

/**
 * DBから指定日の全銘柄株価を取得
 *
 * @param tradeDate 日付 (YYYY-MM-DD)
 * @param session セッション（デフォルト: DAY）
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getAllEquityBarsByDateFromDB(
  tradeDate: string,
  session: SessionType = 'DAY',
  options?: { includeRawJson?: boolean }
): Promise<EquityBarDailyRecord[] | EquityBarBasicRecord[]> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('trade_date', tradeDate)
      .eq('session', session)
      .order('local_code', { ascending: true });

    if (error) {
      throw error;
    }
    return (data ?? []) as EquityBarDailyRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('trade_date', tradeDate)
    .eq('session', session)
    .order('local_code', { ascending: true });

  if (error) {
    throw error;
  }

  return (data ?? []) as EquityBarBasicRecord[];
}

/**
 * DBから最新の株価日付を取得
 */
export async function getLatestEquityBarDateFromDB(): Promise<string | null> {
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
