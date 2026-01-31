/**
 * 上場銘柄マスタ エンドポイント
 *
 * @description J-Quants API V2 /v2/equities/master のデータ取得・保存
 * @see https://jpx-jquants.com/en/spec
 *
 * NOTE: 非営業日を指定した場合、APIは次営業日の情報を返す
 *
 * SCD Type 2 実装:
 * - 変更があった場合のみ新レコードを追加
 * - is_current=true は各銘柄で1レコードのみ
 * - valid_from/valid_to で有効期間を管理
 */

import { JQuantsClient, createJQuantsClient } from '../client';
import type { EquityMasterItem, EquityMasterSnapshotRecord, EquityMasterRecord } from '../types';
import { getSupabaseAdmin } from '../../supabase/admin';
import { POSTGREST_ERROR_CODES } from '../../supabase/errors';
import { batchUpsert } from '../../utils/batch';
import { createLogger, type LogContext } from '../../utils/logger';

const TABLE_NAME = 'equity_master_snapshot';
const TABLE_NAME_SCD = 'equity_master';
const ON_CONFLICT = 'as_of_date,local_code';
const ON_CONFLICT_SCD = 'local_code,valid_from';

export interface FetchEquityMasterParams {
  /** 銘柄コード (5桁) */
  code?: string;
  /** 日付 (YYYY-MM-DD) */
  date?: string;
}

export interface SyncEquityMasterResult {
  /** 取得件数 */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** エラー一覧 */
  errors: Error[];
}

/**
 * APIレスポンスをDBレコード形式に変換（レガシー用）
 * @deprecated Phase 4以降は toEquityMasterSCDRecord を使用
 */
export function toEquityMasterRecord(item: EquityMasterItem): EquityMasterSnapshotRecord {
  return {
    as_of_date: item.Date,
    local_code: item.Code,
    company_name: item.CoName,
    company_name_en: item.CoNameEn,
    sector17_code: item.S17,
    sector17_name: item.S17Nm,
    sector33_code: item.S33,
    sector33_name: item.S33Nm,
    scale_category: item.ScaleCat,
    market_code: item.Mkt,
    market_name: item.MktNm,
    margin_code: item.MarginCode,
    margin_code_name: item.MarginCodeNm,
  };
}

/**
 * APIレスポンスをSCD Type 2レコード形式に変換
 */
export function toEquityMasterSCDRecord(item: EquityMasterItem): EquityMasterRecord {
  return {
    local_code: item.Code,
    company_name: item.CoName,
    company_name_en: item.CoNameEn,
    sector17_code: item.S17,
    sector17_name: item.S17Nm,
    sector33_code: item.S33,
    sector33_name: item.S33Nm,
    scale_category: item.ScaleCat,
    market_code: item.Mkt,
    market_name: item.MktNm,
    margin_code: item.MarginCode ?? null,
    margin_code_name: item.MarginCodeNm ?? null,
    valid_from: item.Date,
    valid_to: null,
    is_current: true,
  };
}

/** 比較対象のフィールド */
const COMPARE_FIELDS = [
  'company_name',
  'company_name_en',
  'sector17_code',
  'sector17_name',
  'sector33_code',
  'sector33_name',
  'scale_category',
  'market_code',
  'market_name',
  'margin_code',
  'margin_code_name',
] as const;

/**
 * 2つのEquityMasterレコードが同一かどうかを比較
 */
export function isSameEquityMaster(
  a: EquityMasterRecord | EquityMasterSnapshotRecord,
  b: EquityMasterRecord | EquityMasterSnapshotRecord
): boolean {
  for (const field of COMPARE_FIELDS) {
    // null と undefined を同等として扱う（DB は null、API は undefined を返す場合がある）
    const va = a[field] ?? null;
    const vb = b[field] ?? null;
    if (va !== vb) {
      return false;
    }
  }
  return true;
}

/**
 * 上場銘柄マスタを取得
 *
 * @param client J-Quantsクライアント
 * @param params 取得パラメータ
 */
export async function fetchEquityMaster(
  client: JQuantsClient,
  params?: FetchEquityMasterParams
): Promise<EquityMasterItem[]> {
  const response = await client.getEquityMaster(params);
  return response.data;
}

/**
 * 上場銘柄マスタを取得してDBに保存
 *
 * @param params 取得パラメータ
 * @param options オプション
 */
export async function syncEquityMaster(
  params?: FetchEquityMasterParams,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncEquityMasterResult> {
  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });
  const logger = createLogger({ dataset: 'equity_master', ...options?.logContext });
  const supabase = getSupabaseAdmin();

  const timer = logger.startTimer('Sync equity master');

  try {
    // 1. APIからデータ取得
    logger.info('Fetching equity master', { code: params?.code, date: params?.date });
    const items = await fetchEquityMaster(client, params);

    if (items.length === 0) {
      logger.info('No equity master data found');
      timer.end({ fetched: 0, inserted: 0 });
      return { fetched: 0, inserted: 0, errors: [] };
    }

    logger.info('Fetched equity master', { rowCount: items.length });

    // 2. DBレコード形式に変換
    const records = items.map(toEquityMasterRecord);

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
 * 指定日の上場銘柄マスタを同期
 *
 * @param date 日付 (YYYY-MM-DD)
 * @param options オプション
 */
export async function syncEquityMasterForDate(
  date: string,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncEquityMasterResult> {
  return syncEquityMaster({ date }, options);
}

/** 銘柄マスタ取得時の基本カラム（raw_json除外） */
export type EquityMasterBasicRecord = Pick<
  EquityMasterSnapshotRecord,
  | 'as_of_date'
  | 'local_code'
  | 'company_name'
  | 'company_name_en'
  | 'sector17_code'
  | 'sector17_name'
  | 'sector33_code'
  | 'sector33_name'
  | 'scale_category'
  | 'market_code'
  | 'market_name'
  | 'margin_code'
  | 'margin_code_name'
  | 'ingested_at'
>;

/** 基本カラムのSELECT文字列 */
const BASIC_COLUMNS =
  'as_of_date,local_code,company_name,company_name_en,sector17_code,sector17_name,sector33_code,sector33_name,scale_category,market_code,market_name,margin_code,margin_code_name,ingested_at';

/**
 * DBから銘柄マスタを取得（単一銘柄・最新日付）
 *
 * @param localCode 銘柄コード (5桁)
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getEquityMasterFromDB(
  localCode: string,
  options?: { includeRawJson?: boolean }
): Promise<EquityMasterSnapshotRecord | EquityMasterBasicRecord | null> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('local_code', localCode)
      .order('as_of_date', { ascending: false })
      .limit(1)
      .single();

    if (error) {
      if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
        return null;
      }
      throw error;
    }
    return data as EquityMasterSnapshotRecord;
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('local_code', localCode)
    .order('as_of_date', { ascending: false })
    .limit(1)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data as EquityMasterBasicRecord;
}

/**
 * DBから銘柄マスタを取得（指定日付）
 *
 * @param localCode 銘柄コード (5桁)
 * @param asOfDate 日付 (YYYY-MM-DD)
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getEquityMasterByDateFromDB(
  localCode: string,
  asOfDate: string,
  options?: { includeRawJson?: boolean }
): Promise<EquityMasterSnapshotRecord | EquityMasterBasicRecord | null> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('local_code', localCode)
      .eq('as_of_date', asOfDate)
      .single();

    if (error) {
      if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
        return null;
      }
      throw error;
    }
    return data as EquityMasterSnapshotRecord;
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('local_code', localCode)
    .eq('as_of_date', asOfDate)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data as EquityMasterBasicRecord;
}

/**
 * DBから全銘柄を取得（指定日付）
 *
 * @param asOfDate 日付 (YYYY-MM-DD)
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getAllEquityMasterByDateFromDB(
  asOfDate: string,
  options?: { includeRawJson?: boolean }
): Promise<EquityMasterSnapshotRecord[] | EquityMasterBasicRecord[]> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('as_of_date', asOfDate)
      .order('local_code', { ascending: true });

    if (error) {
      throw error;
    }
    return (data ?? []) as EquityMasterSnapshotRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('as_of_date', asOfDate)
    .order('local_code', { ascending: true });

  if (error) {
    throw error;
  }

  return (data ?? []) as EquityMasterBasicRecord[];
}

/**
 * DBから最新の銘柄マスタ日付を取得
 */
export async function getLatestEquityMasterDateFromDB(): Promise<string | null> {
  const supabase = getSupabaseAdmin();

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select('as_of_date')
    .order('as_of_date', { ascending: false })
    .limit(1)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data?.as_of_date ?? null;
}

// ============================================
// SCD Type 2 関連関数
// ============================================

/** SCD Type 2 同期結果 */
export interface SyncEquityMasterSCDResult {
  /** APIから取得した件数 */
  fetched: number;
  /** 新規追加した件数 */
  inserted: number;
  /** 更新（クローズ + 新規）した件数 */
  updated: number;
  /** 上場廃止処理した件数 */
  delisted: number;
  /** エラー一覧 */
  errors: Error[];
}

/** SCD基本カラム（クエリ用） */
const SCD_BASIC_COLUMNS =
  'id,local_code,company_name,company_name_en,sector17_code,sector17_name,sector33_code,sector33_name,scale_category,market_code,market_name,margin_code,margin_code_name,valid_from,valid_to,is_current,created_at';

/**
 * 銘柄マスタをSCD Type 2方式で同期
 *
 * 1. APIから最新データを取得
 * 2. 現在有効なレコードと比較
 * 3. 変更があれば旧レコードをクローズし新レコードを追加
 * 4. APIに存在しない銘柄は上場廃止としてクローズ
 *
 * @param date 同期日付 (YYYY-MM-DD)
 * @param options オプション
 */
export async function syncEquityMasterSCD(
  date: string,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncEquityMasterSCDResult> {
  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });
  const logger = createLogger({ dataset: 'equity_master', ...options?.logContext });
  const supabase = getSupabaseAdmin();

  const timer = logger.startTimer('Sync equity master SCD');

  try {
    // 1. APIから最新データを取得
    logger.info('Fetching equity master', { date });
    const items = await fetchEquityMaster(client, { date });

    if (items.length === 0) {
      logger.info('No equity master data found');
      timer.end({ fetched: 0, inserted: 0, updated: 0, delisted: 0 });
      return { fetched: 0, inserted: 0, updated: 0, delisted: 0, errors: [] };
    }

    logger.info('Fetched equity master', { rowCount: items.length });

    // APIレスポンスから実際の有効日を取得（非営業日指定時は次営業日が返る）
    // 全アイテムは同一日付を持つため、最初のアイテムから取得
    const effectiveDate = items[0].Date;
    logger.info('Effective date from API', { requestedDate: date, effectiveDate });

    // 2. 現在有効な全レコードを取得（デフォルト1000行制限を回避）
    const currentRecords: EquityMasterRecord[] = [];
    const PAGE_SIZE = 1000;
    let offset = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const { data, error: fetchError } = await supabase
        .from(TABLE_NAME_SCD)
        .select(SCD_BASIC_COLUMNS)
        .eq('is_current', true)
        .range(offset, offset + PAGE_SIZE - 1);

      if (fetchError) {
        throw fetchError;
      }

      if (!data || data.length === 0) break;
      currentRecords.push(...(data as EquityMasterRecord[]));
      if (data.length < PAGE_SIZE) break;
      offset += PAGE_SIZE;
    }
    logger.info('Loaded current records', { count: currentRecords.length });

    // Map for quick lookup
    const currentMap = new Map<string, EquityMasterRecord>();
    for (const record of (currentRecords ?? []) as EquityMasterRecord[]) {
      currentMap.set(record.local_code, record);
    }

    // 3. 差分を検出
    const toInsert: EquityMasterRecord[] = [];
    const toClose: { id: number; valid_to: string }[] = [];
    const apiCodeSet = new Set<string>();

    for (const item of items) {
      apiCodeSet.add(item.Code);
      const newRecord = toEquityMasterSCDRecord(item);
      const existing = currentMap.get(item.Code);

      if (!existing) {
        // 新規銘柄
        toInsert.push(newRecord);
      } else if (!isSameEquityMaster(existing, newRecord)) {
        // 変更あり → 旧レコードをクローズして新レコード追加
        // valid_toはexclusive（newRecord.valid_fromと同じ日付を設定）
        toClose.push({ id: existing.id!, valid_to: newRecord.valid_from });
        toInsert.push(newRecord);
      }
      // 変更なし → 何もしない
    }

    // 4. 上場廃止処理（APIに存在しない銘柄）
    const delistedRecords: { id: number; valid_to: string }[] = [];
    for (const [code, record] of currentMap) {
      if (!apiCodeSet.has(code)) {
        // 上場廃止もeffectiveDateでクローズ（APIから返された有効日）
        delistedRecords.push({ id: record.id!, valid_to: effectiveDate });
      }
    }

    // 5. DB更新を実行
    const errors: Error[] = [];

    // 5a. 旧レコードをクローズ（更新 + 上場廃止）
    const allToClose = [...toClose, ...delistedRecords];
    if (allToClose.length > 0) {
      // valid_toでグループ化してバッチUPDATE（N+1回避）
      const byValidTo = new Map<string, number[]>();
      for (const { id, valid_to } of allToClose) {
        const ids = byValidTo.get(valid_to);
        if (ids) {
          ids.push(id);
        } else {
          byValidTo.set(valid_to, [id]);
        }
      }
      for (const [validTo, ids] of byValidTo) {
        const { error } = await supabase
          .from(TABLE_NAME_SCD)
          .update({ valid_to: validTo, is_current: false })
          .in('id', ids);

        if (error) {
          errors.push(new Error(`Failed to close ${ids.length} records (valid_to=${validTo}): ${error.message}`));
        }
      }
      logger.info('Closed records', {
        updated: toClose.length,
        delisted: delistedRecords.length,
      });
    }

    // 5a でエラーがあれば、5b をスキップして即座にエラーを投げる
    // （is_current=true の重複を防ぐ）
    if (errors.length > 0) {
      // 最初の3件の個別エラーをログ出力
      for (const e of errors.slice(0, 3)) {
        logger.error('Close error detail', { message: e.message });
      }
      const msg = `Failed to close ${errors.length} record(s) — aborting insert to prevent duplicate is_current=true`;
      const err = new Error(msg);
      timer.endWithError(err);
      throw err;
    }

    // 5b. 新レコードを追加
    let inserted = 0;
    if (toInsert.length > 0) {
      const result = await batchUpsert(supabase, TABLE_NAME_SCD, toInsert, ON_CONFLICT_SCD, {
        onBatchComplete: (batchIndex, count, total) => {
          logger.debug('Batch complete', { batchIndex, inserted: count, total });
        },
      });
      inserted = result.inserted;
      errors.push(...result.errors);
    }

    timer.end({
      fetched: items.length,
      inserted,
      updated: toClose.length,
      delisted: delistedRecords.length,
    });

    return {
      fetched: items.length,
      inserted,
      updated: toClose.length,
      delisted: delistedRecords.length,
      errors,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/**
 * 指定日時点のEquityMasterを取得（SCD Type 2）
 *
 * @param localCode 銘柄コード (5桁)
 * @param asOfDate 日付 (YYYY-MM-DD)
 */
export async function getEquityMasterAsOfDate(
  localCode: string,
  asOfDate: string
): Promise<EquityMasterRecord | null> {
  const supabase = getSupabaseAdmin();

  const { data, error } = await supabase
    .from(TABLE_NAME_SCD)
    .select(SCD_BASIC_COLUMNS)
    .eq('local_code', localCode)
    .lte('valid_from', asOfDate)
    .or(`valid_to.is.null,valid_to.gt.${asOfDate}`)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data as EquityMasterRecord;
}

/**
 * 全銘柄の現在有効データを取得（SCD Type 2）
 */
export async function getAllCurrentEquityMaster(): Promise<EquityMasterRecord[]> {
  const supabase = getSupabaseAdmin();

  const { data, error } = await supabase
    .from(TABLE_NAME_SCD)
    .select(SCD_BASIC_COLUMNS)
    .eq('is_current', true)
    .order('local_code', { ascending: true });

  if (error) {
    throw error;
  }

  return (data ?? []) as EquityMasterRecord[];
}

/**
 * 指定銘柄の履歴を取得（SCD Type 2）
 *
 * @param localCode 銘柄コード (5桁)
 */
export async function getEquityMasterHistory(localCode: string): Promise<EquityMasterRecord[]> {
  const supabase = getSupabaseAdmin();

  const { data, error } = await supabase
    .from(TABLE_NAME_SCD)
    .select(SCD_BASIC_COLUMNS)
    .eq('local_code', localCode)
    .order('valid_from', { ascending: true });

  if (error) {
    throw error;
  }

  return (data ?? []) as EquityMasterRecord[];
}
