/**
 * 財務サマリー エンドポイント
 *
 * @description J-Quants API V2 /v2/fins/summary のデータ取得・保存
 * @see https://jpx-jquants.com/en/spec
 *
 * NOTE: code または date パラメータが必須。ページネーション対応。
 * V2ではDiscNoフィールドが開示の一意識別子として使用可能。
 */

import { JQuantsClient, createJQuantsClient } from '../client';
import type { FinancialSummaryItem, FinancialDisclosureRecord } from '../types';
import { getSupabaseAdmin } from '../../supabase/admin';
import { POSTGREST_ERROR_CODES } from '../../supabase/errors';
import { batchUpsert } from '../../utils/batch';
import { createLogger, type LogContext } from '../../utils/logger';

const TABLE_NAME = 'financial_disclosure';
const ON_CONFLICT = 'disclosure_id';

export interface FetchFinancialSummaryParams {
  /** 銘柄コード (5桁) */
  code?: string;
  /** 日付 (YYYY-MM-DD) - 開示日指定 */
  date?: string;
}

export interface SyncFinancialSummaryResult {
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
 * APIレスポンスから一意識別子を生成
 *
 * DiscNo（開示番号）が存在すればそれを使用
 * なければ Code + DiscDate + DiscTime + DocType + FiscalPeriod の組み合わせで生成
 * （同一分の複数開示に対応するためFiscalPeriodを追加）
 *
 * @throws Code と DiscDate が両方ともない場合はエラー
 */
export function generateDisclosureId(item: FinancialSummaryItem): string {
  if (item.DiscNo) {
    return item.DiscNo;
  }

  // 最低限必要なフィールドのバリデーション
  if (!item.Code || !item.DiscDate) {
    throw new Error(
      `generateDisclosureId: Code and DiscDate are required when DiscNo is missing. ` +
        `Got Code=${item.Code}, DiscDate=${item.DiscDate}`
    );
  }

  // フォールバック: 複合キーから生成（FiscalPeriodを追加して衝突を回避）
  // DiscTimeやDocTypeがない場合は決定論的なデフォルト値を使用
  const parts = [
    item.Code,
    item.DiscDate,
    item.DiscTime?.replace(/:/g, '') ?? '000000',
    item.DocType ?? 'unknown',
    item.FiscalPeriod ?? 'unknown',
  ];

  return parts.join('_');
}

/**
 * APIレスポンスをDBレコード形式に変換
 */
export function toFinancialDisclosureRecord(item: FinancialSummaryItem): FinancialDisclosureRecord {
  return {
    disclosure_id: generateDisclosureId(item),
    disclosed_date: item.DiscDate,
    disclosed_time: item.DiscTime,
    local_code: item.Code,
    sales: item.Sales,
    operating_profit: item.OP,
    ordinary_profit: item.OdP,
    net_income: item.NP,
    eps: item.EPS,
    bps: item.BPS,
    roe: item.ROE,
    fiscal_year_start: item.CurFYSt,
    fiscal_year_end: item.CurFYEn,
    period_type: item.CurPerType,
    doc_type: item.DocType,
    company_name: item.CoName as string | undefined,
    // 会計期間
    cur_per_start: item.CurPerSt,
    cur_per_end: item.CurPerEn,
    // 希薄化EPS
    diluted_eps: item.DEPS,
    // BS
    total_assets: item.TA,
    equity: item.Eq,
    equity_to_asset_ratio: item.EqAR,
    // CF
    cf_operating: item.CFO,
    cf_investing: item.CFI,
    cf_financing: item.CFF,
    cash_equivalents: item.CashEq,
    // ROA
    roa: item.ROA,
    // 配当
    dividend_1q: item.Div1Q,
    dividend_2q: item.Div2Q,
    dividend_3q: item.Div3Q,
    dividend_fy: item.DivFY,
    dividend_annual: item.DivAnn,
    dividend_unit: item.DivUnit,
    // 今期予想
    forecast_sales: item.FSales,
    forecast_op: item.FOP,
    forecast_odp: item.FOdP,
    forecast_np: item.FNP,
    forecast_eps: item.FEPS,
    forecast_dividend_ann: item.FDivAnn,
    // 来期予想
    next_forecast_sales: item.NxFSales,
    next_forecast_op: item.NxFOP,
    next_forecast_odp: item.NxFOdP,
    next_forecast_np: item.NxFNP,
    next_forecast_eps: item.NxFEPS,
    next_forecast_dividend_ann: item.NxFDivAnn,
    // 変更・修正フラグ
    material_change_subsidiary: item.MatChgSub,
    significant_change_content: item.SigChgInC,
    change_by_as_revision: item.ChgByASRev,
    change_no_as_revision: item.ChgNoASRev,
    change_accounting_estimate: item.ChgAcEst,
    retroactive_restatement: item.RetroRst,
    // 株式数
    shares_outstanding_fy: item.ShOutFY,
    treasury_shares_fy: item.TrShFY,
    avg_shares: item.AvgSh,
    // 非連結
    nc_sales: item.NCSales,
    nc_op: item.NCOP,
    nc_odp: item.NCOdP,
    nc_np: item.NCNP,
    nc_eps: item.NCEPS,
    nc_total_assets: item.NCTA,
    nc_equity: item.NCEq,
    nc_equity_to_asset_ratio: item.NCEqAR,
    nc_bps: item.NCBPS,
  };
}

/**
 * 財務サマリーを取得（ページネーション対応・ジェネレータ）
 *
 * @param client J-Quantsクライアント
 * @param params 取得パラメータ
 */
export async function* fetchFinancialSummaryPaginated(
  client: JQuantsClient,
  params: FetchFinancialSummaryParams
): AsyncGenerator<FinancialSummaryItem[], void, unknown> {
  yield* client.getFinancialSummaryPaginated(params);
}

/**
 * 財務サマリーを取得（全件取得）
 *
 * @param client J-Quantsクライアント
 * @param params 取得パラメータ
 */
export async function fetchFinancialSummary(
  client: JQuantsClient,
  params: FetchFinancialSummaryParams
): Promise<FinancialSummaryItem[]> {
  return client.getFinancialSummary(params);
}

/**
 * 財務サマリーを取得してDBに保存
 *
 * @param params 取得パラメータ
 * @param options オプション
 */
export async function syncFinancialSummary(
  params: FetchFinancialSummaryParams,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncFinancialSummaryResult> {
  // API仕様: code または date が必須
  if (!params.code && !params.date) {
    throw new Error('syncFinancialSummary requires either code or date parameter');
  }

  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });
  const logger = createLogger({ dataset: 'financial_disclosure', ...options?.logContext });
  const supabase = getSupabaseAdmin();

  const timer = logger.startTimer('Sync financial summary');

  try {
    logger.info('Fetching financial summary', { code: params.code, date: params.date });

    let totalFetched = 0;
    let totalInserted = 0;
    let pageCount = 0;
    const allErrors: Error[] = [];

    // ページネーションで順次処理
    for await (const pageItems of client.getFinancialSummaryPaginated(params)) {
      pageCount++;
      totalFetched += pageItems.length;

      logger.debug('Processing page', { pageCount, pageItems: pageItems.length });

      // DBレコード形式に変換
      const records = pageItems.map(toFinancialDisclosureRecord);

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

/**
 * 指定日の財務サマリーを同期
 *
 * @param date 開示日 (YYYY-MM-DD)
 * @param options オプション
 */
export async function syncFinancialSummaryForDate(
  date: string,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncFinancialSummaryResult> {
  return syncFinancialSummary({ date }, options);
}

/**
 * 指定銘柄の財務サマリーを同期
 *
 * @param code 銘柄コード (5桁)
 * @param options オプション
 */
export async function syncFinancialSummaryForCode(
  code: string,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
  }
): Promise<SyncFinancialSummaryResult> {
  return syncFinancialSummary({ code }, options);
}

/** 財務開示取得時の基本カラム（raw_json除外） */
export type FinancialDisclosureBasicRecord = Pick<
  FinancialDisclosureRecord,
  | 'disclosure_id'
  | 'disclosed_date'
  | 'disclosed_time'
  | 'local_code'
  | 'sales'
  | 'operating_profit'
  | 'ordinary_profit'
  | 'net_income'
  | 'eps'
  | 'bps'
  | 'roe'
  | 'fiscal_year_start'
  | 'fiscal_year_end'
  | 'period_type'
  | 'doc_type'
  | 'company_name'
  | 'ingested_at'
>;

/** 基本カラムのSELECT文字列 */
const BASIC_COLUMNS =
  'disclosure_id,disclosed_date,disclosed_time,local_code,sales,operating_profit,ordinary_profit,net_income,eps,bps,roe,fiscal_year_start,fiscal_year_end,period_type,doc_type,company_name,ingested_at';

/**
 * DBから財務開示を取得（単一開示）
 *
 * @param disclosureId 開示ID
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getFinancialDisclosureFromDB(
  disclosureId: string,
  options?: { includeRawJson?: boolean }
): Promise<FinancialDisclosureRecord | FinancialDisclosureBasicRecord | null> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('disclosure_id', disclosureId)
      .single();

    if (error) {
      if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
        return null;
      }
      throw error;
    }
    return data as FinancialDisclosureRecord;
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('disclosure_id', disclosureId)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data as FinancialDisclosureBasicRecord;
}

/** limit の最大値（銘柄別） */
const MAX_CODE_LIMIT = 1000;

/**
 * DBから財務開示を取得（銘柄指定・最新順）
 *
 * @param localCode 銘柄コード (5桁)
 * @param limit 取得件数（デフォルト: 10、最大: 1000）
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getFinancialDisclosuresByCodeFromDB(
  localCode: string,
  limit: number = 10,
  options?: { includeRawJson?: boolean }
): Promise<FinancialDisclosureRecord[] | FinancialDisclosureBasicRecord[]> {
  // limit の検証
  if (!Number.isFinite(limit)) {
    throw new Error('getFinancialDisclosuresByCodeFromDB: limit must be a finite number');
  }
  const validLimit = Math.min(Math.max(1, Math.floor(limit)), MAX_CODE_LIMIT);

  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('local_code', localCode)
      .order('disclosed_date', { ascending: false })
      .limit(validLimit);

    if (error) {
      throw error;
    }
    return (data ?? []) as FinancialDisclosureRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('local_code', localCode)
    .order('disclosed_date', { ascending: false })
    .limit(validLimit);

  if (error) {
    throw error;
  }

  return (data ?? []) as FinancialDisclosureBasicRecord[];
}

/**
 * DBから財務開示を取得（開示日指定）
 *
 * @param disclosedDate 開示日 (YYYY-MM-DD)
 * @param options オプション
 * @param options.includeRawJson raw_jsonを含めるか（デフォルト: false）
 */
export async function getFinancialDisclosuresByDateFromDB(
  disclosedDate: string,
  options?: { includeRawJson?: boolean }
): Promise<FinancialDisclosureRecord[] | FinancialDisclosureBasicRecord[]> {
  const supabase = getSupabaseAdmin();

  // raw_jsonを含める場合
  if (options?.includeRawJson) {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('disclosed_date', disclosedDate)
      .order('disclosed_time', { ascending: true });

    if (error) {
      throw error;
    }
    return (data ?? []) as FinancialDisclosureRecord[];
  }

  // 基本カラムのみ取得（raw_json除外・デフォルト）
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select(BASIC_COLUMNS)
    .eq('disclosed_date', disclosedDate)
    .order('disclosed_time', { ascending: true });

  if (error) {
    throw error;
  }

  return (data ?? []) as FinancialDisclosureBasicRecord[];
}

/**
 * DBから最新の開示日を取得
 */
export async function getLatestFinancialDisclosureDateFromDB(): Promise<string | null> {
  const supabase = getSupabaseAdmin();

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select('disclosed_date')
    .order('disclosed_date', { ascending: false })
    .limit(1)
    .single();

  if (error) {
    if (error.code === POSTGREST_ERROR_CODES.NO_ROWS_RETURNED) {
      return null;
    }
    throw error;
  }

  return data?.disclosed_date ?? null;
}

/**
 * @deprecated raw_jsonは削除されました。レコードから直接フィールドを参照してください。
 */
export function extractFinancialData(
  record: FinancialDisclosureRecord
): Partial<FinancialSummaryItem> {
  return {
    DiscDate: record.disclosed_date,
    DiscTime: record.disclosed_time,
    Code: record.local_code,
    Sales: record.sales,
    OP: record.operating_profit,
    OdP: record.ordinary_profit,
    NP: record.net_income,
    EPS: record.eps,
    BPS: record.bps,
    ROE: record.roe,
    CurFYSt: record.fiscal_year_start,
    CurFYEn: record.fiscal_year_end,
    CurPerType: record.period_type,
    DocType: record.doc_type,
    CoName: record.company_name,
  };
}
