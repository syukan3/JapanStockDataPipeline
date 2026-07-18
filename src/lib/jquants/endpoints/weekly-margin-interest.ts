/**
 * 信用取引週末残高 エンドポイント
 *
 * @description J-Quants API V2 /v2/markets/margin-interest のデータ取得・保存・保持管理
 * @see https://jpx-jquants.com/en/spec
 *
 * NOTE: Standard プラン限定エンドポイント。申込日（週末）ベースの週次データで、
 * 営業日が2日以下の週はデータ無し（欠落週が正常）。訂正・祝日ずれを吸収するため
 * 週次ウィンドウ再取得（冪等upsert）方式をとる。
 *
 * 保持ポリシー: 全銘柄は直近1年のみ保持し、保有・ウォッチ銘柄（保護リスト）は
 * 全期間保持する（pruneWeeklyMarginInterest）。
 */

import { JQuantsClient, createJQuantsClient } from '../client';
import type { WeeklyMarginInterestItem, WeeklyMarginInterestRecord } from '../types';
import { getSupabaseAdmin, createAdminClient } from '../../supabase/admin';
import { batchUpsert, batchSelect } from '../../utils/batch';
import { createLogger, type LogContext } from '../../utils/logger';
import { addDays, getJSTDate } from '../../utils/date';

const TABLE_NAME = 'weekly_margin_interest';
const ON_CONFLICT = 'local_code,application_date';

export interface FetchWeeklyMarginInterestParams {
  /** ローカルコード (5桁) */
  code?: string;
  /** 申込日指定 (YYYY-MM-DD) */
  date?: string;
  /** 取得開始日 (YYYY-MM-DD) */
  from?: string;
  /** 取得終了日 (YYYY-MM-DD) */
  to?: string;
}

export interface SyncWeeklyMarginInterestResult {
  /** 取得件数（APIレスポンス行数） */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** エラー一覧 */
  errors: Error[];
}

/**
 * APIレスポンスをDBレコードに変換
 */
export function toWeeklyMarginInterestRecord(
  item: WeeklyMarginInterestItem
): WeeklyMarginInterestRecord {
  return {
    local_code: item.Code,
    application_date: item.Date,
    short_total: item.ShrtVol ?? null,
    long_total: item.LongVol ?? null,
    short_negotiable: item.ShrtNegVol ?? null,
    long_negotiable: item.LongNegVol ?? null,
    short_standardized: item.ShrtStdVol ?? null,
    long_standardized: item.LongStdVol ?? null,
    issue_type: parseIssueType(item.IssType),
  };
}

/**
 * IssType文字列をsmallintに変換（parseInt失敗はnull）
 */
function parseIssueType(issType: string | null | undefined): number | null {
  if (issType === null || issType === undefined || issType === '') {
    return null;
  }
  const parsed = parseInt(issType, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

/**
 * Standard プラン未契約時のエラーを判別してメッセージを補強
 *
 * NOTE: margin-interest は Standard 限定のため、未契約のAPIキーでは 401/403 系が返る。
 * 呼び出し側（cron-f の job_runs failed 記録）にそのまま乗る前提でメッセージに含める。
 */
function wrapPlanError(error: unknown): unknown {
  const statusCode =
    error !== null && typeof error === 'object' && 'statusCode' in error
      ? (error as { statusCode?: number }).statusCode
      : undefined;
  if (statusCode === 401 || statusCode === 403) {
    const message = error instanceof Error ? error.message : String(error);
    return new Error(
      `Weekly margin interest fetch failed (HTTP ${statusCode}): ` +
        `J-Quants Standard未契約の可能性があります。契約状態とAPIキーを確認してください。: ${message}`
    );
  }
  return error;
}

/**
 * 信用取引週末残高を取得
 *
 * @param client J-Quantsクライアント
 * @param params 取得パラメータ（code または date/from/to 指定。バリデーションはAPI任せ）
 */
export async function fetchWeeklyMarginInterest(
  client: JQuantsClient,
  params: FetchWeeklyMarginInterestParams
): Promise<WeeklyMarginInterestItem[]> {
  try {
    return await client.getWeeklyMarginInterest(params);
  } catch (error) {
    throw wrapPlanError(error);
  }
}

/**
 * 信用取引週末残高を取得してDBに保存
 *
 * @param params 取得パラメータ
 * @param options オプション
 */
export async function syncWeeklyMarginInterest(
  params: FetchWeeklyMarginInterestParams,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
    /** trueの場合はDB保存をスキップ（取得件数の確認用） */
    dryRun?: boolean;
  }
): Promise<SyncWeeklyMarginInterestResult> {
  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });
  const logger = createLogger({ dataset: TABLE_NAME, ...options?.logContext });
  const supabase = getSupabaseAdmin();

  const timer = logger.startTimer('Sync weekly margin interest');

  try {
    // 1. APIからデータ取得
    logger.info('Fetching weekly margin interest', {
      code: params.code,
      date: params.date,
      from: params.from,
      to: params.to,
    });
    const items = await fetchWeeklyMarginInterest(client, params);

    if (items.length === 0) {
      logger.info('No weekly margin interest data found');
      timer.end({ fetched: 0, inserted: 0 });
      return { fetched: 0, inserted: 0, errors: [] };
    }

    // 2. DBレコード形式に変換
    const records = items.map(toWeeklyMarginInterestRecord);

    if (options?.dryRun) {
      logger.info('Dry run: skipping upsert', { rowCount: records.length });
      timer.end({ fetched: items.length, inserted: 0, dryRun: true });
      return { fetched: items.length, inserted: 0, errors: [] };
    }

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

/** windowDaysの最大値 */
const MAX_WINDOW_DAYS = 365;

/** デフォルトwindow（欠落週・訂正・祝日ずれを吸収する直近35日） */
const DEFAULT_WINDOW_DAYS = 35;

/**
 * スライディングウィンドウで信用取引週末残高を同期
 *
 * @param windowDays 取得日数（デフォルト: 35日、最大: 365日）
 * @param options オプション
 */
export async function syncWeeklyMarginInterestWithWindow(
  windowDays: number = DEFAULT_WINDOW_DAYS,
  options?: {
    client?: JQuantsClient;
    logContext?: LogContext;
    baseDate?: Date;
    dryRun?: boolean;
  }
): Promise<SyncWeeklyMarginInterestResult> {
  // windowDays の検証
  if (!Number.isFinite(windowDays)) {
    throw new Error('syncWeeklyMarginInterestWithWindow: windowDays must be a finite number');
  }
  const validWindowDays = Math.min(Math.max(1, Math.floor(windowDays)), MAX_WINDOW_DAYS);

  const baseDate = options?.baseDate ?? new Date();
  const to = getJSTDate(baseDate);
  const from = addDays(to, -validWindowDays);

  // NOTE: /v2/markets/margin-interest は from/to 単独指定を受け付けない（code か date が必須。
  // 2026-07-18 の初回本番実行で HTTP 400 を実地確認）。ウィンドウ再取得は暦日ループの
  // date= 指定で行う（申込日以外の日は空レスポンス。冪等upsertのため重複取得も無害）。
  const merged: SyncWeeklyMarginInterestResult = { fetched: 0, inserted: 0, errors: [] };
  let date = from;
  while (date <= to) {
    const result = await syncWeeklyMarginInterest({ date }, options);
    merged.fetched += result.fetched;
    merged.inserted += result.inserted;
    merged.errors.push(...result.errors);
    date = addDays(date, 1);
  }
  return merged;
}

/**
 * プルーニング保護対象の銘柄コード一覧を取得（保有 + ウォッチリスト）
 *
 * - 保有: `portfolio.transactions` を集計し buy - sell > 0 の銘柄
 *   （未削除ポートフォリオのみ。transactions に deleted_at は無く親でフィルタ）
 * - ウォッチ: `portfolio.watchlist_items` の全銘柄
 *
 * NOTE: Scouter src/lib/fetch-holdings.ts と同じ service_role クロススキーマ読み。
 * portfolio スキーマの列名変更時はこの関数の追随が必要。
 */
export async function fetchProtectedLocalCodes(): Promise<string[]> {
  const portfolioClient = createAdminClient('portfolio');

  // 1. 未削除ポートフォリオのIDを取得
  const portfolios = await batchSelect<{ id: string; deleted_at: string | null }>(
    portfolioClient,
    'portfolios',
    { columns: 'id, deleted_at', orderBy: { column: 'id' } }
  );
  const activeIds = new Set(
    portfolios.filter((p) => p.deleted_at === null).map((p) => p.id)
  );

  // 2. 取引を集計して純保有 > 0 の銘柄を抽出
  const transactions = await batchSelect<{
    portfolio_id: string;
    local_code: string;
    trade_type: 'buy' | 'sell';
    quantity: number;
  }>(portfolioClient, 'transactions', {
    columns: 'portfolio_id, local_code, trade_type, quantity',
    orderBy: { column: 'id' },
  });

  const net = new Map<string, number>();
  for (const t of transactions) {
    if (!activeIds.has(t.portfolio_id)) continue;
    const delta = t.trade_type === 'buy' ? t.quantity : -t.quantity;
    net.set(t.local_code, (net.get(t.local_code) ?? 0) + delta);
  }
  const heldCodes = [...net.entries()]
    .filter(([, qty]) => qty > 0)
    .map(([code]) => code);

  // 3. ウォッチリスト銘柄
  const watchlistItems = await batchSelect<{ local_code: string }>(
    portfolioClient,
    'watchlist_items',
    { columns: 'local_code', orderBy: { column: 'local_code' } }
  );

  return [...new Set([...heldCodes, ...watchlistItems.map((w) => w.local_code)])].sort();
}

export interface PruneWeeklyMarginInterestResult {
  /** 削除行数 */
  deleted: number;
  /** 保護対象銘柄数 */
  protectedCount: number;
  /** 削除対象期間の境界（cutoffより古い行が対象） */
  cutoffDate: string;
}

/** プルーニングDELETEの1回あたり日付レンジ（statement timeout対策で刻む） */
const PRUNE_WINDOW_DAYS = 90;

/**
 * 保持期間（1年）を超えた行を削除する。保護リスト銘柄（保有+ウォッチ）は全期間保持。
 *
 * DELETEは日付レンジを刻んで実行する（statement timeout対策。週次データのため
 * 通常の1回分は小さいが、初回バックフィル後の一括削除に備える）。
 */
export async function pruneWeeklyMarginInterest(
  options?: {
    logContext?: LogContext;
    baseDate?: Date;
    /** テスト用に保護リスト取得を差し替える */
    protectedCodes?: string[];
  }
): Promise<PruneWeeklyMarginInterestResult> {
  const logger = createLogger({ dataset: TABLE_NAME, ...options?.logContext });
  const supabase = getSupabaseAdmin();

  const baseDate = options?.baseDate ?? new Date();
  const cutoffDate = addDays(getJSTDate(baseDate), -365);

  const protectedCodes = options?.protectedCodes ?? (await fetchProtectedLocalCodes());

  // 削除対象の最古日を取得（無ければ何もしない）
  const { data: oldestRows, error: oldestError } = await supabase
    .from(TABLE_NAME)
    .select('application_date')
    .lt('application_date', cutoffDate)
    .order('application_date', { ascending: true })
    .limit(1);

  if (oldestError) {
    throw new Error(`Failed to find prune range: ${oldestError.message}`);
  }
  if (!oldestRows || oldestRows.length === 0) {
    logger.info('No rows to prune', { cutoffDate });
    return { deleted: 0, protectedCount: protectedCodes.length, cutoffDate };
  }

  let deleted = 0;
  let windowStart = oldestRows[0].application_date as string;

  while (windowStart < cutoffDate) {
    const windowEnd =
      addDays(windowStart, PRUNE_WINDOW_DAYS) < cutoffDate
        ? addDays(windowStart, PRUNE_WINDOW_DAYS)
        : cutoffDate;

    let query = supabase
      .from(TABLE_NAME)
      .delete({ count: 'exact' })
      .gte('application_date', windowStart)
      .lt('application_date', windowEnd);

    if (protectedCodes.length > 0) {
      query = query.not('local_code', 'in', `(${protectedCodes.join(',')})`);
    }

    const { error, count } = await query;
    if (error) {
      throw new Error(
        `Failed to prune weekly margin interest (${windowStart}..${windowEnd}): ${error.message}`
      );
    }

    deleted += count ?? 0;
    windowStart = windowEnd;
  }

  logger.info('Pruned weekly margin interest', {
    deleted,
    cutoffDate,
    protectedCount: protectedCodes.length,
  });

  return { deleted, protectedCount: protectedCodes.length, cutoffDate };
}

/**
 * DBから最新の申込日を取得
 */
export async function getLatestWeeklyMarginInterestDateFromDB(): Promise<string | null> {
  const supabase = getSupabaseAdmin();

  const { data, error } = await supabase
    .from(TABLE_NAME)
    .select('application_date')
    .order('application_date', { ascending: false })
    .limit(1);

  if (error) {
    throw error;
  }

  return data?.[0]?.application_date ?? null;
}
