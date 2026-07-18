/**
 * 業種別空売り比率 エンドポイント
 *
 * @description J-Quants API V2 /v2/markets/short-ratio のデータ取得・保存
 * @see https://jpx-jquants.com/en/spec
 *
 * NOTE: 業種別（33業種）の売り注文代金内訳を analytics.short_selling_sector に保存する。
 * 市場全体の空売り比率2成分（規制あり/なし）は indicators-sync.ts の fillShortSellingOfficial
 * が全業種を合算して analytics.market_indicators へ書き込む（本モジュールは業種別の生値のみ）。
 */

import { JQuantsClient, createJQuantsClient } from '../client';
import type { ShortRatioItem, ShortSellingSectorRecord } from '../types';
import { createAdminClient } from '../../supabase/admin';
import { batchUpsert } from '../../utils/batch';
import { createLogger, type LogContext } from '../../utils/logger';
import { NonRetryableError } from '../../utils/retry';
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AdminClient = any;

const TABLE_NAME = 'short_selling_sector';
const ON_CONFLICT = 'as_of_date,sector33_code';

/** windowDays の最大値 */
const MAX_WINDOW_DAYS = 3650;
/** デフォルトの取得ウィンドウ（暦日）。訂正・祝日ずれを吸収する日次再取得幅 */
const DEFAULT_WINDOW_DAYS = 14;

export interface SyncShortRatioOptions {
  client?: JQuantsClient;
  logContext?: LogContext;
  /** 取得開始日 (YYYY-MM-DD)。未指定なら baseDate から windowDays 遡る */
  from?: string;
  /** 取得終了日 (YYYY-MM-DD)。未指定なら baseDate */
  to?: string;
  /** from 未指定時の遡り日数（デフォルト14日） */
  windowDays?: number;
  /** ウィンドウ計算の基準日（テスト用。デフォルト: 現在時刻） */
  baseDate?: Date;
  /** 保存先クライアント（テスト・seed 用の差し込み。デフォルト: analytics スキーマの admin） */
  supabase?: AdminClient;
}

export interface SyncShortRatioResult {
  /** 取得件数（APIレスポンス行数） */
  fetched: number;
  /** 保存件数 */
  inserted: number;
  /** 取得ウィンドウ */
  from: string;
  to: string;
  /** エラー一覧 */
  errors: Error[];
}

/** Asia/Tokyo で YYYY-MM-DD に整形 */
function formatJstDate(d: Date): string {
  const formatter = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Tokyo' });
  return formatter.format(d);
}

/** 取得ウィンドウを解決（from/to 明示 > windowDays 遡り） */
export function resolveShortRatioWindow(options?: SyncShortRatioOptions): {
  from: string;
  to: string;
} {
  const baseDate = options?.baseDate ?? new Date();
  const to = options?.to ?? formatJstDate(baseDate);
  if (options?.from) return { from: options.from, to };

  const requested = options?.windowDays ?? DEFAULT_WINDOW_DAYS;
  const validWindowDays = Number.isFinite(requested)
    ? Math.min(Math.max(1, Math.floor(requested)), MAX_WINDOW_DAYS)
    : DEFAULT_WINDOW_DAYS;
  const fromDate = new Date(baseDate);
  fromDate.setDate(fromDate.getDate() - validWindowDays);
  return { from: formatJstDate(fromDate), to };
}

/**
 * APIレスポンスを short_selling_sector レコードへ変換
 */
export function toShortSellingSectorRecord(item: ShortRatioItem): ShortSellingSectorRecord {
  return {
    as_of_date: item.Date,
    sector33_code: item.S33,
    selling_ex_short_value: item.SellExShortVa ?? null,
    short_with_restrictions_value: item.ShrtWithResVa ?? null,
    short_without_restrictions_value: item.ShrtNoResVa ?? null,
  };
}

/**
 * 契約前ガード: プラン未加入時の 401/403 を分かりやすいメッセージで再throw
 */
function rethrowWithPlanHint(error: unknown): never {
  if (
    error instanceof NonRetryableError &&
    (error.statusCode === 401 || error.statusCode === 403)
  ) {
    throw new Error(
      `J-Quants short-ratio の取得に失敗しました (HTTP ${error.statusCode})。` +
        `J-Quants Standard 未契約の可能性があります: ${error.message}`
    );
  }
  throw error;
}

/**
 * 業種別空売り比率を取得
 */
export async function fetchShortRatio(
  client: JQuantsClient,
  params: { s33?: string; date?: string; from?: string; to?: string }
): Promise<ShortRatioItem[]> {
  try {
    return await client.getShortRatio(params);
  } catch (error) {
    rethrowWithPlanHint(error);
  }
}

/**
 * 業種別空売り比率を取得して analytics.short_selling_sector に保存
 *
 * @param options 取得・保存オプション
 */
export async function syncShortRatio(
  options?: SyncShortRatioOptions
): Promise<SyncShortRatioResult> {
  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });
  const logger = createLogger({ dataset: TABLE_NAME, ...options?.logContext });
  const supabase = options?.supabase ?? createAdminClient('analytics');
  const { from, to } = resolveShortRatioWindow(options);

  const timer = logger.startTimer('Sync short ratio');

  try {
    logger.info('Fetching short ratio', { from, to });
    const items = await fetchShortRatio(client, { from, to });

    if (items.length === 0) {
      logger.info('No short ratio data found', { from, to });
      timer.end({ fetched: 0, inserted: 0 });
      return { fetched: 0, inserted: 0, from, to, errors: [] };
    }

    const records = items.map(toShortSellingSectorRecord);
    const result = await batchUpsert(supabase, TABLE_NAME, records, ON_CONFLICT);

    timer.end({ fetched: items.length, inserted: result.inserted, batchCount: result.batchCount });
    return {
      fetched: items.length,
      inserted: result.inserted,
      from,
      to,
      errors: result.errors,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}
