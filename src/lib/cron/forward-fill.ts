/**
 * 前方フィル: 取り込み対象営業日の決定と実行
 *
 * @description 実データテーブルの最新日付を起点に、抜けている営業日を
 * 「最新日（overlap by 1）〜 今日」の範囲で検出して順に取り込む。
 * GH Actions のスケジュール遅延で実行日が JST 日付をまたいでも前営業日を
 * 取りこぼさず、数日落ちても自己修復的にバックフィルする（cron-a-direct から利用）。
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import { getJSTDate, addDays } from '../utils/date';
import { createLogger, type LogContext } from '../utils/logger';
import { getBusinessDaysOrThrow, getCalendarMaxDate } from './business-day';

export interface ForwardFillOptions {
  /** 1回の実行で遡る最大日数（カレンダー日）。下限ガード。 */
  maxBackfillDays?: number;
  /** 1回の実行で処理する最大営業日数（タイムアウト防止。古い日から順に処理）。 */
  maxDaysPerRun?: number;
  /** 基準となる今日（JST, YYYY-MM-DD）。省略時は実時刻から算出（テスト用に注入可能）。 */
  today?: string;
}

/** 取引日単位データセットの設定 */
export interface ForwardFillDatasetConfig {
  /** ログ・通知用のデータセット名 */
  dataset: string;
  /** 実データテーブル（jquants_core） */
  table: string;
  /** 日付カラム */
  dateColumn: string;
  /** 指定日を取り込む sync 関数 */
  sync: (date: string, options: { logContext: LogContext }) => Promise<{ fetched: number; inserted: number; pageCount?: number }>;
  /**
   * 営業日に 0 件を許容するか。
   * - false（equity_bars / topix）: 営業日は必ずデータがある前提。末尾（当日未配信）
   *   以外で 0 件が出たら異常とみなして throw（ステップを失敗させ、stale な派生メトリクス
   *   再計算と無通知を防ぐ）。max(date) を先送りして内部欠損を作らない。
   * - true（financial）: 開示が無い営業日もあり得るため 0 件でも前進する。
   */
  allowEmpty: boolean;
}

export interface RunForwardFillResult {
  /** 実際に処理した営業日 */
  targetDates: string[];
  fetched: number;
  inserted: number;
}

const DEFAULT_MAX_BACKFILL_DAYS = 60;
const DEFAULT_MAX_DAYS_PER_RUN = 20;

/**
 * 実データテーブルの最新日付を取得（YYYY-MM-DD）。データが無ければ null。
 *
 * @description nullable な日付カラム（例: financial_disclosure.disclosed_date）でも
 * watermark が null に化けないよう、`.not(... is null)` で null 行を除外したうえで降順1件を取る。
 *
 * NOTE(perf): 並び替えは `DESC` のみで `NULLS LAST` は付けない。デフォルトの btree
 * インデックス (col ASC NULLS LAST) は後方スキャンで `DESC NULLS FIRST` を満たせるため、
 * `ORDER BY col DESC LIMIT 1` はインデックスのみで即時に解決できる。一方
 * `DESC NULLS LAST` を要求するとインデックス順序と一致せずフルスキャン+ソートに退化し、
 * 大きいテーブル（equity_bar_daily は ~134万行）では statement timeout で失敗する。
 * null は上の `.not(... is null)` で既に除外済みなので `NULLS LAST` は冗長。
 */
export async function getMaxDataDate(
  supabaseCore: SupabaseClient,
  table: string,
  dateColumn: string
): Promise<string | null> {
  const { data, error } = await supabaseCore
    .from(table)
    .select(dateColumn)
    .not(dateColumn, 'is', null)
    .order(dateColumn, { ascending: false })
    .limit(1);

  if (error) {
    throw new Error(`Failed to read max ${dateColumn} from ${table}: ${error.message}`);
  }

  const row = data?.[0] as unknown as Record<string, string> | undefined;
  return row?.[dateColumn] ?? null;
}

/**
 * 前方フィル対象の営業日リストを決定する。
 *
 * - カレンダー鮮度を確認し、DB読取失敗 / カレンダー未整備を「対象日なし」と
 *   取り違えて静かにスキップしないよう、問題があれば例外を投げる。
 * - 既存の最新日「そのもの」から再取得する（+1 ではなく overlap by 1）。
 *   ページング / バッチ upsert 途中の中断で最新日に一部行だけ残っても、
 *   upsert 冪等性により毎回入れ直して自己修復するため。
 * - 制約: 最新日より前の「内部欠損」は本方式では検出しない（末尾欠損の補填が目的。
 *   深い欠損は cron-troubleshoot の整合性チェックで別途検出する）。
 *
 * @returns 取り込み対象の営業日（古い順、最大 maxDaysPerRun 件）
 */
export async function resolveForwardFillDates(
  supabaseCore: SupabaseClient,
  table: string,
  dateColumn: string,
  options?: ForwardFillOptions
): Promise<string[]> {
  const today = options?.today ?? getJSTDate();
  const maxBackfillDays = options?.maxBackfillDays ?? DEFAULT_MAX_BACKFILL_DAYS;
  const maxDaysPerRun = options?.maxDaysPerRun ?? DEFAULT_MAX_DAYS_PER_RUN;
  const floor = addDays(today, -maxBackfillDays);

  // カレンダー鮮度ガード（DB読取失敗 / 未整備を明示的に失敗させる）
  const calendarMax = await getCalendarMaxDate(supabaseCore);
  if (calendarMax === null) {
    throw new Error('Failed to read trading_calendar max date (DB error or empty calendar)');
  }
  if (calendarMax < today) {
    throw new Error(
      `trading_calendar is stale (max=${calendarMax} < today=${today}); calendar sync must run first`
    );
  }

  const maxDate = await getMaxDataDate(supabaseCore, table, dateColumn);
  let start = maxDate ?? floor; // overlap by 1（最新日そのものから再取得）
  if (start < floor) {
    start = floor;
  }
  if (start > today) {
    return []; // 既に最新まで取り込み済み
  }

  const businessDays = await getBusinessDaysOrThrow(supabaseCore, start, today);
  return businessDays.slice(0, maxDaysPerRun);
}

/**
 * 取引日単位データセットの前方フィルを実行する。
 *
 * cap 選択:
 *   allowEmpty=true は 0件日で watermark(max) が前進しないため、処理上限が小さいと
 *   「先頭が全て0件の期間」で today に到達できず停滞し得る。これを防ぐため上限を
 *   backfill 窓いっぱい（= maxBackfillDays）に広げ、毎回必ず today まで到達させる
 *   （allowEmpty=true は 1日が安価なデータセットのみに付与する前提）。
 *   allowEmpty=false は重い・必ずデータがある前提なので maxDaysPerRun を維持する。
 */
export async function runForwardFill(
  supabaseCore: SupabaseClient,
  cfg: ForwardFillDatasetConfig,
  options?: ForwardFillOptions
): Promise<RunForwardFillResult> {
  const logger = createLogger({ module: 'forward-fill', dataset: cfg.dataset });

  const maxBackfillDays = options?.maxBackfillDays ?? DEFAULT_MAX_BACKFILL_DAYS;
  const baseMaxDaysPerRun = options?.maxDaysPerRun ?? DEFAULT_MAX_DAYS_PER_RUN;
  const maxDaysPerRun = cfg.allowEmpty ? maxBackfillDays : baseMaxDaysPerRun;

  const targetDates = await resolveForwardFillDates(supabaseCore, cfg.table, cfg.dateColumn, {
    ...options,
    maxBackfillDays,
    maxDaysPerRun,
  });

  if (targetDates.length === 0) {
    logger.info('No target dates to process (already up to date)', { dataset: cfg.dataset });
    return { targetDates: [], fetched: 0, inserted: 0 };
  }

  logger.info('Forward-fill', { dataset: cfg.dataset, count: targetDates.length, dates: targetDates });

  const today = options?.today ?? getJSTDate();
  const logContext: LogContext = { jobName: 'cron_a', dataset: cfg.dataset };
  const processedDates: string[] = [];
  let fetched = 0;
  let inserted = 0;

  for (const date of targetDates) {
    const result = await cfg.sync(date, { logContext });
    fetched += result.fetched;
    inserted += result.inserted;
    processedDates.push(date);
    logger.info('day completed', { dataset: cfg.dataset, date, fetched: result.fetched, inserted: result.inserted });

    // 0件を許容しないデータセット（equity_bars / topix）で「当日（today）以外」の営業日が
    // 0件だった場合は throw する。
    // - 当日（today）の0件は「まだ未配信」の正常ケースなので許容（次回 overlap で取得）。
    // - それ以外の営業日（過去日 / cap で切られた最終日 / today休場時の前営業日）の0件は
    //   異常。後続を処理して max(date) を進めると 0件日が「最新日より前の内部欠損」になり
    //   前方フィルでは自動復旧できない。さらに成功扱い（exit 0）のままだと派生メトリクスが
    //   stale な max で再計算され失敗通知も出ない。throw でステップを失敗させ、次回
    //   overlap by 1 で再取得させる。
    // NOTE: 配列末尾ではなく「date === today」で判定する（cap や休場で末尾が当日とは限らない）。
    if (!cfg.allowEmpty && result.fetched === 0 && date !== today) {
      throw new Error(
        `${cfg.dataset}: business day ${date} returned 0 rows (data expected; not today=${today}). ` +
          `Failing to avoid a silent internal gap.`
      );
    }
  }

  return { targetDates: processedDates, fetched, inserted };
}
