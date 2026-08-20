/**
 * 週足バックフィル（API 取得 → 週足集計 → analytics.equity_bar_weekly へ upsert）
 *
 * @description
 * 設計正本: ../../../docs/PLANS-longterm-log-chart-2026-08.md §4.2
 * seed:weekly-bars（初回10年バックフィル）と Cron A の新規追跡銘柄バックフィルで共有する。
 *
 * **equity_bar_daily には書かない**（アーカイブ対象を膨らませないため）。
 * `syncEquityBarsDailyForCode` はページごとに jquants_core.equity_bar_daily へ upsert するので
 * この経路では使わず、取得専用の `fetchEquityBarsDailyPaginated` を使う。
 */

import { createJQuantsClient, type JQuantsClient } from '../jquants/client';
import {
  fetchEquityBarsDailyPaginated,
  toEquityBarDailyRecord,
} from '../jquants/endpoints/equity-bars-daily';
import { createAdminClient } from '../supabase/admin';
import { batchUpsert } from '../utils/batch';
import { createLogger, type LogContext } from '../utils/logger';
import { aggregateWeeklyBars, type WeeklyBarRecord, type WeeklyBarSourceRow } from './weekly-bars';

/** analytics スキーマの週足テーブル */
export const WEEKLY_BARS_TABLE = 'equity_bar_weekly';
/** 00124 の PK（週で安定させるため week_end はキーにしない） */
export const WEEKLY_BARS_ON_CONFLICT = 'local_code,week_start';

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

/** analytics スキーマ束縛の supabase-js クライアント（スキーマ動的型のため any で受ける・repo 慣習） */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnalyticsClient = any;

export interface BackfillWeeklyBarsOptions {
  /** J-Quants クライアント（省略時は既定クライアント。60req/min のレート制御はクライアント側） */
  client?: JQuantsClient;
  /** analytics クライアント（省略時は service_role の analytics クライアント） */
  analytics?: AnalyticsClient;
  logContext?: LogContext;
}

export interface BackfillWeeklyBarsResult {
  local_code: string;
  /** API から取得した日足件数 */
  fetched: number;
  /** 集計した週足行数 */
  weeks: number;
  /** upsert 行数 */
  upserted: number;
  /** API のページ数 */
  pageCount: number;
}

/**
 * 1銘柄の日足を期間指定で取得し、週足へ集計して upsert する
 *
 * @param code 銘柄コード（5桁。4桁目は英字もあり得る）
 * @param from 取得開始日（YYYY-MM-DD）
 * @param to 取得終了日（YYYY-MM-DD）
 */
export async function backfillWeeklyBarsForCode(
  code: string,
  from: string,
  to: string,
  options?: BackfillWeeklyBarsOptions
): Promise<BackfillWeeklyBarsResult> {
  if (!code) {
    throw new Error('backfillWeeklyBarsForCode: code は必須です');
  }
  if (!DATE_RE.test(from) || !DATE_RE.test(to)) {
    throw new Error(`backfillWeeklyBarsForCode: from/to は YYYY-MM-DD 形式で指定してください: ${from}..${to}`);
  }
  if (from > to) {
    throw new Error(`backfillWeeklyBarsForCode: from は to 以前にしてください: ${from}..${to}`);
  }

  const logger = createLogger({ dataset: WEEKLY_BARS_TABLE, ...options?.logContext });
  const client = options?.client ?? createJQuantsClient({ logContext: options?.logContext });

  const timer = logger.startTimer('Backfill weekly bars');

  try {
    // 取得専用ページネーション（equity_bar_daily への書き込みは行わない）
    const dailyBars: WeeklyBarSourceRow[] = [];
    let pageCount = 0;

    for await (const pageItems of fetchEquityBarsDailyPaginated(client, { code, from, to })) {
      pageCount++;
      for (const item of pageItems) {
        dailyBars.push(toEquityBarDailyRecord(item, 'DAY'));
      }
    }

    const weeklyBars: WeeklyBarRecord[] = aggregateWeeklyBars(dailyBars);

    if (weeklyBars.length === 0) {
      logger.warn('No weekly bars aggregated', { code, from, to, fetched: dailyBars.length });
      timer.end({ code, fetched: dailyBars.length, weeks: 0, upserted: 0, pageCount });
      return { local_code: code, fetched: dailyBars.length, weeks: 0, upserted: 0, pageCount };
    }

    const analytics = options?.analytics ?? createAdminClient('analytics');
    const result = await batchUpsert(
      analytics,
      WEEKLY_BARS_TABLE,
      weeklyBars,
      WEEKLY_BARS_ON_CONFLICT
    );

    if (result.errors.length > 0) {
      throw new Error(
        `equity_bar_weekly upsert failed for ${code}: ${result.errors[0].message}`
      );
    }

    timer.end({
      code,
      fetched: dailyBars.length,
      weeks: weeklyBars.length,
      upserted: result.inserted,
      pageCount,
    });

    return {
      local_code: code,
      fetched: dailyBars.length,
      weeks: weeklyBars.length,
      upserted: result.inserted,
      pageCount,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}
