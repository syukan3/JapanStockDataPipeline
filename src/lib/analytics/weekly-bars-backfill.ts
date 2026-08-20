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
 *
 * 併せて、取得した日足の分割・併合イベントを台帳 equity_bar_weekly_rebase_events へ
 * 「適用済み」として記録する（二重調整防止・§4.2）。API の adj_* は取得時点で全イベントを
 * 織り込み済みなので、Cron A の7日検知窓が同じ日を拾っても増分係数を重ねてはならない。
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
/** 適用済み再基準化イベント台帳（00124） */
export const WEEKLY_REBASE_EVENTS_TABLE = 'equity_bar_weekly_rebase_events';
/** 台帳の PK */
export const WEEKLY_REBASE_EVENTS_ON_CONFLICT = 'local_code,event_date';

/**
 * バックフィルの upsert バッチサイズ。
 *
 * 10年 ≒ 522週なので **1リクエスト＝1 SQL文で全期間を投入する**（約130KB で 1MB 制限内）。
 * 分割すると先行バッチだけ成功して中断した銘柄が「週足あり」＝バックフィル済みと判定され、
 * 中間週が恒久的に欠損する（新規判定は行の有無しか見ない）。単文なら失敗時に全ロールバックされ、
 * 翌日の実行で同じ銘柄がもう一度バックフィル対象になる。
 */
export const WEEKLY_BACKFILL_BATCH_SIZE = 1000;

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
  /** 台帳へ「適用済み」として記録した分割・併合イベント数（既記録は含む・件数は検知ベース） */
  rebaseEvents: number;
}

/** analytics.equity_bar_weekly_rebase_events の1行（applied_at は DB default に委ねる） */
export interface WeeklyRebaseEventRow {
  local_code: string;
  event_date: string;
  adjustment_factor: number;
}

/**
 * 取得した日足から分割・併合イベント（factor が非 null かつ ≠1 の日）を抽出する
 *
 * API の adj_* は取得時点で全イベントを織り込み済みなので、バックフィルした週足は
 * 既にそのイベントを反映している。にもかかわらず Cron A の7日検知窓に同じ日が入ると
 * 「未適用」と誤判定され増分係数が二重に掛かるため、**適用済みとして台帳へ直接記録**する
 * （RPC apply_weekly_rebase_event は呼ばない）。設計正本 §4.2 の台帳シーディング。
 *
 * 集計と同じく session を持つ行は DAY のみ採用し、同一日の重複は先勝ちで1件にまとめる。
 */
export function extractRebaseEvents(bars: WeeklyBarSourceRow[]): WeeklyRebaseEventRow[] {
  const events = new Map<string, WeeklyRebaseEventRow>();
  for (const bar of bars) {
    if (bar.session != null && bar.session !== 'DAY') continue;
    if (bar.adjustment_factor == null || bar.adjustment_factor === '') continue;
    const factor = Number(bar.adjustment_factor);
    if (!Number.isFinite(factor) || factor === 1) continue;
    if (events.has(bar.trade_date)) continue;
    events.set(bar.trade_date, {
      local_code: bar.local_code,
      event_date: bar.trade_date,
      adjustment_factor: factor,
    });
  }
  return [...events.values()].sort((a, b) =>
    a.event_date < b.event_date ? -1 : a.event_date > b.event_date ? 1 : 0
  );
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
      return { local_code: code, fetched: dailyBars.length, weeks: 0, upserted: 0, pageCount, rebaseEvents: 0 };
    }

    const analytics = options?.analytics ?? createAdminClient('analytics');

    // 台帳を週足 upsert より **前** に書く。順序が逆だと「週足は入ったが台帳は落ちた」状態で
    // 中断した場合、以後この銘柄は「週足あり」判定でバックフィル対象から外れ、台帳の空白が
    // 埋まらないまま7日検知窓の再検知で増分係数が二重に掛かる。先に書けば中断しても
    // 週足が無いまま＝翌日また同じバックフィルが走り、台帳 upsert は冪等なので自己修復する。
    const rebaseEvents = extractRebaseEvents(dailyBars);
    await seedRebaseEvents(analytics, code, rebaseEvents);

    const result = await batchUpsert(
      analytics,
      WEEKLY_BARS_TABLE,
      weeklyBars,
      WEEKLY_BARS_ON_CONFLICT,
      { batchSize: WEEKLY_BACKFILL_BATCH_SIZE }
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
      rebaseEvents: rebaseEvents.length,
    });

    return {
      local_code: code,
      fetched: dailyBars.length,
      weeks: weeklyBars.length,
      upserted: result.inserted,
      pageCount,
      rebaseEvents: rebaseEvents.length,
    };
  } catch (error) {
    timer.endWithError(error as Error);
    throw error;
  }
}

/**
 * 抽出した分割・併合イベントを台帳へ「適用済み」として記録する（on conflict do nothing 相当）
 *
 * 既記録の行は更新しない（`applied_at` と当時の係数を保全する）。イベント数は10年でも数件なので
 * バッチ分割はしない。失敗は throw（週足を書かずに翌日リトライさせ、台帳の空白を作らない）。
 */
async function seedRebaseEvents(
  analytics: AnalyticsClient,
  code: string,
  events: WeeklyRebaseEventRow[]
): Promise<void> {
  if (events.length === 0) return;

  const { error } = await analytics
    .from(WEEKLY_REBASE_EVENTS_TABLE)
    .upsert(events, { onConflict: WEEKLY_REBASE_EVENTS_ON_CONFLICT, ignoreDuplicates: true });

  if (error) {
    throw new Error(
      `equity_bar_weekly_rebase_events upsert failed for ${code}: ${error.message}`
    );
  }
}
