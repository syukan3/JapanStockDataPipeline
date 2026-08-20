/**
 * 週足（追跡銘柄限定）日次メンテスクリプト（GH Actions用 / Cron A ステップ）
 *
 * @description
 * 保有 ∪ ウォッチリストの銘柄について analytics.equity_bar_weekly を最新に保つ。
 * 設計正本: ../../../docs/PLANS-longterm-log-chart-2026-08.md §4.3
 *
 *   1. 追跡銘柄リストを portfolio スキーマから service_role で導出
 *   2. 新規追跡銘柄（週足0行）は J-Quants から10年バックフィル（ウォッチ追加の翌日に長期チャートが出る）
 *   3. 分割・併合を7日窓で検知 → 台帳未記録のみを **イベント日の降順** に RPC 適用
 *   4. 直近2 ISO週を equity_bar_daily から再集計して upsert（API 呼び出し不要・冪等）
 *   5. weekly と daily の直近 adj_close を突合し、乖離を warn（処理は落とさない）
 *
 * 前提: 本スクリプトは Cron A の equity_bars 同期 **と** 日足 rebase(00093) の成功後に実行する
 * （cron-a.yml のゲート）。未調整の日足から週足を作らないため。
 *
 * - job_runs / job_locks は使わない（upsert は冪等。refresh-technical.ts と同方針）
 * - 失敗しても翌日の実行が検知7日窓と2週集計窓で追いつく（自己修復）
 *
 * 実行: npx tsx scripts/cron/refresh-weekly-bars.ts
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { batchUpsert } from '../../src/lib/utils/batch';
import {
  getLatestTradeDate,
  subtractDays,
  DETECT_LOOKBACK_DAYS,
} from '../../src/lib/analytics/rebase-adjusted-bars';
import { getTrackedLocalCodes } from '../../src/lib/analytics/tracked-codes';
import { getBackfillRange } from '../../src/lib/analytics/weekly-bars';
import {
  backfillWeeklyBarsForCode,
  WEEKLY_BARS_TABLE,
  WEEKLY_BARS_ON_CONFLICT,
} from '../../src/lib/analytics/weekly-bars-backfill';
import {
  aggregateWeeklyBarsByCode,
  applyWeeklyRebaseEvents,
  detectTrackedEventsInWindow,
  fetchAppliedRebaseEventKeys,
  fetchDailyBarsForCodes,
  fetchWeeklyClosesSince,
  findCodesWithoutWeeklyBars,
  findWeeklyDailyMismatches,
  recentWeeksStart,
  selectUnappliedEvents,
  RECENT_WEEKS,
} from '../../src/lib/analytics/refresh-weekly-bars';

function validateEnv(): void {
  const required = ['NEXT_PUBLIC_SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY', 'JQUANTS_API_KEY'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

async function main(): Promise<void> {
  validateEnv();
  const logger = createLogger({ module: 'refresh-weekly-bars' });

  const core = createAdminClient('jquants_core');
  const analytics = createAdminClient('analytics');

  // 1) 追跡銘柄（保有 ∪ ウォッチ）
  const codes = await getTrackedLocalCodes();
  logger.info('Tracked codes', { count: codes.length });
  if (codes.length === 0) {
    logger.info('No tracked codes; nothing to do');
    console.log(JSON.stringify({ success: true, trackedCodes: 0, backfilled: 0, applied: 0, upserted: 0 }));
    return;
  }

  const latestTradeDate = await getLatestTradeDate(core);
  logger.info('Latest trade date', { latestTradeDate });

  // 2) 新規追跡銘柄を10年バックフィル（直列。60req/min は J-Quants クライアントのトークンバケット）
  const newCodes = await findCodesWithoutWeeklyBars(analytics, codes);
  logger.info('New tracked codes to backfill', { count: newCodes.length, codes: newCodes });

  const { from: backfillFrom } = getBackfillRange(latestTradeDate);
  let backfilledWeeks = 0;
  // 1銘柄の失敗で既存銘柄の日次メンテ（3〜5）まで止めない。失敗は集約して最後に throw する
  // （バックフィルは単文 upsert なので部分書き込みは残らず、翌日そのまま再試行される）。
  const backfillErrors: string[] = [];
  for (const code of newCodes) {
    try {
      const result = await backfillWeeklyBarsForCode(code, backfillFrom, latestTradeDate, { analytics });
      backfilledWeeks += result.upserted;
      logger.info('Backfilled weekly bars', {
        localCode: code,
        from: backfillFrom,
        to: latestTradeDate,
        fetched: result.fetched,
        weeks: result.weeks,
        upserted: result.upserted,
        rebaseEvents: result.rebaseEvents,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      backfillErrors.push(`${code}: ${message}`);
      logger.error('Backfill failed; continuing with other codes', { localCode: code, error: message });
    }
  }

  // 3) 分割・併合の検知 → 台帳未記録のみをイベント日の降順に RPC 適用
  const windowStart = subtractDays(latestTradeDate, DETECT_LOOKBACK_DAYS);
  const detected = await detectTrackedEventsInWindow(core, codes, latestTradeDate);
  const appliedKeys = await fetchAppliedRebaseEventKeys(analytics, codes, windowStart, latestTradeDate);
  const pending = selectUnappliedEvents(detected, codes, appliedKeys);
  logger.info('Detected adjustment events', {
    windowStart,
    windowEnd: latestTradeDate,
    detected: detected.length,
    unapplied: pending.length,
  });

  const applied = await applyWeeklyRebaseEvents(analytics, pending);
  for (const r of applied) {
    logger.info(r.affected_rows < 0 ? 'Rebase event already recorded (skipped)' : 'Applied weekly rebase event', {
      localCode: r.local_code,
      eventDate: r.event_date,
      factor: r.adjustment_factor,
      affectedRows: r.affected_rows,
    });
  }

  // 4) 直近2 ISO週を日足から再集計（週の途中も同一 week_start 行を冪等更新）
  const weeksFrom = recentWeeksStart(latestTradeDate);
  const dailyBars = await fetchDailyBarsForCodes(core, codes, weeksFrom, latestTradeDate);
  const weeklyBars = aggregateWeeklyBarsByCode(dailyBars);
  logger.info('Recent weeks aggregated', {
    weeks: RECENT_WEEKS,
    from: weeksFrom,
    to: latestTradeDate,
    dailyBars: dailyBars.length,
    weeklyBars: weeklyBars.length,
  });

  let upserted = 0;
  if (weeklyBars.length > 0) {
    const result = await batchUpsert(analytics, WEEKLY_BARS_TABLE, weeklyBars, WEEKLY_BARS_ON_CONFLICT);
    if (result.errors.length > 0) {
      throw new Error(`equity_bar_weekly upsert failed: ${result.errors[0].message}`);
    }
    upserted = result.inserted;
  }

  // 5) 軽い突合検証（分割検知漏れの兆候を拾う安全網。乖離しても処理は落とさない）
  const weeklyCloses = await fetchWeeklyClosesSince(analytics, codes, weeksFrom);
  const mismatches = findWeeklyDailyMismatches(weeklyCloses, dailyBars);
  for (const m of mismatches) {
    logger.warn('Weekly/daily adj_close mismatch', {
      localCode: m.local_code,
      reason: m.reason,
      weekStart: m.week_start,
      weeklyAdjClose: m.weekly_adj_close,
      tradeDate: m.trade_date,
      dailyAdjClose: m.daily_adj_close,
      relativeDiff: m.relative_diff,
    });
  }

  console.log(
    JSON.stringify({
      success: backfillErrors.length === 0,
      latestTradeDate,
      trackedCodes: codes.length,
      backfilledCodes: newCodes.length - backfillErrors.length,
      backfilledWeeks,
      backfillErrors,
      detectedEvents: detected.length,
      appliedEvents: applied.length,
      recentWeeksFrom: weeksFrom,
      upserted,
      mismatches: mismatches.length,
    })
  );

  if (backfillErrors.length > 0) {
    throw new Error(`新規追跡銘柄のバックフィルに失敗しました: ${backfillErrors.join(' / ')}`);
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    const logger = createLogger({ module: 'refresh-weekly-bars' });
    logger.error('Script failed', { error: error instanceof Error ? error.message : String(error) });
    process.exit(1);
  });
