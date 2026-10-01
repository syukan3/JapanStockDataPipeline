/**
 * 営業日カレンダーの自前生成（J-Quants OFF 中の Cron A 先頭ステップ）
 *
 * @description
 * 内閣府祝日CSVから今日−30日〜＋370日のカレンダーを作り、**まだ無い日だけ**を
 * trading_calendar に入れる（ON CONFLICT DO NOTHING・source='generated'）。J-Quants の行は上書きしない。
 * OFF 中の Cron A の成否記録として job_runs('cron_a') も書く（ON 時は calendar ルートが書く。
 * 未達検知 00061 が cron_a を見続けられるようにするため）。
 * 設計正本: ../../../docs/PLANS-jquants-off-switch-2026-10.md §4.1
 *
 * 残り日数の警告: カレンダーの最終日、または内閣府CSVの収録年末が「今日＋60日」より手前なら
 * 注意メールを送る（翌年の祝日は例年2月ごろに公表される）。
 *
 * 実行: npx tsx scripts/cron/sync-calendar-generated.ts [--dry-run]
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { addDays, getJSTDate } from '../../src/lib/utils/date';
import {
  buildGeneratedCalendar,
  fetchCabinetOfficeHolidays,
} from '../../src/lib/calendar/generated-calendar';
import { startJobRun, completeJobRun } from '../../src/lib/cron/job-run';
import { sendOpsNoticeEmail } from '../../src/lib/notification/email';

const logger = createLogger({ module: 'sync-calendar-generated' });

const LOOKBACK_DAYS = 30;
const LOOKAHEAD_DAYS = 370;
const RUNWAY_WARN_DAYS = 60;

async function main(): Promise<void> {
  const dryRun = process.argv.includes('--dry-run');
  const today = getJSTDate();
  const core = createAdminClient('jquants_core');
  const ingest = createAdminClient('jquants_ingest');

  let runId: string | undefined;
  if (!dryRun) {
    const started = await startJobRun(ingest, {
      jobName: 'cron_a',
      meta: { dataset: 'calendar', mode: 'jquants_off', source: 'generated' },
    });
    if (started.error) {
      logger.info('cron_a の job_run を開始できず（実行済みの可能性）。カレンダー生成は続行する', {
        reason: started.error,
      });
    } else {
      runId = started.runId;
    }
  }

  try {
    const holidays = await fetchCabinetOfficeHolidays();
    const records = buildGeneratedCalendar(
      addDays(today, -LOOKBACK_DAYS),
      addDays(today, LOOKAHEAD_DAYS),
      holidays
    );

    let inserted = 0;
    if (!dryRun && records.length > 0) {
      // 生成行は J-Quants の行を上書きしない（ignoreDuplicates = ON CONFLICT DO NOTHING）
      const { error, count } = await core
        .from('trading_calendar')
        .upsert(records, { onConflict: 'calendar_date', ignoreDuplicates: true, count: 'exact' });
      if (error) throw new Error(`trading_calendar への挿入に失敗しました: ${error.message}`);
      inserted = count ?? 0;
    }

    const { data: maxRow, error: maxError } = await core
      .from('trading_calendar')
      .select('calendar_date')
      .order('calendar_date', { ascending: false })
      .limit(1);
    if (maxError) throw new Error(`trading_calendar の最終日取得に失敗しました: ${maxError.message}`);
    const calendarMax = (maxRow?.[0] as { calendar_date: string } | undefined)?.calendar_date ?? null;

    const runwayLimit = addDays(today, RUNWAY_WARN_DAYS);
    const csvEnd = `${holidays.lastYear}-12-31`;
    const warnings: string[] = [];
    if (!calendarMax || calendarMax < runwayLimit) {
      warnings.push(`営業日カレンダーの最終日が ${calendarMax ?? 'なし'}（今日＋${RUNWAY_WARN_DAYS}日 ${runwayLimit} より手前）`);
    }
    if (csvEnd < runwayLimit) {
      warnings.push(`内閣府祝日CSVの収録が ${csvEnd} まで。翌年分の公表（例年2月）を待っている状態`);
    }

    logger.info('生成カレンダー同期完了', {
      dryRun,
      generated: records.length,
      inserted,
      calendarMax,
      csvLastYear: holidays.lastYear,
      warnings,
    });

    if (warnings.length > 0) {
      for (const w of warnings) console.log(`::warning::${w}`);
      if (!dryRun) {
        await sendOpsNoticeEmail('営業日カレンダーの残りが少なくなっています（J-Quants OFF 中）', [
          ...warnings,
          '対処: 内閣府CSVの更新を待つ（自動で取り込まれる）。急ぐ場合は J-Quants を ON にして公式カレンダーを取得する。',
        ]);
      }
    }

    if (runId) await completeJobRun(ingest, runId, 'success');
  } catch (error) {
    if (runId) {
      await completeJobRun(ingest, runId, 'failed', error instanceof Error ? error.message : String(error));
    }
    throw error;
  }
}

main().catch((error) => {
  logger.error('生成カレンダー同期に失敗しました', {
    error: error instanceof Error ? error.message : String(error),
  });
  process.exit(1);
});
