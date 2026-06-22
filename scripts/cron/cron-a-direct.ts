/**
 * Cron A 直接実行スクリプト（GH Actions用）
 *
 * @description Vercel 10秒制限を回避するため、GH Actionsランナー上で直接実行
 * - CLI引数でdatasetを指定: npx tsx scripts/cron/cron-a-direct.ts <dataset>
 * - 対応dataset: equity_bars, topix, financial, equity_master
 * - 環境変数は GH Actions secrets から直接セットされる（.env.local 不要）
 * - job_runs は使わない（複数datasetが同一job_name+target_dateで衝突するため）
 * - sync関数はupsertなので冪等性あり
 *
 * ## 対象日の決め方（前方フィル方式）
 * - equity_bars / topix / financial（日次・取引日単位）:
 *   実データテーブルの最新日付（overlap by 1: 最新日そのものを含む）〜 今日 までの
 *   営業日を順に取り込む。これにより「実行が JST 日付をまたいだ」「数日落ちた」等で
 *   抜けた営業日を自己修復的にバックフィルする（旧実装は実行時刻の当日のみを要求
 *   していたため、GH Actions のスケジュール遅延で日付がロールオーバーすると
 *   取りこぼしていた）。
 * - equity_master（日次スナップショット / SCD Type2）:
 *   取引日ではなく「現時点の銘柄マスタ」を表すため、当日（営業日のみ）を対象とする。
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger, type LogContext } from '../../src/lib/utils/logger';
import { syncEquityBarsDailyForDate, syncEquityMasterSCD } from '../../src/lib/jquants/endpoints';
import { syncTopixBarsDailyForDate } from '../../src/lib/jquants/endpoints/index-topix';
import { syncFinancialSummaryForDate } from '../../src/lib/jquants/endpoints/fins-summary';
import { getJSTDate } from '../../src/lib/utils/date';
import { isBusinessDayInDB } from '../../src/lib/cron/business-day';
import { runForwardFill, type ForwardFillDatasetConfig } from '../../src/lib/cron/forward-fill';

const SUPPORTED_DATASETS = ['equity_bars', 'topix', 'financial', 'equity_master'] as const;
type Dataset = (typeof SUPPORTED_DATASETS)[number];

const PER_DATE_DATASETS: Record<'equity_bars' | 'topix' | 'financial', ForwardFillDatasetConfig> = {
  equity_bars: { dataset: 'equity_bars', table: 'equity_bar_daily', dateColumn: 'trade_date', sync: syncEquityBarsDailyForDate, allowEmpty: false },
  topix: { dataset: 'topix', table: 'topix_bar_daily', dateColumn: 'trade_date', sync: syncTopixBarsDailyForDate, allowEmpty: false },
  financial: { dataset: 'financial', table: 'financial_disclosure', dateColumn: 'disclosed_date', sync: syncFinancialSummaryForDate, allowEmpty: true },
};

/**
 * 正の整数の環境変数を取得（不正値は throw）。
 * 0 / NaN / 負値を黙って通すと slice(0, 0/NaN) で「対象日なし」と誤認するため。
 */
function getPositiveIntEnv(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') {
    return defaultValue;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid ${name}: "${raw}" (must be a positive integer)`);
  }
  return value;
}

/** 1回の実行で遡る最大日数（カレンダー日） */
const MAX_BACKFILL_DAYS = getPositiveIntEnv('SYNC_MAX_BACKFILL_DAYS', 60);
/** 1回の実行で処理する最大営業日数（タイムアウト防止。古い日から順に処理） */
const MAX_DAYS_PER_RUN = getPositiveIntEnv('SYNC_MAX_DAYS_PER_RUN', 20);

/**
 * 環境変数バリデーション
 */
function validateEnv(): void {
  const required = [
    'JQUANTS_API_KEY',
    'NEXT_PUBLIC_SUPABASE_URL',
    'SUPABASE_SERVICE_ROLE_KEY',
  ];

  const missing = required.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

function parseDataset(): Dataset {
  const arg = process.argv[2];
  if (!arg) {
    throw new Error(`Usage: npx tsx scripts/cron/cron-a-direct.ts <dataset>\nSupported datasets: ${SUPPORTED_DATASETS.join(', ')}`);
  }
  if (!(SUPPORTED_DATASETS as readonly string[]).includes(arg)) {
    throw new Error(`Unknown dataset: ${arg}\nSupported datasets: ${SUPPORTED_DATASETS.join(', ')}`);
  }
  return arg as Dataset;
}

/**
 * 銘柄マスタ（日次スナップショット）の処理。当日（営業日のみ）を対象とする。
 */
async function runEquityMaster(
  logger: ReturnType<typeof createLogger>,
  supabaseCore: SupabaseClient
): Promise<{ targetDates: string[]; fetched: number; inserted: number }> {
  const today = getJSTDate();
  const isBizDay = await isBusinessDayInDB(supabaseCore, today);
  if (!isBizDay) {
    logger.info('Today is not a business day, skipping equity_master', { today });
    return { targetDates: [], fetched: 0, inserted: 0 };
  }

  const logContext: LogContext = { jobName: 'cron_a', dataset: 'equity_master' };
  const result = await syncEquityMasterSCD(today, { logContext });

  logger.info('equity_master sync completed', {
    targetDate: today,
    fetched: result.fetched,
    inserted: result.inserted,
  });

  return { targetDates: [today], fetched: result.fetched, inserted: result.inserted };
}

async function main(): Promise<void> {
  const dataset = parseDataset();
  validateEnv();

  const logger = createLogger({ module: 'cron-a-direct', dataset });
  const supabaseCore = createAdminClient('jquants_core');

  logger.info(`Processing ${dataset}`);

  const { targetDates, fetched, inserted } =
    dataset === 'equity_master'
      ? await runEquityMaster(logger, supabaseCore)
      : await runForwardFill(supabaseCore, PER_DATE_DATASETS[dataset], {
          maxBackfillDays: MAX_BACKFILL_DAYS,
          maxDaysPerRun: MAX_DAYS_PER_RUN,
        });

  logger.info(`${dataset} sync completed`, {
    targetDates,
    fetched,
    inserted,
  });

  const output = {
    success: true,
    dataset,
    targetDates,
    fetched,
    inserted,
  };

  console.log(JSON.stringify(output, null, 2));
}

main()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    const logger = createLogger({ module: 'cron-a-direct' });
    logger.error('Script failed', { error: error instanceof Error ? error.message : String(error) });
    process.exit(1);
  });
