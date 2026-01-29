/**
 * Cron A 直接実行スクリプト（GH Actions用）
 *
 * @description Vercel 10秒制限を回避するため、GH Actionsランナー上で直接実行
 * - CLI引数でdatasetを指定: npx tsx scripts/cron/cron-a-direct.ts <dataset>
 * - 対応dataset: equity_bars, equity_master
 * - 環境変数は GH Actions secrets から直接セットされる（.env.local 不要）
 * - job_runs は使わない（複数datasetが同一job_name+target_dateで衝突するため）
 * - sync関数はupsertなので冪等性あり
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger, type LogContext } from '../../src/lib/utils/logger';
import { syncEquityBarsDailyForDate, syncEquityMasterForDate } from '../../src/lib/jquants/endpoints';
import { getJSTDate } from '../../src/lib/utils/date';
import { getPreviousBusinessDay } from '../../src/lib/cron/business-day';

const SUPPORTED_DATASETS = ['equity_bars', 'equity_master'] as const;
type Dataset = (typeof SUPPORTED_DATASETS)[number];

/**
 * dataset別のsync関数ディスパッチ
 */
function getSyncFn(dataset: Dataset): (date: string, options: { logContext: LogContext }) => Promise<{ fetched: number; inserted: number; pageCount?: number }> {
  switch (dataset) {
    case 'equity_bars':
      return syncEquityBarsDailyForDate;
    case 'equity_master':
      return (date, options) => syncEquityMasterForDate(date, options);
  }
}

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

async function main(): Promise<void> {
  const dataset = parseDataset();
  validateEnv();

  const logger = createLogger({ module: 'cron-a-direct', dataset });
  const syncFn = getSyncFn(dataset);

  const supabaseCore = createAdminClient('jquants_core');

  // 処理対象日を決定（前営業日）
  const today = getJSTDate();
  const targetDate = await getPreviousBusinessDay(supabaseCore, today);

  if (!targetDate) {
    throw new Error('Could not determine previous business day (calendar data may be missing)');
  }

  logger.info(`Processing ${dataset}`, { targetDate });

  const logContext: LogContext = {
    jobName: 'cron_a',
    dataset,
  };

  const result = await syncFn(targetDate, { logContext });

  logger.info(`${dataset} sync completed`, {
    targetDate,
    fetched: result.fetched,
    inserted: result.inserted,
    pageCount: result.pageCount,
  });

  const output = {
    success: true,
    dataset,
    targetDates: [targetDate],
    fetched: result.fetched,
    inserted: result.inserted,
    pageCount: result.pageCount,
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
