/**
 * Cron D 直接実行スクリプト（GH Actions用）
 *
 * @description Vercel タイムアウト制限を回避するため、GH Actionsランナー上で直接実行
 * - CLI引数: --source=all|fred|estat, --backfill-days=N
 * - 環境変数は GH Actions secrets から直接セットされる（.env.local 不要）
 * - job_runs でジョブ実行記録を管理
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { handleCronD, CRON_D_SOURCES, type CronDSource } from '../../src/lib/cron/handlers/macro';

const JOB_NAME = 'cron-d-macro' as const;

const logger = createLogger({ module: 'cron-d-direct' });

/**
 * CLI 引数をパース
 */
function parseArgs(): { source: CronDSource; backfillDays: number } {
  let source: CronDSource = 'all';
  let backfillDays = 0;

  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith('--source=')) {
      const value = arg.split('=')[1];
      if (!(CRON_D_SOURCES as readonly string[]).includes(value)) {
        throw new Error(`Invalid source: ${value}. Valid values: ${CRON_D_SOURCES.join(', ')}`);
      }
      source = value as CronDSource;
    } else if (arg.startsWith('--backfill-days=')) {
      const value = parseInt(arg.split('=')[1], 10);
      if (isNaN(value) || value < 0) {
        throw new Error(`Invalid backfill-days: ${arg.split('=')[1]}. Must be a non-negative integer.`);
      }
      backfillDays = value;
    }
  }

  return { source, backfillDays };
}

/**
 * 環境変数バリデーション
 */
function validateEnv(): void {
  const required = [
    'NEXT_PUBLIC_SUPABASE_URL',
    'SUPABASE_SERVICE_ROLE_KEY',
    'FRED_API_KEY',
    'ESTAT_API_KEY',
  ];

  const missing = required.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

async function main(): Promise<void> {
  const { source, backfillDays } = parseArgs();
  validateEnv();

  logger.info('Starting Cron D direct execution', { source, backfillDays });

  const supabaseIngest = createAdminClient('jquants_ingest');

  // runId を生成し、job_runs に INSERT
  const runId = crypto.randomUUID();

  const { error: insertError } = await supabaseIngest
    .from('job_runs')
    .insert({
      run_id: runId,
      job_name: JOB_NAME,
      status: 'running',
      meta: { source, backfill_days: backfillDays },
    });

  if (insertError) {
    logger.error('Failed to insert job_runs', { error: insertError.message });
    throw new Error(`Failed to start job run: ${insertError.message}`);
  }

  try {
    // ハンドラー実行
    const result = await handleCronD(source, runId, backfillDays);

    // job_runs を更新
    const finalStatus = result.success ? 'success' : 'failed';
    await supabaseIngest
      .from('job_runs')
      .update({
        status: finalStatus,
        finished_at: new Date().toISOString(),
        error_message: result.errors.length > 0 ? result.errors.join('; ') : null,
        meta: {
          source,
          backfill_days: backfillDays,
          seriesProcessed: result.seriesProcessed,
          rowsUpserted: result.rowsUpserted,
          skippedValues: result.skippedValues,
        },
      })
      .eq('run_id', runId);

    const output = {
      success: result.success,
      runId,
      source,
      seriesProcessed: result.seriesProcessed,
      rowsUpserted: result.rowsUpserted,
      skippedValues: result.skippedValues,
      errors: result.errors.length > 0 ? result.errors : undefined,
    };

    logger.info('Cron D completed', output);
    console.log(JSON.stringify(output, null, 2));

    if (!result.success) {
      process.exit(1);
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Cron D failed with exception', { runId, source, error: errorMessage });

    // job_runs を失敗として更新
    await supabaseIngest
      .from('job_runs')
      .update({
        status: 'failed',
        finished_at: new Date().toISOString(),
        error_message: errorMessage,
      })
      .eq('run_id', runId)
      .then(() => {}, () => {}); // クリーンアップ失敗は握りつぶす

    throw error;
  }
}

main()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    logger.error('Script failed', { error: error instanceof Error ? error.message : String(error) });
    process.exit(1);
  });
