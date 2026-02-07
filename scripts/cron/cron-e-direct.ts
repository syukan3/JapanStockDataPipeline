/**
 * Cron E 直接実行スクリプト（GH Actions用）
 *
 * @description 優待データ同期（kabuyutai.com + eスマート証券 CSV）
 * - CLI引数: --source=all|kabuyutai|kabu_csv
 * - 環境変数は GH Actions secrets から直接セットされる
 * - job_runs でジョブ実行記録を管理
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { handleCronE } from '../../src/lib/cron/handlers/yutai';
import { CRON_E_SOURCES, type CronESource } from '../../src/lib/yutai/types';

const JOB_NAME = 'cron-e-yutai' as const;

const logger = createLogger({ module: 'cron-e-direct' });

/**
 * CLI 引数をパース
 */
function parseArgs(): { source: CronESource } {
  let source: CronESource = 'all';

  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith('--source=')) {
      const value = arg.split('=')[1];
      if (!(CRON_E_SOURCES as readonly string[]).includes(value)) {
        throw new Error(`Invalid source: ${value}. Valid values: ${CRON_E_SOURCES.join(', ')}`);
      }
      source = value as CronESource;
    }
  }

  return { source };
}

/**
 * 環境変数バリデーション
 */
function validateEnv(): void {
  const required = [
    'NEXT_PUBLIC_SUPABASE_URL',
    'SUPABASE_SERVICE_ROLE_KEY',
  ];

  const missing = required.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

async function main(): Promise<void> {
  const { source } = parseArgs();
  validateEnv();

  logger.info('Starting Cron E direct execution', { source });

  const supabaseIngest = createAdminClient('jquants_ingest');

  // runId を生成し、job_runs に INSERT
  const runId = crypto.randomUUID();

  const { error: insertError } = await supabaseIngest
    .from('job_runs')
    .insert({
      run_id: runId,
      job_name: JOB_NAME,
      status: 'running',
      meta: { source },
    });

  if (insertError) {
    logger.error('Failed to insert job_runs', { error: insertError.message });
    throw new Error(`Failed to start job run: ${insertError.message}`);
  }

  try {
    const result = await handleCronE(source, runId);

    const finalStatus = result.success ? 'success' : 'failed';
    await supabaseIngest
      .from('job_runs')
      .update({
        status: finalStatus,
        finished_at: new Date().toISOString(),
        error_message: result.errors.length > 0 ? result.errors.join('; ') : null,
        meta: {
          source,
          benefitsUpserted: result.benefitsUpserted,
          inventoryUpserted: result.inventoryUpserted,
        },
      })
      .eq('run_id', runId);

    const output = {
      success: result.success,
      runId,
      source,
      benefitsUpserted: result.benefitsUpserted,
      inventoryUpserted: result.inventoryUpserted,
      errors: result.errors.length > 0 ? result.errors : undefined,
    };

    logger.info('Cron E completed', output);
    console.log(JSON.stringify(output, null, 2));

    if (!result.success) {
      process.exit(1);
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Cron E failed with exception', { runId, source, error: errorMessage });

    await supabaseIngest
      .from('job_runs')
      .update({
        status: 'failed',
        finished_at: new Date().toISOString(),
        error_message: errorMessage,
      })
      .eq('run_id', runId)
      .then(() => {}, () => {});

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
