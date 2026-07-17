/**
 * Cron F 直接実行スクリプト（GH Actions用）
 *
 * @description 信用取引週末残高の週次同期（J-Quants Standard /v2/markets/margin-interest）
 * - 直近35日ウィンドウ再取得（欠落週・訂正・祝日ずれを吸収、冪等upsert）
 * - 同期後にプルーニング（全銘柄1年保持・保有+ウォッチ銘柄は全期間保持）
 * - 環境変数は GH Actions secrets から直接セットされる
 * - job_runs でジョブ実行記録を管理
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import {
  syncWeeklyMarginInterestWithWindow,
  pruneWeeklyMarginInterest,
} from '../../src/lib/jquants/endpoints/weekly-margin-interest';

const JOB_NAME = 'weekly-margin' as const;

const logger = createLogger({ module: 'cron-f-direct' });

/**
 * 環境変数バリデーション
 */
function validateEnv(): void {
  const required = [
    'NEXT_PUBLIC_SUPABASE_URL',
    'SUPABASE_SERVICE_ROLE_KEY',
    'JQUANTS_API_KEY',
  ];

  const missing = required.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

async function main(): Promise<void> {
  validateEnv();

  logger.info('Starting Cron F direct execution');

  const supabaseIngest = createAdminClient('jquants_ingest');

  // runId を生成し、job_runs に INSERT
  const runId = crypto.randomUUID();

  const { error: insertError } = await supabaseIngest
    .from('job_runs')
    .insert({
      run_id: runId,
      job_name: JOB_NAME,
      status: 'running',
      meta: {},
    });

  if (insertError) {
    logger.error('Failed to insert job_runs', { error: insertError.message });
    throw new Error(`Failed to start job run: ${insertError.message}`);
  }

  try {
    const syncResult = await syncWeeklyMarginInterestWithWindow();
    const pruneResult = await pruneWeeklyMarginInterest();

    const success = syncResult.errors.length === 0;
    const finalStatus = success ? 'success' : 'failed';
    await supabaseIngest
      .from('job_runs')
      .update({
        status: finalStatus,
        finished_at: new Date().toISOString(),
        error_message:
          syncResult.errors.length > 0
            ? syncResult.errors.map((e) => e.message).join('; ')
            : null,
        meta: {
          fetched: syncResult.fetched,
          inserted: syncResult.inserted,
          pruned: pruneResult.deleted,
          protectedCount: pruneResult.protectedCount,
        },
      })
      .eq('run_id', runId);

    const output = {
      success,
      runId,
      fetched: syncResult.fetched,
      inserted: syncResult.inserted,
      pruned: pruneResult.deleted,
      protectedCount: pruneResult.protectedCount,
      errors: syncResult.errors.length > 0 ? syncResult.errors.map((e) => e.message) : undefined,
    };

    logger.info('Cron F completed', output);
    console.log(JSON.stringify(output, null, 2));

    if (!success) {
      process.exit(1);
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Cron F failed with exception', { runId, error: errorMessage });

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
