/**
 * Cron E ハンドラー: 優待データ同期
 *
 * @description kabuyutai.com から優待情報、eスマート証券 CSV から在庫情報を取得し、
 * jquants_core テーブルに UPSERT する
 */

import { createLogger, type LogContext } from '../../utils/logger';
import { createAdminClient } from '../../supabase/admin';
import { sendJobFailureEmail } from '../../notification/email';
import { fetchAllYutaiBenefits } from '../../yutai/kabuyutai-client';
import { fetchMarginInventoryCsv } from '../../yutai/kabu-csv-client';
import type { CronESource, CronEResult, YutaiBenefit, MarginInventory } from '../../yutai/types';

const logger = createLogger({ module: 'cron-e-yutai' });

const JOB_NAME = 'cron-e-yutai';
const UPSERT_BATCH_SIZE = 500;

/**
 * 優待情報を UPSERT
 */
async function upsertBenefits(
  benefits: YutaiBenefit[],
  logContext: LogContext,
): Promise<{ upserted: number; errors: string[] }> {
  const supabaseCore = createAdminClient('jquants_core');
  let totalUpserted = 0;
  const errors: string[] = [];
  const now = new Date().toISOString();

  const rows = benefits.map((b) => ({
    local_code: b.local_code,
    company_name: b.company_name,
    min_shares: b.min_shares,
    benefit_content: b.benefit_content,
    benefit_value: b.benefit_value,
    record_month: b.record_month,
    record_day: b.record_day,
    category: b.category,
    source: 'kabuyutai',
    fetched_at: now,
    updated_at: now,
  }));

  for (let i = 0; i < rows.length; i += UPSERT_BATCH_SIZE) {
    const batch = rows.slice(i, i + UPSERT_BATCH_SIZE);
    const { error } = await supabaseCore
      .from('yutai_benefit')
      .upsert(batch, { onConflict: 'local_code,min_shares,record_month' });

    if (error) {
      logger.error('Failed to upsert benefits', {
        ...logContext,
        batchIndex: i,
        error: error.message,
      });
      errors.push(`benefits batch ${i}: ${error.message}`);
    } else {
      totalUpserted += batch.length;
    }
  }

  return { upserted: totalUpserted, errors };
}

/**
 * 在庫情報を UPSERT
 */
async function upsertInventory(
  inventory: MarginInventory[],
  logContext: LogContext,
): Promise<{ upserted: number; errors: string[] }> {
  const supabaseCore = createAdminClient('jquants_core');
  let totalUpserted = 0;
  const errors: string[] = [];
  const now = new Date().toISOString();

  const rows = inventory.map((inv) => ({
    local_code: inv.local_code,
    broker: inv.broker,
    inventory_date: inv.inventory_date,
    inventory_qty: inv.inventory_qty,
    is_available: inv.is_available,
    loan_type: inv.loan_type,
    loan_term: inv.loan_term,
    premium_fee: inv.premium_fee,
    source: inv.source,
    fetched_at: now,
  }));

  for (let i = 0; i < rows.length; i += UPSERT_BATCH_SIZE) {
    const batch = rows.slice(i, i + UPSERT_BATCH_SIZE);
    const { error } = await supabaseCore
      .from('margin_inventory')
      .upsert(batch, { onConflict: 'local_code,broker,inventory_date,loan_type' });

    if (error) {
      logger.error('Failed to upsert inventory', {
        ...logContext,
        batchIndex: i,
        error: error.message,
      });
      errors.push(`inventory batch ${i}: ${error.message}`);
    } else {
      totalUpserted += batch.length;
    }
  }

  return { upserted: totalUpserted, errors };
}

/**
 * JST日付を取得
 */
function getJSTDateString(): string {
  return new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Tokyo' });
}

/**
 * Cron E メインハンドラー
 */
export async function handleCronE(
  source: CronESource,
  runId: string,
): Promise<CronEResult> {
  const logContext: LogContext = {
    jobName: JOB_NAME,
    runId,
  };

  logger.info('Starting Cron E handler', { source, runId });

  const result: CronEResult = {
    success: true,
    source,
    benefitsUpserted: 0,
    inventoryUpserted: 0,
    errors: [],
  };

  try {
    // kabuyutai.com から優待情報取得
    if (source === 'kabuyutai' || source === 'all') {
      logger.info('Fetching kabuyutai benefits', logContext);
      const benefits = await fetchAllYutaiBenefits();

      if (benefits.length > 0) {
        const { upserted, errors } = await upsertBenefits(benefits, logContext);
        result.benefitsUpserted = upserted;
        result.errors.push(...errors);
      }

      logger.info('kabuyutai benefits processed', {
        ...logContext,
        fetched: benefits.length,
        upserted: result.benefitsUpserted,
      });
    }

    // eスマート証券 CSV から在庫情報取得
    if (source === 'kabu_csv' || source === 'all') {
      const inventoryDate = getJSTDateString();
      logger.info('Fetching margin inventory CSV', { ...logContext, inventoryDate });

      const inventory = await fetchMarginInventoryCsv(inventoryDate);

      if (inventory.length > 0) {
        const { upserted, errors } = await upsertInventory(inventory, logContext);
        result.inventoryUpserted = upserted;
        result.errors.push(...errors);
      }

      logger.info('Margin inventory processed', {
        ...logContext,
        fetched: inventory.length,
        upserted: result.inventoryUpserted,
      });
    }

    if (result.errors.length > 0) {
      result.success = false;
    }

    logger.info('Cron E handler completed', {
      source,
      runId,
      benefitsUpserted: result.benefitsUpserted,
      inventoryUpserted: result.inventoryUpserted,
      errorCount: result.errors.length,
    });

    return result;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Cron E handler failed', {
      source,
      runId,
      error: errorMessage,
    });

    try {
      await sendJobFailureEmail({
        jobName: JOB_NAME,
        error: errorMessage,
        runId,
        dataset: source,
        timestamp: new Date(),
      });
    } catch (notifyError) {
      logger.error('Failed to send failure notification', {
        error: notifyError instanceof Error ? notifyError.message : String(notifyError),
      });
    }

    return {
      success: false,
      source,
      benefitsUpserted: result.benefitsUpserted,
      inventoryUpserted: result.inventoryUpserted,
      errors: [...result.errors, errorMessage],
    };
  }
}
