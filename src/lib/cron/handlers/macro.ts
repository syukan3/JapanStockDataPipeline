/**
 * Cron D ハンドラー: マクロ経済データ同期
 *
 * @description FRED API / e-Stat API からマクロ経済指標を取得し、
 * macro_indicator_daily に UPSERT する
 */

import { z } from 'zod';
import { createLogger, type LogContext } from '../../utils/logger';
import { createFredClient } from '../../fred/client';
import { createEStatClient } from '../../estat/client';
import { isMonthlyOrLower } from '../../fred/series-config';
import { createAdminClient } from '../../supabase/admin';
import { sendJobFailureEmail } from '../../notification/email';

const logger = createLogger({ module: 'cron-d-macro' });

/** Cron D で処理するソース */
export const CRON_D_SOURCES = ['fred', 'estat', 'all'] as const;
export type CronDSource = (typeof CRON_D_SOURCES)[number];

/** リクエストボディのスキーマ */
export const CronDRequestSchema = z.object({
  source: z.enum(CRON_D_SOURCES).default('all'),
  backfill_days: z.number().int().min(0).default(0),
});

export type CronDRequest = z.infer<typeof CronDRequestSchema>;

/** Cron D の結果 */
export interface CronDResult {
  success: boolean;
  source: CronDSource;
  seriesProcessed: number;
  rowsUpserted: number;
  skippedValues: number;
  errors: string[];
}

/** ジョブ名 */
const JOB_NAME = 'cron-d-macro';

/** 月次指標のvintage再取得日数（直近3ヶ月） */
const VINTAGE_REFETCH_DAYS = 90;

/** 初回バックフィル日数（2年分） */
const INITIAL_BACKFILL_DAYS = 730;

/** 系列メタデータの型 */
interface SeriesMetadata {
  series_id: string;
  source: 'fred' | 'estat';
  source_series_id: string;
  source_filter: Record<string, string> | null;
  frequency: string;
  last_value_date: string | null;
}

/**
 * 日付を YYYY-MM-DD 形式で返す（N日前）
 */
function daysAgo(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

/**
 * FRED 系列を処理
 */
async function processFredSeries(
  seriesList: SeriesMetadata[],
  backfillDays: number,
  logContext: LogContext
): Promise<{ rowsUpserted: number; skippedValues: number; errors: string[] }> {
  const fredClient = createFredClient({ logContext });
  const supabaseCore = createAdminClient('jquants_core');

  let totalUpserted = 0;
  let totalSkipped = 0;
  const errors: string[] = [];

  for (const series of seriesList) {
    try {
      // 取得開始日を決定
      let observationStart: string;

      if (backfillDays > 0) {
        // 明示的バックフィル
        observationStart = daysAgo(backfillDays);
      } else if (!series.last_value_date) {
        // 初回: 2年分バックフィル
        observationStart = daysAgo(INITIAL_BACKFILL_DAYS);
      } else if (isMonthlyOrLower(series.frequency)) {
        // 月次指標: 直近3ヶ月再取得（vintage対応）
        observationStart = daysAgo(VINTAGE_REFETCH_DAYS);
      } else {
        // 差分のみ: last_value_date から
        observationStart = series.last_value_date;
      }

      logger.info('Fetching FRED series', {
        seriesId: series.series_id,
        observationStart,
      });

      const { observations, skippedCount } = await fredClient.getSeriesObservations(
        series.source_series_id,
        observationStart
      );

      totalSkipped += skippedCount;

      if (observations.length === 0) {
        logger.info('No new data for FRED series', { seriesId: series.series_id });
        continue;
      }

      // macro_indicator_daily に UPSERT
      const rows = observations.map((obs) => ({
        indicator_date: obs.date,
        series_id: series.series_id,
        source: 'fred' as const,
        value: obs.value,
        released_at: obs.releasedAt,
        updated_at: new Date().toISOString(),
      }));

      // バッチ UPSERT（1000行ずつ）
      const batchSize = 1000;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        const { error } = await supabaseCore
          .from('macro_indicator_daily')
          .upsert(batch, { onConflict: 'indicator_date,series_id' });

        if (error) {
          logger.error('Failed to upsert FRED data', {
            seriesId: series.series_id,
            batchIndex: i,
            error: error.message,
          });
          errors.push(`${series.series_id}: ${error.message}`);
          continue;
        }
      }

      totalUpserted += rows.length;

      // macro_series_metadata を更新
      const maxDate = observations.reduce(
        (max, obs) => (obs.date > max ? obs.date : max),
        observations[0].date
      );

      await supabaseCore
        .from('macro_series_metadata')
        .update({
          last_fetched_at: new Date().toISOString(),
          last_value_date: maxDate,
        })
        .eq('series_id', series.series_id);

      logger.info('FRED series processed', {
        seriesId: series.series_id,
        rowsUpserted: rows.length,
        skipped: skippedCount,
      });
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      logger.error('Failed to process FRED series', {
        seriesId: series.series_id,
        error: msg,
      });
      errors.push(`${series.series_id}: ${msg}`);
    }
  }

  return { rowsUpserted: totalUpserted, skippedValues: totalSkipped, errors };
}

/**
 * e-Stat 系列を処理
 */
async function processEStatSeries(
  seriesList: SeriesMetadata[],
  logContext: LogContext
): Promise<{ rowsUpserted: number; skippedValues: number; errors: string[] }> {
  const estatClient = createEStatClient({ logContext });
  const supabaseCore = createAdminClient('jquants_core');

  let totalUpserted = 0;
  let totalSkipped = 0;
  const errors: string[] = [];

  for (const series of seriesList) {
    try {
      logger.info('Fetching e-Stat series', {
        seriesId: series.series_id,
        statsDataId: series.source_series_id,
      });

      const { observations, skippedCount } = await estatClient.getStatsData(
        series.source_series_id,
        series.source_filter
      );

      totalSkipped += skippedCount;

      if (observations.length === 0) {
        logger.info('No data for e-Stat series', { seriesId: series.series_id });
        continue;
      }

      // last_value_date 以降のデータのみ UPSERT（ただし初回は全件）
      const filteredObs = series.last_value_date
        ? observations.filter((obs) => obs.date >= series.last_value_date!)
        : observations;

      if (filteredObs.length === 0) {
        logger.info('No new data for e-Stat series', { seriesId: series.series_id });
        continue;
      }

      // macro_indicator_daily に UPSERT
      const rows = filteredObs.map((obs) => ({
        indicator_date: obs.date,
        series_id: series.series_id,
        source: 'estat' as const,
        value: obs.value,
        released_at: new Date().toISOString(), // e-Stat は取得日時を暫定記録
        updated_at: new Date().toISOString(),
      }));

      const batchSize = 1000;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        const { error } = await supabaseCore
          .from('macro_indicator_daily')
          .upsert(batch, { onConflict: 'indicator_date,series_id' });

        if (error) {
          logger.error('Failed to upsert e-Stat data', {
            seriesId: series.series_id,
            batchIndex: i,
            error: error.message,
          });
          errors.push(`${series.series_id}: ${error.message}`);
          continue;
        }
      }

      totalUpserted += rows.length;

      // macro_series_metadata を更新
      const maxDate = filteredObs.reduce(
        (max, obs) => (obs.date > max ? obs.date : max),
        filteredObs[0].date
      );

      await supabaseCore
        .from('macro_series_metadata')
        .update({
          last_fetched_at: new Date().toISOString(),
          last_value_date: maxDate,
        })
        .eq('series_id', series.series_id);

      logger.info('e-Stat series processed', {
        seriesId: series.series_id,
        rowsUpserted: rows.length,
        skipped: skippedCount,
      });
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      logger.error('Failed to process e-Stat series', {
        seriesId: series.series_id,
        error: msg,
      });
      errors.push(`${series.series_id}: ${msg}`);
    }
  }

  return { rowsUpserted: totalUpserted, skippedValues: totalSkipped, errors };
}

/**
 * Cron D メインハンドラー
 */
export async function handleCronD(
  source: CronDSource,
  runId: string,
  backfillDays: number = 0
): Promise<CronDResult> {
  const logContext: LogContext = {
    jobName: JOB_NAME,
    runId,
  };

  logger.info('Starting Cron D handler', { source, runId, backfillDays });

  const result: CronDResult = {
    success: true,
    source,
    seriesProcessed: 0,
    rowsUpserted: 0,
    skippedValues: 0,
    errors: [],
  };

  try {
    const supabaseCore = createAdminClient('jquants_core');

    // macro_series_metadata から対象系列一覧を取得
    let query = supabaseCore.from('macro_series_metadata').select('*');

    if (source !== 'all') {
      query = query.eq('source', source);
    }

    const { data: seriesList, error: fetchError } = await query;

    if (fetchError) {
      throw new Error(`Failed to fetch series metadata: ${fetchError.message}`);
    }

    if (!seriesList || seriesList.length === 0) {
      logger.info('No series to process', { source });
      return result;
    }

    const fredSeries = seriesList.filter((s: SeriesMetadata) => s.source === 'fred');
    const estatSeries = seriesList.filter((s: SeriesMetadata) => s.source === 'estat');

    // FRED 系列を処理
    if (fredSeries.length > 0) {
      const fredResult = await processFredSeries(fredSeries, backfillDays, logContext);
      result.rowsUpserted += fredResult.rowsUpserted;
      result.skippedValues += fredResult.skippedValues;
      result.errors.push(...fredResult.errors);
      result.seriesProcessed += fredSeries.length;
    }

    // e-Stat 系列を処理
    if (estatSeries.length > 0) {
      const estatResult = await processEStatSeries(estatSeries, logContext);
      result.rowsUpserted += estatResult.rowsUpserted;
      result.skippedValues += estatResult.skippedValues;
      result.errors.push(...estatResult.errors);
      result.seriesProcessed += estatSeries.length;
    }

    // エラーがあっても部分成功として扱う
    if (result.errors.length > 0) {
      result.success = result.errors.length < seriesList.length; // 全系列失敗なら false
    }

    logger.info('Cron D handler completed', {
      source,
      runId,
      seriesProcessed: result.seriesProcessed,
      rowsUpserted: result.rowsUpserted,
      skippedValues: result.skippedValues,
      errorCount: result.errors.length,
    });

    return result;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Cron D handler failed', {
      source,
      runId,
      error: errorMessage,
    });

    // 失敗通知を送信
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
      seriesProcessed: result.seriesProcessed,
      rowsUpserted: result.rowsUpserted,
      skippedValues: result.skippedValues,
      errors: [...result.errors, errorMessage],
    };
  }
}
