/**
 * DB自動アーカイブスクリプト: equity_bar_daily
 *
 * @description DB容量が閾値を超えた場合、最古の半年分をCSV.gzとして
 *   Supabase Storageにエクスポートし、DBから削除する。
 *
 * - CLI引数: --threshold-mb=N (デフォルト: 450)
 * - 環境変数: NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,
 *             SUPABASE_ACCESS_TOKEN, RESEND_API_KEY, ALERT_EMAIL_TO
 */

import { gzipSync } from 'node:zlib';
import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { sendJobFailureEmail, sendJobSuccessEmail } from '../../src/lib/notification/email';

const JOB_NAME = 'db-archival' as const;
const STORAGE_BUCKET = 'db-archives';
const TABLE = 'equity_bar_daily';

/** 半年分の営業日数（約125日） */
const ARCHIVE_TRADING_DAYS = 125;

/** DBに最低限残す営業日数（安全装置） */
const MIN_REMAINING_TRADING_DAYS = 300;

/** デフォルト閾値 (MB) */
const DEFAULT_THRESHOLD_MB = 450;

/** ページあたりの行数 */
const PAGE_SIZE = 1000;

const logger = createLogger({ module: 'archive-equity-bars' });

// ---------------------------------------------------------------------------
// CLI Args
// ---------------------------------------------------------------------------

function parseArgs(): { thresholdMb: number } {
  let thresholdMb = DEFAULT_THRESHOLD_MB;

  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith('--threshold-mb=')) {
      const value = parseInt(arg.split('=')[1], 10);
      if (isNaN(value) || value <= 0) {
        throw new Error(`Invalid threshold-mb: ${arg.split('=')[1]}. Must be a positive integer.`);
      }
      thresholdMb = value;
    }
  }

  return { thresholdMb };
}

// ---------------------------------------------------------------------------
// Env validation
// ---------------------------------------------------------------------------

function validateEnv(): void {
  const required = [
    'NEXT_PUBLIC_SUPABASE_URL',
    'SUPABASE_SERVICE_ROLE_KEY',
    'SUPABASE_ACCESS_TOKEN',
  ];

  const missing = required.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

// ---------------------------------------------------------------------------
// Management API helpers
// ---------------------------------------------------------------------------

function getManagementApiConfig(): { projectRef: string; accessToken: string } {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
  // Extract project ref from URL: https://<ref>.supabase.co
  const match = supabaseUrl.match(/https:\/\/([^.]+)\.supabase\.co/);
  if (!match) {
    throw new Error(`Cannot extract project ref from URL: ${supabaseUrl}`);
  }
  return {
    projectRef: match[1],
    accessToken: process.env.SUPABASE_ACCESS_TOKEN!,
  };
}

/** Execute SQL via Supabase Management API */
async function executeSql(sql: string): Promise<{ rows: Record<string, unknown>[] }> {
  const { projectRef, accessToken } = getManagementApiConfig();
  const url = `https://api.supabase.com/v1/projects/${projectRef}/database/query`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query: sql }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Management API error (${response.status}): ${body}`);
  }

  return response.json() as Promise<{ rows: Record<string, unknown>[] }>;
}

// ---------------------------------------------------------------------------
// DB Size check
// ---------------------------------------------------------------------------

async function getDbSizeMb(): Promise<number> {
  const result = await executeSql(
    `SELECT pg_database_size(current_database()) AS size_bytes`
  );
  const sizeBytes = Number(result.rows[0]?.size_bytes ?? 0);
  return sizeBytes / (1024 * 1024);
}

// ---------------------------------------------------------------------------
// Data range
// ---------------------------------------------------------------------------

interface DataRange {
  minDate: string;
  maxDate: string;
  tradingDayCount: number;
}

async function getDataRange(): Promise<DataRange | null> {
  const result = await executeSql(
    `SELECT
       MIN(trade_date) AS min_date,
       MAX(trade_date) AS max_date,
       COUNT(DISTINCT trade_date) AS cnt
     FROM jquants_core.${TABLE}`
  );

  const row = result.rows[0];
  if (!row || !row.min_date || !row.max_date) {
    return null; // テーブルが空
  }

  return {
    minDate: String(row.min_date),
    maxDate: String(row.max_date),
    tradingDayCount: Number(row.cnt ?? 0),
  };
}

// ---------------------------------------------------------------------------
// Cutoff calculation
// ---------------------------------------------------------------------------

async function getArchiveCutoffDate(
  archiveDays: number
): Promise<string> {
  // Get the Nth distinct trade_date (ascending) to determine cutoff
  const result = await executeSql(
    `SELECT trade_date FROM (
       SELECT DISTINCT trade_date
       FROM jquants_core.${TABLE}
       ORDER BY trade_date ASC
       LIMIT ${archiveDays}
     ) sub
     ORDER BY trade_date DESC
     LIMIT 1`
  );

  if (!result.rows[0]) {
    throw new Error(`Not enough trading days to archive ${archiveDays} days`);
  }

  return String(result.rows[0].trade_date);
}

// ---------------------------------------------------------------------------
// CSV export (streaming page-by-page to avoid OOM)
// ---------------------------------------------------------------------------

function toCsvRow(row: Record<string, unknown>): string {
  return Object.values(row)
    .map((v) => {
      if (v === null || v === undefined) return '';
      const str = String(v);
      if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
      }
      return str;
    })
    .join(',');
}

/**
 * ページ単位でデータを取得し、CSV文字列チャンクを配列で返す。
 * 全行を一度にメモリに保持せず、ページ単位で処理する。
 */
async function exportToCsvChunks(
  supabaseCore: ReturnType<typeof createAdminClient>,
  cutoffDate: string,
): Promise<{ chunks: string[]; totalRows: number; header: string }> {
  const chunks: string[] = [];
  let totalRows = 0;
  let header = '';

  for (let page = 0; ; page++) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;

    // 主キー (trade_date, local_code, session) で安定ソート
    const { data, error } = await supabaseCore
      .from(TABLE)
      .select('*')
      .lte('trade_date', cutoffDate)
      .order('trade_date', { ascending: true })
      .order('local_code', { ascending: true })
      .order('session', { ascending: true })
      .range(from, to);

    if (error) {
      throw new Error(`Batch select failed at page ${page}: ${error.message}`);
    }

    if (!data || data.length === 0) break;

    // Build header from first page
    if (page === 0) {
      header = Object.keys(data[0]).join(',');
    }

    const csvLines = data.map(toCsvRow).join('\n');
    chunks.push(csvLines);
    totalRows += data.length;

    logger.info(`Page ${page + 1}: ${data.length} rows (total: ${totalRows})`);

    if (data.length < PAGE_SIZE) break;
  }

  return { chunks, totalRows, header };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const { thresholdMb } = parseArgs();
  validateEnv();

  const supabaseCore = createAdminClient('jquants_core');
  const supabaseIngest = createAdminClient('jquants_ingest');

  // 1. DB容量チェック
  logger.info('Checking DB size...');
  const dbSizeMb = await getDbSizeMb();
  logger.info(`DB size: ${dbSizeMb.toFixed(1)} MB (threshold: ${thresholdMb} MB)`);

  if (dbSizeMb < thresholdMb) {
    logger.info('No archival needed - DB size is below threshold');
    console.log(JSON.stringify({
      success: true,
      action: 'skipped',
      dbSizeMb: Math.round(dbSizeMb),
      thresholdMb,
    }, null, 2));
    return;
  }

  // Start job run
  const runId = crypto.randomUUID();
  const startTime = Date.now();

  const { error: insertError } = await supabaseIngest
    .from('job_runs')
    .insert({
      run_id: runId,
      job_name: JOB_NAME,
      status: 'running',
      meta: { threshold_mb: thresholdMb, db_size_mb_before: Math.round(dbSizeMb) },
    });

  if (insertError) {
    throw new Error(`Failed to start job run: ${insertError.message}`);
  }

  try {
    // 2. データ範囲取得
    logger.info('Getting data range...');
    const range = await getDataRange();

    if (!range) {
      logger.info('Table is empty - skipping archival');
      await supabaseIngest
        .from('job_runs')
        .update({
          status: 'success',
          finished_at: new Date().toISOString(),
          meta: { action: 'skipped_empty_table' },
        })
        .eq('run_id', runId);

      console.log(JSON.stringify({
        success: true,
        action: 'skipped_empty_table',
      }, null, 2));
      return;
    }

    logger.info('Data range', {
      minDate: range.minDate,
      maxDate: range.maxDate,
      tradingDayCount: range.tradingDayCount,
    });

    // 3. アーカイブ範囲の決定
    const archiveDays = Math.min(ARCHIVE_TRADING_DAYS, range.tradingDayCount - MIN_REMAINING_TRADING_DAYS);

    if (archiveDays <= 0) {
      const msg = `Cannot archive: only ${range.tradingDayCount} trading days in DB, need ${MIN_REMAINING_TRADING_DAYS} minimum remaining`;
      logger.warn(msg);

      await supabaseIngest
        .from('job_runs')
        .update({
          status: 'success',
          finished_at: new Date().toISOString(),
          meta: {
            threshold_mb: thresholdMb,
            db_size_mb_before: Math.round(dbSizeMb),
            action: 'skipped_insufficient_data',
            trading_days: range.tradingDayCount,
          },
        })
        .eq('run_id', runId);

      console.log(JSON.stringify({
        success: true,
        action: 'skipped_insufficient_data',
        tradingDayCount: range.tradingDayCount,
        minRequired: MIN_REMAINING_TRADING_DAYS,
      }, null, 2));
      return;
    }

    const cutoffDate = await getArchiveCutoffDate(archiveDays);
    const remainingDays = range.tradingDayCount - archiveDays;

    logger.info('Archive plan', {
      archiveRange: `${range.minDate} to ${cutoffDate}`,
      archiveDays,
      remainingDays,
    });

    // 安全装置: 残り営業日数チェック
    if (remainingDays < MIN_REMAINING_TRADING_DAYS) {
      throw new Error(
        `Safety check failed: remaining ${remainingDays} trading days < ${MIN_REMAINING_TRADING_DAYS} minimum`
      );
    }

    // 4. データエクスポート（ページ単位で取得）
    logger.info('Exporting data...');
    const { chunks, totalRows, header } = await exportToCsvChunks(supabaseCore, cutoffDate);

    logger.info(`Exported ${totalRows} rows in ${chunks.length} pages`);

    if (totalRows === 0) {
      throw new Error('No rows exported - aborting');
    }

    // 5. CSV結合→gzip→Storageアップロード
    logger.info('Compressing and uploading...');
    const csv = header + '\n' + chunks.join('\n') + '\n';
    const gzipped = gzipSync(Buffer.from(csv, 'utf-8'));
    const fileName = `${TABLE}/${range.minDate}_to_${cutoffDate}.csv.gz`;

    logger.info(`Compressed: ${csv.length} bytes -> ${gzipped.length} bytes`);

    // Use public schema client for storage
    const supabasePublic = createAdminClient('public');

    const { error: uploadError } = await supabasePublic
      .storage
      .from(STORAGE_BUCKET)
      .upload(fileName, gzipped, {
        contentType: 'application/gzip',
        upsert: true,
      });

    if (uploadError) {
      throw new Error(`Storage upload failed: ${uploadError.message}`);
    }

    logger.info(`Uploaded to ${STORAGE_BUCKET}/${fileName}`);

    // 6. DELETE前のCOUNT検証
    logger.info('Verifying row count before delete...');
    const countResult = await executeSql(
      `SELECT COUNT(*) AS cnt FROM jquants_core.${TABLE} WHERE trade_date <= '${cutoffDate}'`
    );
    const dbRowCount = Number(countResult.rows[0]?.cnt ?? 0);

    if (dbRowCount !== totalRows) {
      throw new Error(
        `Row count mismatch: exported ${totalRows} rows but DB has ${dbRowCount} rows for trade_date <= '${cutoffDate}'. Aborting delete.`
      );
    }

    logger.info(`Row count verified: ${dbRowCount} rows match export`);

    // 7. DBから削除
    logger.info('Deleting archived data from DB...');
    const deleteResult = await executeSql(
      `DELETE FROM jquants_core.${TABLE} WHERE trade_date <= '${cutoffDate}'`
    );
    logger.info('Delete completed', deleteResult);

    // VACUUM FULL で実サイズを回収
    logger.info('Running VACUUM FULL (this may take a while)...');
    await executeSql(`VACUUM FULL jquants_core.${TABLE}`);
    logger.info('VACUUM FULL completed');

    // 8. 結果検証
    const dbSizeAfter = await getDbSizeMb();
    const durationMs = Date.now() - startTime;

    const result = {
      success: true,
      action: 'archived',
      runId,
      archiveRange: `${range.minDate} to ${cutoffDate}`,
      rowsArchived: totalRows,
      archiveDays,
      remainingDays,
      storagePath: `${STORAGE_BUCKET}/${fileName}`,
      compressedSizeBytes: gzipped.length,
      dbSizeMbBefore: Math.round(dbSizeMb),
      dbSizeMbAfter: Math.round(dbSizeAfter),
      savedMb: Math.round(dbSizeMb - dbSizeAfter),
      durationMs,
    };

    // job_runs 更新
    await supabaseIngest
      .from('job_runs')
      .update({
        status: 'success',
        finished_at: new Date().toISOString(),
        meta: result,
      })
      .eq('run_id', runId);

    logger.info('Archival completed', result);
    console.log(JSON.stringify(result, null, 2));

    // メール通知（成功） - 通知失敗はアーカイブ結果に影響させない
    try {
      await sendJobSuccessEmail({
        jobName: JOB_NAME,
        runId,
        rowCount: totalRows,
        durationMs,
        timestamp: new Date(),
      });
    } catch (emailError) {
      logger.warn('Failed to send success notification email', {
        error: emailError instanceof Error ? emailError.message : String(emailError),
      });
    }

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error('Archival failed', { runId, error: errorMessage });

    // job_runs を失敗として更新
    await supabaseIngest
      .from('job_runs')
      .update({
        status: 'failed',
        finished_at: new Date().toISOString(),
        error_message: errorMessage,
      })
      .eq('run_id', runId)
      .then(() => {}, () => {});

    // メール通知（失敗）
    await sendJobFailureEmail({
      jobName: JOB_NAME,
      runId,
      error: errorMessage,
      timestamp: new Date(),
    });

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
