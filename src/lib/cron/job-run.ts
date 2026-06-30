/**
 * ジョブ実行ログ管理
 *
 * @description job_runs, job_run_items テーブルへのCRUD操作
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import { createLogger } from '../utils/logger';

const logger = createLogger({ module: 'job-run' });

export type JobStatus = 'running' | 'success' | 'failed';
export type JobName = 'cron_a' | 'cron_b' | 'cron_c' | 'cron-d-macro' | 'cron-e-yutai' | 'scouter-yutai-cross' | 'db-archival';

export interface JobRunRecord {
  run_id: string;
  job_name: JobName;
  target_date: string | null;
  status: JobStatus;
  started_at: string;
  finished_at: string | null;
  error_message: string | null;
  meta: Record<string, unknown>;
}

export interface JobRunItemRecord {
  run_id: string;
  dataset: string;
  status: JobStatus;
  row_count: number | null;
  page_count: number | null;
  started_at: string;
  finished_at: string | null;
  error_message: string | null;
  meta: Record<string, unknown>;
}

export interface StartJobRunOptions {
  /** ジョブ名 */
  jobName: JobName;
  /** 対象日付（optional） */
  targetDate?: string;
  /** メタデータ */
  meta?: Record<string, unknown>;
}

export interface StartJobRunResult {
  /** 実行ID */
  runId: string;
  /** エラー */
  error?: string;
}

/**
 * ジョブ実行を開始（job_runs に INSERT）
 */
export async function startJobRun(
  supabase: SupabaseClient,
  options: StartJobRunOptions
): Promise<StartJobRunResult> {
  const { jobName, targetDate, meta = {} } = options;

  logger.debug('Starting job run', { jobName, targetDate });

  const { data, error } = await supabase
    .from('job_runs')
    .insert({
      job_name: jobName,
      target_date: targetDate ?? null,
      status: 'running',
      meta,
    })
    .select('run_id')
    .single();

  if (!error) {
    logger.info('Job run started', { jobName, targetDate, runId: data.run_id });
    return { runId: data.run_id };
  }

  // 冪等性: 同一 job_name + target_date の行が既存（uq_job_runs_job_target は target_date が
  // 非null のときのみ有効な部分ユニークインデックス）。**失敗(failed)した前回 run のみ**を
  // 再取得して running に戻すことで、watchdog による再ディスパッチが既存行を success へ更新でき、
  // 収束する。既存が success / running の場合は再取得せず「実行済み」として扱い、成功監査ログを
  // 巻き戻さない（status='failed' 条件で update 0 件 → PGRST116）。
  // 部分インデックスのため supabase-js の upsert(onConflict) は使えず、insert→update で実現する。
  if (error.code === '23505' && targetDate) {
    const { data: reclaimed, error: reclaimError } = await supabase
      .from('job_runs')
      .update({
        status: 'running',
        started_at: new Date().toISOString(),
        finished_at: null,
        error_message: null,
        meta,
      })
      .eq('job_name', jobName)
      .eq('target_date', targetDate)
      .eq('status', 'failed')
      .select('run_id')
      .single();

    if (reclaimError || !reclaimed) {
      // PGRST116 = 該当行なし（既存が success または running）→ 実行済みとして再実行しない
      if (reclaimError?.code === 'PGRST116') {
        logger.info('Job run already exists and is not failed; skipping re-run', { jobName, targetDate });
        return { runId: '', error: 'Job already executed for this target date' };
      }
      logger.error('Failed to reclaim existing job run', { jobName, targetDate, error: reclaimError });
      return { runId: '', error: reclaimError?.message ?? 'Failed to reclaim existing job run' };
    }

    // 再実行に伴い前回の job_run_items をクリア（古い dataset 状態を残さない）
    const { error: itemsError } = await supabase
      .from('job_run_items')
      .delete()
      .eq('run_id', reclaimed.run_id);
    if (itemsError) {
      logger.warn('Failed to clear stale job_run_items on reclaim', { runId: reclaimed.run_id, error: itemsError });
    }

    logger.info('Job run reclaimed for re-run', { jobName, targetDate, runId: reclaimed.run_id });
    return { runId: reclaimed.run_id };
  }

  logger.error('Failed to start job run', { jobName, targetDate, error });
  return { runId: '', error: error.message };
}

/**
 * ジョブ実行を完了
 */
export async function completeJobRun(
  supabase: SupabaseClient,
  runId: string,
  status: 'success' | 'failed',
  errorMessage?: string
): Promise<void> {
  logger.debug('Completing job run', { runId, status });

  const updateData: Partial<JobRunRecord> = {
    status,
    finished_at: new Date().toISOString(),
  };

  if (errorMessage) {
    // エラーメッセージが長すぎる場合は切り詰める（DB制限考慮）
    updateData.error_message = errorMessage.length > 10000
      ? errorMessage.slice(0, 10000) + '... (truncated)'
      : errorMessage;
  }

  const { error } = await supabase
    .from('job_runs')
    .update(updateData)
    .eq('run_id', runId);

  if (error) {
    logger.error('Failed to complete job run', { runId, status, error });
    return;
  }

  logger.info('Job run completed', { runId, status });
}

/**
 * データセット処理を開始（job_run_items に INSERT）
 */
export async function startJobRunItem(
  supabase: SupabaseClient,
  runId: string,
  dataset: string,
  meta?: Record<string, unknown>
): Promise<void> {
  logger.debug('Starting job run item', { runId, dataset });

  const { error } = await supabase
    .from('job_run_items')
    .insert({
      run_id: runId,
      dataset,
      status: 'running',
      meta: meta ?? {},
    });

  if (error) {
    logger.error('Failed to start job run item', { runId, dataset, error });
  }
}

/**
 * データセット処理を完了
 */
export async function completeJobRunItem(
  supabase: SupabaseClient,
  runId: string,
  dataset: string,
  status: 'success' | 'failed',
  options?: {
    rowCount?: number;
    pageCount?: number;
    errorMessage?: string;
    meta?: Record<string, unknown>;
  }
): Promise<void> {
  logger.debug('Completing job run item', { runId, dataset, status });

  const updateData: Partial<JobRunItemRecord> = {
    status,
    finished_at: new Date().toISOString(),
    row_count: options?.rowCount ?? null,
    page_count: options?.pageCount ?? null,
    error_message: options?.errorMessage ?? null,
  };

  if (options?.meta) {
    updateData.meta = options.meta;
  }

  const { error } = await supabase
    .from('job_run_items')
    .update(updateData)
    .eq('run_id', runId)
    .eq('dataset', dataset);

  if (error) {
    logger.error('Failed to complete job run item', { runId, dataset, status, error });
  }
}

/**
 * 最新のジョブ実行を取得
 */
export async function getLatestJobRun(
  supabase: SupabaseClient,
  jobName: JobName,
  status?: JobStatus
): Promise<JobRunRecord | null> {
  let query = supabase
    .from('job_runs')
    .select('*')
    .eq('job_name', jobName)
    .order('started_at', { ascending: false })
    .limit(1);

  if (status) {
    query = query.eq('status', status);
  }

  const { data, error } = await query.single();

  if (error) {
    if (error.code === 'PGRST116') {
      // Row not found
      return null;
    }
    logger.error('Failed to get latest job run', { jobName, status, error });
    return null;
  }

  return data;
}

/**
 * 特定日付のジョブ実行があるかチェック
 */
export async function hasJobRunForDate(
  supabase: SupabaseClient,
  jobName: JobName,
  targetDate: string,
  status?: JobStatus
): Promise<boolean> {
  let query = supabase
    .from('job_runs')
    .select('run_id')
    .eq('job_name', jobName)
    .eq('target_date', targetDate)
    .limit(1);

  if (status) {
    query = query.eq('status', status);
  }

  const { data, error } = await query;

  if (error) {
    logger.error('Failed to check job run existence', { jobName, targetDate, error });
    return false;
  }

  return data !== null && data.length > 0;
}

/**
 * 失敗したジョブ実行を取得（再実行候補）
 */
export async function getFailedJobRuns(
  supabase: SupabaseClient,
  jobName: JobName,
  limit: number = 10
): Promise<JobRunRecord[]> {
  const { data, error } = await supabase
    .from('job_runs')
    .select('*')
    .eq('job_name', jobName)
    .eq('status', 'failed')
    .order('started_at', { ascending: false })
    .limit(limit);

  if (error) {
    logger.error('Failed to get failed job runs', { jobName, error });
    return [];
  }

  return data ?? [];
}
