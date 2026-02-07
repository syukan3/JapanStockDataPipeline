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

  if (error) {
    // 冪等性: 同一 job_name + target_date で既に実行済みの場合
    if (error.code === '23505') {
      logger.info('Job run already exists for this target date', { jobName, targetDate });
      return { runId: '', error: 'Job already executed for this target date' };
    }

    logger.error('Failed to start job run', { jobName, targetDate, error });
    return { runId: '', error: error.message };
  }

  logger.info('Job run started', { jobName, targetDate, runId: data.run_id });
  return { runId: data.run_id };
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
