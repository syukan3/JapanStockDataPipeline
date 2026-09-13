/**
 * ジョブ実行ログ管理
 *
 * @description job_runs, job_run_items テーブルへのCRUD操作
 */

import { randomUUID } from 'node:crypto';
import type { SupabaseClient } from '@supabase/supabase-js';
import { createLogger } from '../utils/logger';
import { withPostgrestRetry } from '../utils/retry';

const logger = createLogger({ module: 'job-run' });

export type JobStatus = 'running' | 'success' | 'failed';
export type JobName = 'cron_a' | 'cron_b' | 'cron_c' | 'cron-d-macro' | 'cron-e-yutai' | 'weekly-margin' | 'scouter-yutai-cross' | 'db-archival';

export interface JobRunRecord {
  run_id: string;
  attempt_id: string;
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

interface StartJobRunBaseOptions {
  /** 対象日付（optional） */
  targetDate?: string;
  /** メタデータ */
  meta?: Record<string, unknown>;
  /** 指定秒数より古い running run を再取得する */
  reclaimStaleAfterSeconds?: number;
  /** 指定秒数より古い success run を再観測のため再取得する */
  reclaimSuccessAfterSeconds?: number;
}

/**
 * running 行が1件も無いか（＝直前の claim はコミットしていないと言い切れるか）。
 *
 * 取りこぼした claim が実はコミット済みだと、投げ直しても `already_executed` が返って
 * ジョブが「何もせず正常終了」してしまう。running 行があるとき、および判定自体が
 * できなかったときはフェイルクローズし、従来どおり失敗を見せる。
 */
async function noRunningJobRun(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  supabase: SupabaseClient<any, any, any>,
  jobName: JobName,
  targetDate: string
): Promise<boolean> {
  const { data, error } = await supabase
    .from('job_runs')
    .select('status')
    .eq('job_name', jobName)
    .eq('target_date', targetDate)
    .maybeSingle();
  if (error) return false;
  if (!data) return true;
  return data.status !== 'running';
}

/**
 * この attempt がまだ running のままか（＝直前の完了要求はコミットしていないか）。
 *
 * 判定できないときは false を返してフェイルクローズする。
 */
async function attemptStillRunning(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  supabase: SupabaseClient<any, any, any>,
  runId: string,
  attemptId: string
): Promise<boolean> {
  const { data, error } = await supabase
    .from('job_runs')
    .select('status, attempt_id')
    .eq('run_id', runId)
    .maybeSingle();
  if (error || !data) return false;
  return data.status === 'running' && data.attempt_id === attemptId;
}

export type StartJobRunOptions =
  | (StartJobRunBaseOptions & {
      jobName: 'cron_b';
      /** claimと同時にfailedへfenceするcoverage dataset */
      coverageDataset?: 'earnings_calendar';
    })
  | (StartJobRunBaseOptions & {
      jobName: Exclude<JobName, 'cron_b'>;
      coverageDataset?: never;
    });

export interface StartJobRunResult {
  /** 実行ID */
  runId: string;
  /** reclaimごとに更新されるfencing token */
  attemptId?: string;
  /** エラー */
  error?: string;
  /** エラーがDB障害ではなく実行済みを表すか */
  alreadyExecuted?: boolean;
}

export type CompleteJobRunResult =
  | { completed: true }
  | {
      completed: false;
      reason: 'superseded' | 'db_error';
      error?: string;
    };

/**
 * ジョブ実行を開始（job_runs に INSERT）
 */
export async function startJobRun(
  supabase: SupabaseClient,
  options: StartJobRunOptions
): Promise<StartJobRunResult> {
  const {
    jobName,
    targetDate,
    meta = {},
    reclaimStaleAfterSeconds,
    reclaimSuccessAfterSeconds,
    coverageDataset,
  } = options;

  logger.debug('Starting job run', { jobName, targetDate });

  // stale running / success の再取得は、状態判定とattempt_id更新を
  // DBの1トランザクションで行う。旧attemptは以降の確定書込みを拒否される。
  if (
    reclaimStaleAfterSeconds !== undefined ||
    reclaimSuccessAfterSeconds !== undefined ||
    coverageDataset !== undefined
  ) {
    if (!targetDate) {
      return {
        runId: '',
        error: 'targetDate is required for atomic job claim',
      };
    }

    const { data: claimData, error: claimError } = await withPostgrestRetry(
      () =>
        supabase.rpc('claim_job_run', {
          p_job_name: jobName,
          p_target_date: targetDate,
          p_meta: meta,
          p_running_stale_after_seconds: reclaimStaleAfterSeconds ?? null,
          p_success_stale_after_seconds: reclaimSuccessAfterSeconds ?? null,
          p_coverage_dataset: coverageDataset ?? null,
        }),
      {
        maxRetries: 2,
        isRetrySafe: () => noRunningJobRun(supabase, jobName, targetDate),
        onRetry: (attempt, error, delayMs) =>
          logger.warn('Retrying job claim', { jobName, targetDate, attempt, delayMs, error: error.message }),
      }
    );

    if (claimError) {
      logger.error('Failed to atomically claim job run', {
        jobName,
        targetDate,
        error: claimError,
      });
      return {
        runId: '',
        error: claimError.message ?? 'Failed to atomically claim job run',
      };
    }

    type ClaimRow = {
      run_id: string;
      attempt_id: string | null;
      claimed: boolean;
      reason: string;
    };
    const claim = (Array.isArray(claimData) ? claimData[0] : claimData) as
      | ClaimRow
      | null;

    if (!claim) {
      return { runId: '', error: 'Atomic job claim returned no result' };
    }
    if (!claim.claimed) {
      logger.info('Job run already exists and is not claimable; skipping re-run', {
        jobName,
        targetDate,
        reason: claim.reason,
      });
      return {
        runId: '',
        error: 'Job already executed for this target date',
        alreadyExecuted: true,
      };
    }
    if (!claim.run_id || !claim.attempt_id) {
      return { runId: '', error: 'Atomic job claim returned an invalid result' };
    }

    logger.info('Job run atomically claimed', {
      jobName,
      targetDate,
      runId: claim.run_id,
      reason: claim.reason,
    });
    return { runId: claim.run_id, attemptId: claim.attempt_id };
  }

  // run_id をこちらで採番し、同じ id の再送は ignoreDuplicates で吸収させる。
  // こうしないと、取りこぼした1回目がコミット済みだったときに
  // （target_date が null の Cron A/C では部分ユニーク制約も効かず）running 行が2本できる。
  const newRunId = randomUUID();
  const { error } = await withPostgrestRetry(
    () =>
      supabase.from('job_runs').upsert(
        {
          run_id: newRunId,
          job_name: jobName,
          target_date: targetDate ?? null,
          status: 'running',
          meta,
        },
        { onConflict: 'run_id', ignoreDuplicates: true }
      ),
    { maxRetries: 2 }
  );

  if (!error) {
    logger.info('Job run started', { jobName, targetDate, runId: newRunId });
    return { runId: newRunId };
  }

  // 冪等性: 同一 job_name + target_date の行が既存（uq_job_runs_job_target は target_date が
  // 非null のときのみ有効な部分ユニークインデックス）。既定では failed の前回 run を再取得する。
  // 期限切れの running / success は上のatomic claim経路だけで再取得する。
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
      // PGRST116 = 該当行なし（既存が success または有効な running）
      if (reclaimError?.code === 'PGRST116') {
        logger.info('Job run already exists and is not failed; skipping re-run', { jobName, targetDate });
        return {
          runId: '',
          error: 'Job already executed for this target date',
          alreadyExecuted: true,
        };
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
  errorMessage?: string,
  attemptId?: string,
  heartbeatMeta?: Record<string, unknown>
): Promise<CompleteJobRunResult> {
  logger.debug('Completing job run', { runId, status });

  const normalizedError = errorMessage
    ? errorMessage.length > 10000
      ? errorMessage.slice(0, 10000) + '... (truncated)'
      : errorMessage
    : undefined;

  if (attemptId) {
    // 取りこぼした1回目が実は通っていた場合、投げ直すと status が running でなくなっていて
    // false が返り「他の attempt に追い越された」と誤認する。まだ running のときだけ投げ直す。
    const { data, error } = await withPostgrestRetry(
      () =>
        supabase.rpc('complete_job_run_attempt', {
          p_run_id: runId,
          p_attempt_id: attemptId,
          p_status: status,
          p_error_message: normalizedError ?? null,
          p_heartbeat_meta: heartbeatMeta ?? {},
        }),
      {
        maxRetries: 2,
        isRetrySafe: () => attemptStillRunning(supabase, runId, attemptId),
      }
    );

    if (error) {
      logger.error('Failed to complete fenced job run', {
        runId,
        attemptId,
        status,
        error,
      });
      return {
        completed: false,
        reason: 'db_error',
        error: error.message ?? 'Failed to complete fenced job run',
      };
    }
    if (data !== true) {
      logger.warn('Job completion rejected for superseded attempt', {
        runId,
        attemptId,
        status,
      });
      return { completed: false, reason: 'superseded' };
    }

    logger.info('Fenced job run completed', { runId, attemptId, status });
    return { completed: true };
  }

  const updateData: Partial<JobRunRecord> = {
    status,
    finished_at: new Date().toISOString(),
  };

  if (normalizedError) {
    updateData.error_message = normalizedError;
  }

  const { error } = await supabase
    .from('job_runs')
    .update(updateData)
    .eq('run_id', runId);

  if (error) {
    logger.error('Failed to complete job run', { runId, status, error });
    return {
      completed: false,
      reason: 'db_error',
      error: error.message ?? 'Failed to complete job run',
    };
  }

  logger.info('Job run completed', { runId, status });
  return { completed: true };
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

  // PK(run_id, dataset) があるので、二重に届いても2回目は 23505 になるだけ
  const { error } = await withPostgrestRetry(
    () =>
      supabase.from('job_run_items').insert({
        run_id: runId,
        dataset,
        status: 'running',
        meta: meta ?? {},
      }),
    { maxRetries: 2 }
  );

  if (error) {
    // 23505 = 同一 (run_id, dataset) が既にある。瞬断で投げ直したときに起こりうるが、
    // 目的の行は存在しているので失敗ではない
    if (error.code === '23505') {
      logger.debug('Job run item already exists', { runId, dataset });
    } else {
      logger.error('Failed to start job run item', { runId, dataset, error });
    }
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
