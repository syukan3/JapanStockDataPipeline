/**
 * 死活監視（ハートビート）
 *
 * @description job_heartbeat テーブルを更新して監視可能な状態を維持
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import { createLogger } from '../utils/logger';
import type { JobName, JobStatus } from './job-run';

const logger = createLogger({ module: 'heartbeat' });

export interface HeartbeatRecord {
  job_name: JobName;
  last_seen_at: string;
  last_status: JobStatus;
  last_run_id: string | null;
  last_target_date: string | null;
  last_error: string | null;
  meta: Record<string, unknown>;
}

export interface UpdateHeartbeatOptions {
  /** ジョブ名 */
  jobName: JobName;
  /** ステータス */
  status: JobStatus;
  /** 実行ID */
  runId?: string;
  /** 対象日付 */
  targetDate?: string;
  /** エラーメッセージ */
  error?: string;
  /** メタデータ */
  meta?: Record<string, unknown>;
}

/**
 * ハートビートを更新（UPSERT）
 *
 * @param supabase Supabase クライアント（jquants_ingest スキーマ）
 * @param options 更新オプション
 */
export async function updateHeartbeat(
  supabase: SupabaseClient,
  options: UpdateHeartbeatOptions
): Promise<void> {
  const { jobName, status, runId, targetDate, error, meta = {} } = options;

  logger.debug('Updating heartbeat', { jobName, status });

  const record: Partial<HeartbeatRecord> = {
    job_name: jobName,
    last_seen_at: new Date().toISOString(),
    last_status: status,
    last_run_id: runId ?? null,
    last_target_date: targetDate ?? null,
    last_error: error ? (error.length > 1000 ? error.slice(0, 1000) + '...' : error) : null,
    meta,
  };

  const { error: upsertError } = await supabase
    .from('job_heartbeat')
    .upsert(record, {
      onConflict: 'job_name',
    });

  if (upsertError) {
    logger.error('Failed to update heartbeat', { jobName, error: upsertError });
    return;
  }

  logger.debug('Heartbeat updated', { jobName, status });
}

/**
 * 全ジョブのハートビート状態を取得
 */
export async function getAllHeartbeats(
  supabase: SupabaseClient
): Promise<HeartbeatRecord[]> {
  const { data, error } = await supabase
    .from('job_heartbeat')
    .select('*')
    .order('job_name');

  if (error) {
    logger.error('Failed to get all heartbeats', { error });
    return [];
  }

  return data ?? [];
}

/**
 * 特定ジョブのハートビート状態を取得
 */
export async function getHeartbeat(
  supabase: SupabaseClient,
  jobName: JobName
): Promise<HeartbeatRecord | null> {
  const { data, error } = await supabase
    .from('job_heartbeat')
    .select('*')
    .eq('job_name', jobName)
    .single();

  if (error) {
    if (error.code === 'PGRST116') {
      // Row not found
      return null;
    }
    logger.error('Failed to get heartbeat', { jobName, error });
    return null;
  }

  return data;
}

/**
 * ジョブが正常に動作しているかチェック
 *
 * @param heartbeat ハートビートレコード
 * @param staleThresholdHours 古いとみなす閾値（時間、デフォルト: 25）
 */
export function isJobHealthy(
  heartbeat: HeartbeatRecord | null,
  staleThresholdHours: number = 25
): { healthy: boolean; reason?: string } {
  if (!heartbeat) {
    return { healthy: false, reason: 'No heartbeat record found' };
  }

  const lastSeenAt = new Date(heartbeat.last_seen_at);
  const now = new Date();
  const hoursSinceLastSeen = (now.getTime() - lastSeenAt.getTime()) / (1000 * 60 * 60);

  if (hoursSinceLastSeen > staleThresholdHours) {
    return {
      healthy: false,
      reason: `Stale: last seen ${Math.floor(hoursSinceLastSeen)} hours ago`,
    };
  }

  if (heartbeat.last_status === 'failed') {
    return {
      healthy: false,
      reason: `Last run failed: ${heartbeat.last_error ?? 'Unknown error'}`,
    };
  }

  return { healthy: true };
}

/**
 * 全ジョブの健全性をチェック
 */
export async function checkAllJobsHealth(
  supabase: SupabaseClient,
  staleThresholdHours: number = 25
): Promise<{
  healthy: boolean;
  jobs: Array<{
    jobName: JobName;
    healthy: boolean;
    reason?: string;
    lastSeenAt?: string;
    lastStatus?: JobStatus;
  }>;
}> {
  const heartbeats = await getAllHeartbeats(supabase);

  const jobNames: JobName[] = ['cron_a', 'cron_b', 'cron_c'];
  const results: Array<{
    jobName: JobName;
    healthy: boolean;
    reason?: string;
    lastSeenAt?: string;
    lastStatus?: JobStatus;
  }> = [];

  let allHealthy = true;

  for (const jobName of jobNames) {
    const heartbeat = heartbeats.find((h) => h.job_name === jobName) ?? null;
    const health = isJobHealthy(heartbeat, staleThresholdHours);

    if (!health.healthy) {
      allHealthy = false;
    }

    results.push({
      jobName,
      healthy: health.healthy,
      reason: health.reason,
      lastSeenAt: heartbeat?.last_seen_at,
      lastStatus: heartbeat?.last_status,
    });
  }

  return {
    healthy: allHealthy,
    jobs: results,
  };
}
