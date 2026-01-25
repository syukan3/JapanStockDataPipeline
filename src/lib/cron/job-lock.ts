/**
 * ジョブロック（同時実行防止）
 *
 * @description テーブルベースロックで二重起動を防止
 * Supabase Pooler (Transaction mode) では Advisory Lock が使用できないため
 */

import type { SupabaseClient } from '@supabase/supabase-js';
import { createLogger } from '../utils/logger';

const logger = createLogger({ module: 'job-lock' });

export interface LockResult {
  /** ロック取得成功 */
  success: boolean;
  /** ロックトークン（解放時に必要） */
  token?: string;
  /** エラーメッセージ */
  error?: string;
}

/**
 * ジョブロックを取得
 *
 * @param supabase Supabase クライアント（jquants_ingest スキーマ）
 * @param jobName ジョブ名 (cron_a, cron_b, cron_c)
 * @param ttlSeconds ロックの有効期限（秒、デフォルト: 600）
 * @returns ロック結果
 *
 * @example
 * ```typescript
 * const lock = await acquireLock(supabaseIngest, 'cron_a', 600);
 * if (!lock.success) {
 *   return Response.json({ error: 'Another job is running' }, { status: 409 });
 * }
 * try {
 *   // 処理...
 * } finally {
 *   await releaseLock(supabaseIngest, 'cron_a', lock.token!);
 * }
 * ```
 */
export async function acquireLock(
  supabase: SupabaseClient,
  jobName: string,
  ttlSeconds: number = 600
): Promise<LockResult> {
  const token = crypto.randomUUID();
  const lockedUntil = new Date(Date.now() + ttlSeconds * 1000);

  logger.debug('Attempting to acquire lock', { jobName, ttlSeconds });

  try {
    // 1. 既存のロックをチェック
    const { data: existingLock, error: selectError } = await supabase
      .from('job_locks')
      .select('locked_until, lock_token')
      .eq('job_name', jobName)
      .single();

    if (selectError && selectError.code !== 'PGRST116') {
      // PGRST116 = row not found (これはOK)
      logger.error('Failed to check existing lock', { jobName, error: selectError });
      return { success: false, error: selectError.message };
    }

    const now = new Date();

    if (existingLock) {
      // ロックが存在し、まだ有効な場合
      if (new Date(existingLock.locked_until) > now) {
        logger.info('Lock already held', {
          jobName,
          lockedUntil: existingLock.locked_until,
        });
        return { success: false, error: 'Lock already held by another process' };
      }

      // ロックが期限切れの場合、更新
      const { error: updateError } = await supabase
        .from('job_locks')
        .update({
          locked_until: lockedUntil.toISOString(),
          lock_token: token,
          updated_at: now.toISOString(),
        })
        .eq('job_name', jobName)
        .eq('lock_token', existingLock.lock_token); // 楽観的ロック

      if (updateError) {
        logger.warn('Failed to update expired lock (race condition)', { jobName, error: updateError });
        return { success: false, error: 'Failed to acquire lock (race condition)' };
      }

      logger.info('Lock acquired (updated expired)', { jobName, token });
      return { success: true, token };
    }

    // 2. 新規ロックを作成
    const { error: insertError } = await supabase
      .from('job_locks')
      .insert({
        job_name: jobName,
        locked_until: lockedUntil.toISOString(),
        lock_token: token,
        updated_at: now.toISOString(),
      });

    if (insertError) {
      // 同時に insert された可能性（一意制約違反）
      if (insertError.code === '23505') {
        logger.info('Lock already created by another process', { jobName });
        return { success: false, error: 'Lock already held by another process' };
      }
      logger.error('Failed to create lock', { jobName, error: insertError });
      return { success: false, error: insertError.message };
    }

    logger.info('Lock acquired (created new)', { jobName, token });
    return { success: true, token };
  } catch (error) {
    logger.error('Unexpected error acquiring lock', { jobName, error });
    return { success: false, error: String(error) };
  }
}

/**
 * ジョブロックを解放
 *
 * @param supabase Supabase クライアント（jquants_ingest スキーマ）
 * @param jobName ジョブ名
 * @param token ロックトークン
 */
export async function releaseLock(
  supabase: SupabaseClient,
  jobName: string,
  token: string
): Promise<void> {
  logger.debug('Releasing lock', { jobName, token });

  const { error } = await supabase
    .from('job_locks')
    .delete()
    .eq('job_name', jobName)
    .eq('lock_token', token);

  if (error) {
    logger.warn('Failed to release lock', { jobName, token, error });
    // ロック解放失敗は警告のみ（TTL で自動解放される）
    return;
  }

  logger.info('Lock released', { jobName, token });
}

/**
 * ロックを延長（長時間処理用）
 *
 * @param supabase Supabase クライアント
 * @param jobName ジョブ名
 * @param token ロックトークン
 * @param ttlSeconds 新しい有効期限（秒）
 */
export async function extendLock(
  supabase: SupabaseClient,
  jobName: string,
  token: string,
  ttlSeconds: number = 600
): Promise<boolean> {
  const lockedUntil = new Date(Date.now() + ttlSeconds * 1000);

  const { error } = await supabase
    .from('job_locks')
    .update({
      locked_until: lockedUntil.toISOString(),
      updated_at: new Date().toISOString(),
    })
    .eq('job_name', jobName)
    .eq('lock_token', token);

  if (error) {
    logger.warn('Failed to extend lock', { jobName, token, error });
    return false;
  }

  logger.debug('Lock extended', { jobName, token, lockedUntil: lockedUntil.toISOString() });
  return true;
}

/**
 * 期限切れロックをクリーンアップ（定期メンテナンス用）
 */
export async function cleanupExpiredLocks(supabase: SupabaseClient): Promise<number> {
  const { data, error } = await supabase
    .from('job_locks')
    .delete()
    .lt('locked_until', new Date().toISOString())
    .select('job_name');

  if (error) {
    logger.error('Failed to cleanup expired locks', { error });
    return 0;
  }

  const count = data?.length ?? 0;
  if (count > 0) {
    logger.info('Cleaned up expired locks', { count });
  }

  return count;
}
