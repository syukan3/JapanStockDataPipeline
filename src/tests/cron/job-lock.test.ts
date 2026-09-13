/**
 * cron/job-lock.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  acquireLock,
  releaseLock,
  extendLock,
  cleanupExpiredLocks,
} from '@/lib/cron/job-lock';

describe('cron/job-lock.ts', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-15T10:00:00.000Z'));
    // crypto.randomUUID をモック
    vi.spyOn(crypto, 'randomUUID').mockReturnValue('mock-uuid-token');
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  describe('acquireLock', () => {
    it('新規ロックを取得できる', async () => {
      const insertMock = vi.fn().mockResolvedValue({ error: null });
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' }, // Row not found
          }),
          insert: insertMock,
        })),
      };

      const result = await acquireLock(mockSupabase as any, 'cron_a', 600);

      expect(result.success).toBe(true);
      expect(result.token).toBe('mock-uuid-token');
      expect(insertMock).toHaveBeenCalledWith(
        expect.objectContaining({
          job_name: 'cron_a',
          lock_token: 'mock-uuid-token',
        })
      );
    });

    it('既存の有効なロックがある場合は取得失敗', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: {
              locked_until: '2024-01-15T10:30:00.000Z', // まだ有効
              lock_token: 'existing-token',
            },
            error: null,
          }),
        })),
      };

      const result = await acquireLock(mockSupabase as any, 'cron_a');

      expect(result.success).toBe(false);
      expect(result.error).toBe('Lock already held by another process');
    });

    it('期限切れのロックを上書きできる', async () => {
      const updateMock = vi.fn().mockReturnThis();
      const eqMock = vi.fn();

      // 最初のselect用
      const selectEqMock = vi.fn().mockReturnThis();

      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: selectEqMock,
          single: vi.fn().mockResolvedValue({
            data: {
              locked_until: '2024-01-15T09:00:00.000Z', // 期限切れ
              lock_token: 'old-token',
            },
            error: null,
          }),
          update: updateMock,
        })),
      };

      // updateの後のeq呼び出し
      updateMock.mockReturnValue({
        eq: vi.fn().mockReturnValue({
          eq: vi.fn().mockResolvedValue({ error: null }),
        }),
      });

      const result = await acquireLock(mockSupabase as any, 'cron_a');

      expect(result.success).toBe(true);
      expect(result.token).toBe('mock-uuid-token');
    });

    it('同時にinsertされた場合（一意制約違反）は失敗', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' },
          }),
          insert: vi.fn().mockResolvedValue({
            error: { code: '23505', message: 'duplicate key' },
          }),
        })),
      };

      const result = await acquireLock(mockSupabase as any, 'cron_a');

      expect(result.success).toBe(false);
      expect(result.error).toBe('Lock already held by another process');
    });

    it('一過性エラーでも自分のtokenでロックが取れていれば成功扱い', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' },
          }),
          // 取りこぼした1回目が実はコミットしていたケース
          maybeSingle: vi.fn().mockResolvedValue({
            data: { lock_token: 'mock-uuid-token' },
            error: null,
          }),
          insert: vi.fn().mockResolvedValue({
            error: { message: 'Gateway Timeout' },
            status: 504,
          }),
        })),
      };

      const result = await acquireLock(mockSupabase as any, 'cron_a');

      expect(result.success).toBe(true);
      expect(result.token).toBe('mock-uuid-token');
    });

    it('一過性エラーで他プロセスのロックなら投げ直さずに失敗', async () => {
      const insertMock = vi.fn().mockResolvedValue({
        error: { message: 'Gateway Timeout' },
        status: 504,
      });
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' },
          }),
          maybeSingle: vi.fn().mockResolvedValue({
            data: { lock_token: 'someone-else' },
            error: null,
          }),
          insert: insertMock,
        })),
      };

      const result = await acquireLock(mockSupabase as any, 'cron_a');

      expect(result.success).toBe(false);
      expect(result.error).toBe('Gateway Timeout');
      // 他プロセスが握っているので投げ直さない
      expect(insertMock).toHaveBeenCalledTimes(1);
    });

    it('DBエラーの場合は失敗', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'P0001', message: 'Database error' },
          }),
        })),
      };

      const result = await acquireLock(mockSupabase as any, 'cron_a');

      expect(result.success).toBe(false);
      expect(result.error).toBe('Database error');
    });

    it('TTLを指定できる', async () => {
      const insertMock = vi.fn().mockResolvedValue({ error: null });
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' },
          }),
          insert: insertMock,
        })),
      };

      await acquireLock(mockSupabase as any, 'cron_a', 300); // 5分

      expect(insertMock).toHaveBeenCalledWith(
        expect.objectContaining({
          locked_until: '2024-01-15T10:05:00.000Z', // 5分後
        })
      );
    });

    it('期限切れロック更新時のレースコンディションで失敗', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: {
              locked_until: '2024-01-15T09:00:00.000Z', // 期限切れ
              lock_token: 'old-token',
            },
            error: null,
          }),
          update: vi.fn().mockReturnValue({
            eq: vi.fn().mockReturnValue({
              eq: vi.fn().mockResolvedValue({
                error: { message: 'No rows updated' },
              }),
            }),
          }),
        })),
      };

      const result = await acquireLock(mockSupabase as any, 'cron_a');

      expect(result.success).toBe(false);
      expect(result.error).toBe('Failed to acquire lock (race condition)');
    });
  });

  describe('releaseLock', () => {
    it('ロックを解放する', async () => {
      const deleteMock = vi.fn().mockReturnThis();
      const eqMock = vi.fn();
      eqMock.mockReturnValueOnce({ eq: eqMock });
      eqMock.mockResolvedValueOnce({ error: null });

      const mockSupabase = {
        from: vi.fn(() => ({
          delete: deleteMock,
          eq: eqMock,
        })),
      };

      await releaseLock(mockSupabase as any, 'cron_a', 'test-token');

      expect(deleteMock).toHaveBeenCalled();
      expect(eqMock).toHaveBeenCalledWith('job_name', 'cron_a');
      expect(eqMock).toHaveBeenCalledWith('lock_token', 'test-token');
    });

    it('解放失敗でも例外を投げない', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          delete: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnValue({
            eq: vi.fn().mockResolvedValue({
              error: { message: 'Delete failed' },
            }),
          }),
        })),
      };

      // 例外が投げられないことを確認
      await expect(
        releaseLock(mockSupabase as any, 'cron_a', 'test-token')
      ).resolves.toBeUndefined();
    });
  });

  describe('extendLock', () => {
    it('ロックを延長できる', async () => {
      const updateMock = vi.fn().mockReturnThis();
      const eqMock = vi.fn();
      eqMock.mockReturnValueOnce({ eq: eqMock });
      eqMock.mockResolvedValueOnce({ error: null });

      const mockSupabase = {
        from: vi.fn(() => ({
          update: updateMock,
          eq: eqMock,
        })),
      };

      const result = await extendLock(mockSupabase as any, 'cron_a', 'test-token', 300);

      expect(result).toBe(true);
      expect(updateMock).toHaveBeenCalledWith(
        expect.objectContaining({
          locked_until: '2024-01-15T10:05:00.000Z', // 5分後
        })
      );
    });

    it('延長失敗時はfalseを返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          update: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnValue({
            eq: vi.fn().mockResolvedValue({
              error: { message: 'Update failed' },
            }),
          }),
        })),
      };

      const result = await extendLock(mockSupabase as any, 'cron_a', 'test-token');

      expect(result).toBe(false);
    });

    it('デフォルトTTLは600秒', async () => {
      const updateMock = vi.fn().mockReturnThis();
      const eqMock = vi.fn();
      eqMock.mockReturnValueOnce({ eq: eqMock });
      eqMock.mockResolvedValueOnce({ error: null });

      const mockSupabase = {
        from: vi.fn(() => ({
          update: updateMock,
          eq: eqMock,
        })),
      };

      await extendLock(mockSupabase as any, 'cron_a', 'test-token');

      expect(updateMock).toHaveBeenCalledWith(
        expect.objectContaining({
          locked_until: '2024-01-15T10:10:00.000Z', // 10分後（600秒）
        })
      );
    });
  });

  describe('cleanupExpiredLocks', () => {
    it('期限切れロックを削除する', async () => {
      const deleteMock = vi.fn().mockReturnThis();
      const ltMock = vi.fn().mockReturnThis();
      const selectMock = vi.fn().mockResolvedValue({
        data: [{ job_name: 'cron_a' }, { job_name: 'cron_b' }],
        error: null,
      });

      const mockSupabase = {
        from: vi.fn(() => ({
          delete: deleteMock,
          lt: ltMock,
          select: selectMock,
        })),
      };

      const result = await cleanupExpiredLocks(mockSupabase as any);

      expect(result).toBe(2);
      expect(ltMock).toHaveBeenCalledWith('locked_until', '2024-01-15T10:00:00.000Z');
    });

    it('削除対象がない場合は0を返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          delete: vi.fn().mockReturnThis(),
          lt: vi.fn().mockReturnThis(),
          select: vi.fn().mockResolvedValue({
            data: [],
            error: null,
          }),
        })),
      };

      const result = await cleanupExpiredLocks(mockSupabase as any);

      expect(result).toBe(0);
    });

    it('DBエラーの場合は0を返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          delete: vi.fn().mockReturnThis(),
          lt: vi.fn().mockReturnThis(),
          select: vi.fn().mockResolvedValue({
            data: null,
            error: { message: 'Delete failed' },
          }),
        })),
      };

      const result = await cleanupExpiredLocks(mockSupabase as any);

      expect(result).toBe(0);
    });
  });
});
