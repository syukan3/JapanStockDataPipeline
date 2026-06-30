/**
 * cron/job-run.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  startJobRun,
  completeJobRun,
  startJobRunItem,
  completeJobRunItem,
  getLatestJobRun,
  hasJobRunForDate,
  getFailedJobRuns,
} from '@/lib/cron/job-run';

describe('cron/job-run.ts', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-15T10:00:00.000Z'));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('startJobRun', () => {
    it('ジョブ実行を開始してrun_idを返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          insert: vi.fn().mockReturnThis(),
          select: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: { run_id: 'test-run-id-123' },
            error: null,
          }),
        })),
      };

      const result = await startJobRun(mockSupabase as any, {
        jobName: 'cron_a',
        targetDate: '2024-01-15',
      });

      expect(result.runId).toBe('test-run-id-123');
      expect(result.error).toBeUndefined();
    });

    it('targetDateなしでも実行できる', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          insert: vi.fn().mockReturnThis(),
          select: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: { run_id: 'test-run-id-456' },
            error: null,
          }),
        })),
      };

      const result = await startJobRun(mockSupabase as any, {
        jobName: 'cron_b',
      });

      expect(result.runId).toBe('test-run-id-456');
    });

    it('metaデータを渡せる', async () => {
      const insertMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          insert: insertMock,
          select: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: { run_id: 'test-run-id' },
            error: null,
          }),
        })),
      };

      await startJobRun(mockSupabase as any, {
        jobName: 'cron_a',
        meta: { source: 'manual', user: 'admin' },
      });

      expect(insertMock).toHaveBeenCalledWith(
        expect.objectContaining({
          meta: { source: 'manual', user: 'admin' },
        })
      );
    });

    it('同一日付の既存runを再取得してrun_idを返す（再ディスパッチ収束）', async () => {
      const single = vi.fn()
        .mockResolvedValueOnce({ data: null, error: { code: '23505', message: 'duplicate key value' } })
        .mockResolvedValueOnce({ data: { run_id: 'existing-run-id' }, error: null });
      const updateMock = vi.fn().mockReturnThis();
      const itemsDeleteEq = vi.fn().mockResolvedValue({ error: null });
      const jobRunsChain = {
        insert: vi.fn().mockReturnThis(),
        update: updateMock,
        select: vi.fn().mockReturnThis(),
        eq: vi.fn().mockReturnThis(),
        single,
      };
      const jobRunItemsChain = {
        delete: vi.fn().mockReturnThis(),
        eq: itemsDeleteEq,
      };
      const mockSupabase = {
        from: vi.fn((table: string) => (table === 'job_run_items' ? jobRunItemsChain : jobRunsChain)),
      };

      const result = await startJobRun(mockSupabase as any, {
        jobName: 'cron_a',
        targetDate: '2024-01-15',
      });

      // 既存の失敗/中断行を running に戻して同じ run_id を返す
      expect(result.runId).toBe('existing-run-id');
      expect(result.error).toBeUndefined();
      expect(updateMock).toHaveBeenCalledWith(
        expect.objectContaining({ status: 'running', finished_at: null, error_message: null })
      );
      // 古い job_run_items はクリアされる
      expect(itemsDeleteEq).toHaveBeenCalledWith('run_id', 'existing-run-id');
    });

    it('既存runがsuccess/running（failed以外）なら再取得せず実行済みを返す', async () => {
      // insert → 23505、update(status=failed条件) → 0件(PGRST116) = 成功/実行中行は巻き戻さない
      const single = vi.fn()
        .mockResolvedValueOnce({ data: null, error: { code: '23505', message: 'duplicate key value' } })
        .mockResolvedValueOnce({ data: null, error: { code: 'PGRST116', message: 'no rows' } });
      const jobRunsChain = {
        insert: vi.fn().mockReturnThis(),
        update: vi.fn().mockReturnThis(),
        select: vi.fn().mockReturnThis(),
        eq: vi.fn().mockReturnThis(),
        single,
      };
      const mockSupabase = {
        from: vi.fn(() => jobRunsChain),
      };

      const result = await startJobRun(mockSupabase as any, {
        jobName: 'cron_a',
        targetDate: '2024-01-15',
      });

      expect(result.runId).toBe('');
      expect(result.error).toBe('Job already executed for this target date');
    });

    it('targetDateなしの23505は再取得せずエラーを返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          insert: vi.fn().mockReturnThis(),
          select: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: '23505', message: 'duplicate key value' },
          }),
        })),
      };

      const result = await startJobRun(mockSupabase as any, {
        jobName: 'cron_b',
      });

      expect(result.runId).toBe('');
      expect(result.error).toBe('duplicate key value');
    });

    it('DBエラーの場合エラーを返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          insert: vi.fn().mockReturnThis(),
          select: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'P0001', message: 'Database error' },
          }),
        })),
      };

      const result = await startJobRun(mockSupabase as any, {
        jobName: 'cron_a',
      });

      expect(result.runId).toBe('');
      expect(result.error).toBe('Database error');
    });
  });

  describe('completeJobRun', () => {
    it('ジョブ実行を成功で完了する', async () => {
      const updateMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          update: updateMock,
          eq: vi.fn().mockResolvedValue({ error: null }),
        })),
      };

      await completeJobRun(mockSupabase as any, 'run-123', 'success');

      expect(updateMock).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'success',
          finished_at: '2024-01-15T10:00:00.000Z',
        })
      );
    });

    it('エラーメッセージ付きで失敗を記録する', async () => {
      const updateMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          update: updateMock,
          eq: vi.fn().mockResolvedValue({ error: null }),
        })),
      };

      await completeJobRun(mockSupabase as any, 'run-123', 'failed', 'API timeout');

      expect(updateMock).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'failed',
          error_message: 'API timeout',
        })
      );
    });

    it('長いエラーメッセージを切り詰める（10000文字）', async () => {
      const updateMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          update: updateMock,
          eq: vi.fn().mockResolvedValue({ error: null }),
        })),
      };

      const longMessage = 'x'.repeat(15000);
      await completeJobRun(mockSupabase as any, 'run-123', 'failed', longMessage);

      expect(updateMock).toHaveBeenCalledWith(
        expect.objectContaining({
          error_message: 'x'.repeat(10000) + '... (truncated)',
        })
      );
    });

    it('DBエラーでも例外を投げない', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          update: vi.fn().mockReturnThis(),
          eq: vi.fn().mockResolvedValue({ error: { message: 'DB Error' } }),
        })),
      };

      // 例外が投げられないことを確認
      await expect(
        completeJobRun(mockSupabase as any, 'run-123', 'success')
      ).resolves.toBeUndefined();
    });
  });

  describe('startJobRunItem', () => {
    it('データセット処理を開始する', async () => {
      const insertMock = vi.fn().mockResolvedValue({ error: null });
      const mockSupabase = {
        from: vi.fn(() => ({
          insert: insertMock,
        })),
      };

      await startJobRunItem(mockSupabase as any, 'run-123', 'equity_bars_daily');

      expect(insertMock).toHaveBeenCalledWith({
        run_id: 'run-123',
        dataset: 'equity_bars_daily',
        status: 'running',
        meta: {},
      });
    });

    it('metaデータを渡せる', async () => {
      const insertMock = vi.fn().mockResolvedValue({ error: null });
      const mockSupabase = {
        from: vi.fn(() => ({
          insert: insertMock,
        })),
      };

      await startJobRunItem(mockSupabase as any, 'run-123', 'equity_bars_daily', {
        pageNumber: 1,
      });

      expect(insertMock).toHaveBeenCalledWith(
        expect.objectContaining({
          meta: { pageNumber: 1 },
        })
      );
    });
  });

  describe('completeJobRunItem', () => {
    it('データセット処理を成功で完了する', async () => {
      const updateMock = vi.fn().mockReturnThis();
      const eqMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          update: updateMock,
          eq: eqMock,
        })),
      };

      // 2つのeq呼び出しの最後でPromiseを返す
      eqMock.mockReturnValueOnce({ eq: eqMock });
      eqMock.mockResolvedValueOnce({ error: null });

      await completeJobRunItem(mockSupabase as any, 'run-123', 'equity_bars_daily', 'success', {
        rowCount: 1000,
        pageCount: 5,
      });

      expect(updateMock).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'success',
          row_count: 1000,
          page_count: 5,
        })
      );
    });

    it('失敗時にエラーメッセージを記録する', async () => {
      const updateMock = vi.fn().mockReturnThis();
      const eqMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          update: updateMock,
          eq: eqMock,
        })),
      };

      eqMock.mockReturnValueOnce({ eq: eqMock });
      eqMock.mockResolvedValueOnce({ error: null });

      await completeJobRunItem(mockSupabase as any, 'run-123', 'equity_bars_daily', 'failed', {
        errorMessage: 'API Error',
      });

      expect(updateMock).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'failed',
          error_message: 'API Error',
        })
      );
    });
  });

  describe('getLatestJobRun', () => {
    it('最新のジョブ実行を取得する', async () => {
      const mockJobRun = {
        run_id: 'run-123',
        job_name: 'cron_a',
        status: 'success',
        started_at: '2024-01-15T09:00:00.000Z',
      };

      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: mockJobRun,
            error: null,
          }),
        })),
      };

      const result = await getLatestJobRun(mockSupabase as any, 'cron_a');

      expect(result).toEqual(mockJobRun);
    });

    it('ステータスでフィルタできる', async () => {
      const eqMock = vi.fn().mockReturnThis();
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: eqMock,
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: { run_id: 'run-123' },
            error: null,
          }),
        })),
      };

      await getLatestJobRun(mockSupabase as any, 'cron_a', 'success');

      // job_name と status の2回呼ばれる
      expect(eqMock).toHaveBeenCalledWith('job_name', 'cron_a');
      expect(eqMock).toHaveBeenCalledWith('status', 'success');
    });

    it('見つからない場合nullを返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'PGRST116' },
          }),
        })),
      };

      const result = await getLatestJobRun(mockSupabase as any, 'cron_a');

      expect(result).toBeNull();
    });

    it('DBエラーの場合nullを返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockReturnThis(),
          single: vi.fn().mockResolvedValue({
            data: null,
            error: { code: 'P0001', message: 'DB Error' },
          }),
        })),
      };

      const result = await getLatestJobRun(mockSupabase as any, 'cron_a');

      expect(result).toBeNull();
    });
  });

  describe('hasJobRunForDate', () => {
    it('実行済みの場合trueを返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          limit: vi.fn().mockResolvedValue({
            data: [{ run_id: 'run-123' }],
            error: null,
          }),
        })),
      };

      const result = await hasJobRunForDate(mockSupabase as any, 'cron_a', '2024-01-15');

      expect(result).toBe(true);
    });

    it('未実行の場合falseを返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          limit: vi.fn().mockResolvedValue({
            data: [],
            error: null,
          }),
        })),
      };

      const result = await hasJobRunForDate(mockSupabase as any, 'cron_a', '2024-01-15');

      expect(result).toBe(false);
    });

    it('ステータスでフィルタできる', async () => {
      const eqCalls: Array<[string, unknown]> = [];

      // クエリビルダーのモック（すべてのメソッドがチェーン可能）
      const createQueryBuilder = (): any => {
        const builder: any = {
          select: vi.fn(() => builder),
          eq: vi.fn((col, val) => {
            eqCalls.push([col, val]);
            return builder;
          }),
          limit: vi.fn(() => builder),
          then: (resolve: (value: any) => void) => {
            resolve({ data: [{ run_id: 'run-123' }], error: null });
          },
        };
        return builder;
      };

      const mockSupabase = {
        from: vi.fn(() => createQueryBuilder()),
      };

      await hasJobRunForDate(mockSupabase as any, 'cron_a', '2024-01-15', 'success');

      // eq が status で呼ばれたことを確認
      expect(eqCalls).toContainEqual(['status', 'success']);
    });

    it('DBエラーの場合falseを返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          limit: vi.fn().mockResolvedValue({
            data: null,
            error: { message: 'DB Error' },
          }),
        })),
      };

      const result = await hasJobRunForDate(mockSupabase as any, 'cron_a', '2024-01-15');

      expect(result).toBe(false);
    });
  });

  describe('getFailedJobRuns', () => {
    it('失敗したジョブ実行のリストを返す', async () => {
      const mockJobRuns = [
        { run_id: 'run-1', status: 'failed' },
        { run_id: 'run-2', status: 'failed' },
      ];

      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockResolvedValue({
            data: mockJobRuns,
            error: null,
          }),
        })),
      };

      const result = await getFailedJobRuns(mockSupabase as any, 'cron_a');

      expect(result).toEqual(mockJobRuns);
    });

    it('limit数を指定できる', async () => {
      const limitMock = vi.fn().mockResolvedValue({ data: [], error: null });
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: limitMock,
        })),
      };

      await getFailedJobRuns(mockSupabase as any, 'cron_a', 5);

      expect(limitMock).toHaveBeenCalledWith(5);
    });

    it('DBエラーの場合空配列を返す', async () => {
      const mockSupabase = {
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          order: vi.fn().mockReturnThis(),
          limit: vi.fn().mockResolvedValue({
            data: null,
            error: { message: 'DB Error' },
          }),
        })),
      };

      const result = await getFailedJobRuns(mockSupabase as any, 'cron_a');

      expect(result).toEqual([]);
    });
  });
});
