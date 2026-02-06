/**
 * cron/handlers/cron-a-chunk.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const {
  mockSyncEquityBarsDailySinglePage,
  mockDetermineTargetDates,
  mockCreateAdminClient,
  mockStartJobRun,
  mockCompleteJobRun,
  mockUpdateHeartbeat,
} = vi.hoisted(() => ({
  mockSyncEquityBarsDailySinglePage: vi.fn(),
  mockDetermineTargetDates: vi.fn(),
  mockCreateAdminClient: vi.fn(),
  mockStartJobRun: vi.fn(),
  mockCompleteJobRun: vi.fn(),
  mockUpdateHeartbeat: vi.fn(),
}));

vi.mock('@/lib/jquants/endpoints/equity-bars-daily', () => ({
  syncEquityBarsDailySinglePage: mockSyncEquityBarsDailySinglePage,
}));

vi.mock('@/lib/cron/catch-up', () => ({
  determineTargetDates: mockDetermineTargetDates,
}));

vi.mock('@/lib/supabase/admin', () => ({
  createAdminClient: mockCreateAdminClient,
}));

vi.mock('@/lib/cron/job-run', () => ({
  startJobRun: mockStartJobRun,
  completeJobRun: mockCompleteJobRun,
}));

vi.mock('@/lib/cron/heartbeat', () => ({
  updateHeartbeat: mockUpdateHeartbeat,
}));

vi.mock('@/lib/utils/logger', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    startTimer: vi.fn(() => ({ end: vi.fn(), endWithError: vi.fn() })),
  })),
}));

import { handleCronAChunk, CronAChunkRequestSchema } from '@/lib/cron/handlers/cron-a-chunk';

describe('cron/handlers/cron-a-chunk.ts', () => {
  beforeEach(() => {
    mockSyncEquityBarsDailySinglePage.mockReset();
    mockDetermineTargetDates.mockReset();
    mockCreateAdminClient.mockReturnValue({});
    mockStartJobRun.mockReset();
    mockCompleteJobRun.mockReset();
    mockUpdateHeartbeat.mockReset();
  });

  describe('CronAChunkRequestSchema', () => {
    it('date単独で有効', () => {
      const result = CronAChunkRequestSchema.safeParse({ date: '2024-01-15' });
      expect(result.success).toBe(true);
    });

    it('pagination_key + dateの組み合わせで有効', () => {
      const result = CronAChunkRequestSchema.safeParse({
        pagination_key: 'abc123',
        date: '2024-01-15',
      });
      expect(result.success).toBe(true);
    });

    it('pagination_keyのみ（dateなし）は無効', () => {
      const result = CronAChunkRequestSchema.safeParse({
        pagination_key: 'abc123',
      });
      expect(result.success).toBe(false);
    });

    it('両方省略で有効（キャッチアップで自動決定）', () => {
      const result = CronAChunkRequestSchema.safeParse({});
      expect(result.success).toBe(true);
    });
  });

  describe('date指定', () => {
    it('キャッチアップをスキップする', async () => {
      mockSyncEquityBarsDailySinglePage.mockResolvedValue({
        fetched: 100,
        inserted: 100,
        paginationKey: undefined,
      });
      mockStartJobRun.mockResolvedValue({ runId: 'run-1' });
      mockCompleteJobRun.mockResolvedValue(undefined);
      mockUpdateHeartbeat.mockResolvedValue(undefined);

      await handleCronAChunk({ date: '2024-01-15' });

      expect(mockDetermineTargetDates).not.toHaveBeenCalled();
    });
  });

  describe('date未指定', () => {
    it('determineTargetDatesで日付を決定する', async () => {
      mockDetermineTargetDates.mockResolvedValue(['2024-01-12']);
      mockSyncEquityBarsDailySinglePage.mockResolvedValue({
        fetched: 100,
        inserted: 100,
        paginationKey: undefined,
      });
      mockStartJobRun.mockResolvedValue({ runId: 'run-1' });
      mockCompleteJobRun.mockResolvedValue(undefined);
      mockUpdateHeartbeat.mockResolvedValue(undefined);

      const result = await handleCronAChunk({});

      expect(mockDetermineTargetDates).toHaveBeenCalled();
      expect(result.targetDate).toBe('2024-01-12');
    });
  });

  describe('対象日なし', () => {
    it('done=trueで返す', async () => {
      mockDetermineTargetDates.mockResolvedValue([]);

      const result = await handleCronAChunk({});

      expect(result.success).toBe(true);
      expect(result.done).toBe(true);
      expect(result.targetDate).toBeNull();
      expect(result.fetched).toBe(0);
    });
  });

  describe('ページ処理', () => {
    it('1ページ処理後にpagination_keyを返す（完了でない）', async () => {
      mockSyncEquityBarsDailySinglePage.mockResolvedValue({
        fetched: 100,
        inserted: 100,
        paginationKey: 'next-page',
      });

      const result = await handleCronAChunk({ date: '2024-01-15' });

      expect(result.done).toBe(false);
      expect(result.pagination_key).toBe('next-page');
      expect(result.fetched).toBe(100);
    });

    it('最終ページでdone=trueを返す', async () => {
      mockSyncEquityBarsDailySinglePage.mockResolvedValue({
        fetched: 50,
        inserted: 50,
        paginationKey: undefined,
      });
      mockStartJobRun.mockResolvedValue({ runId: 'run-1' });
      mockCompleteJobRun.mockResolvedValue(undefined);
      mockUpdateHeartbeat.mockResolvedValue(undefined);

      const result = await handleCronAChunk({ date: '2024-01-15' });

      expect(result.done).toBe(true);
      expect(result.pagination_key).toBeUndefined();
    });
  });

  describe('job_run記録', () => {
    it('最終ページ完了時にjob_runを記録する', async () => {
      mockSyncEquityBarsDailySinglePage.mockResolvedValue({
        fetched: 50,
        inserted: 50,
        paginationKey: undefined,
      });
      mockStartJobRun.mockResolvedValue({ runId: 'run-complete' });
      mockCompleteJobRun.mockResolvedValue(undefined);
      mockUpdateHeartbeat.mockResolvedValue(undefined);

      await handleCronAChunk({ date: '2024-01-15' });

      expect(mockStartJobRun).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({
          jobName: 'cron_a',
          targetDate: '2024-01-15',
          meta: expect.objectContaining({ source: 'chunk_fallback' }),
        })
      );
      expect(mockCompleteJobRun).toHaveBeenCalled();
    });

    it('job_run記録失敗でも成功を返す', async () => {
      mockSyncEquityBarsDailySinglePage.mockResolvedValue({
        fetched: 50,
        inserted: 50,
        paginationKey: undefined,
      });
      mockStartJobRun.mockRejectedValue(new Error('DB Error'));

      const result = await handleCronAChunk({ date: '2024-01-15' });

      expect(result.success).toBe(true);
    });
  });

  describe('syncエラー', () => {
    it('success=falseを返す', async () => {
      mockSyncEquityBarsDailySinglePage.mockRejectedValue(new Error('Sync error'));

      const result = await handleCronAChunk({ date: '2024-01-15' });

      expect(result.success).toBe(false);
      expect(result.error).toBe('Sync error');
      expect(result.done).toBe(false);
    });
  });
});
