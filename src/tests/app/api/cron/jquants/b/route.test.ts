/**
 * app/api/cron/jquants/b/route.ts のcoverage契約テスト
 */

import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  requireCronAuth: vi.fn(),
  acquireLock: vi.fn(),
  releaseLock: vi.fn(),
  startJobRun: vi.fn(),
  completeJobRun: vi.fn(),
  updateHeartbeat: vi.fn(),
  getNextBusinessDay: vi.fn(),
  handleCronB: vi.fn(),
  createAdminClient: vi.fn(),
}));

vi.mock('@/lib/cron/auth', () => ({
  requireCronAuth: mocks.requireCronAuth,
}));

vi.mock('@/lib/cron/job-lock', () => ({
  acquireLock: mocks.acquireLock,
  releaseLock: mocks.releaseLock,
}));

vi.mock('@/lib/cron/job-run', () => ({
  startJobRun: mocks.startJobRun,
  completeJobRun: mocks.completeJobRun,
}));

vi.mock('@/lib/cron/heartbeat', () => ({
  updateHeartbeat: mocks.updateHeartbeat,
}));

vi.mock('@/lib/cron/business-day', () => ({
  getNextBusinessDay: mocks.getNextBusinessDay,
}));

vi.mock('@/lib/cron/handlers', () => ({
  handleCronB: mocks.handleCronB,
}));

vi.mock('@/lib/supabase/admin', () => ({
  createAdminClient: mocks.createAdminClient,
}));

vi.mock('@/lib/utils/logger', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
}));

import { POST } from '@/app/api/cron/jquants/b/route';

const coreClient = { schema: 'jquants_core' };
const ingestClient = { schema: 'jquants_ingest' };

function request(): Request {
  return new Request('http://localhost/api/cron/jquants/b', {
    method: 'POST',
  });
}

describe('POST /api/cron/jquants/b', () => {
  beforeEach(() => {
    mocks.requireCronAuth.mockReturnValue(null);
    mocks.createAdminClient.mockImplementation((schema: string) =>
      schema === 'jquants_core' ? coreClient : ingestClient
    );
    mocks.getNextBusinessDay.mockResolvedValue('2024-01-16');
    mocks.acquireLock.mockResolvedValue({ success: true, token: 'lock-token' });
    mocks.releaseLock.mockResolvedValue(undefined);
    mocks.startJobRun.mockResolvedValue({
      runId: 'run-123',
      attemptId: 'attempt-123',
    });
    mocks.completeJobRun.mockResolvedValue({ completed: true });
    mocks.updateHeartbeat.mockResolvedValue(undefined);
    mocks.handleCronB.mockResolvedValue({
      success: true,
      announcementDate: '2024-01-16',
      fetched: 0,
      inserted: 0,
    });
  });

  it('core calendarでtargetDateを先に決め、job runとrunning/final heartbeatへ伝播する', async () => {
    const response = await POST(request());

    expect(response.status).toBe(200);
    expect(mocks.createAdminClient).toHaveBeenCalledWith('jquants_core');
    expect(mocks.getNextBusinessDay).toHaveBeenCalledWith(coreClient);
    expect(mocks.getNextBusinessDay.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.acquireLock.mock.invocationCallOrder[0]
    );
    expect(mocks.startJobRun).toHaveBeenCalledWith(ingestClient, {
      jobName: 'cron_b',
      targetDate: '2024-01-16',
      reclaimStaleAfterSeconds: 60,
      reclaimSuccessAfterSeconds: 21_600,
      coverageDataset: 'earnings_calendar',
    });
    expect(mocks.handleCronB).toHaveBeenCalledWith(
      'run-123',
      '2024-01-16',
      'attempt-123'
    );
    expect(mocks.completeJobRun).toHaveBeenCalledWith(
      ingestClient,
      'run-123',
      'success',
      undefined,
      'attempt-123',
      { fetched: 0, inserted: 0 }
    );
    expect(mocks.updateHeartbeat).not.toHaveBeenCalled();

    await expect(response.json()).resolves.toMatchObject({
      success: true,
      announcementDate: '2024-01-16',
      fetched: 0,
      inserted: 0,
    });
  });

  it('targetDateを決定できなければロックやjob runを開始せずfail closedする', async () => {
    mocks.getNextBusinessDay.mockResolvedValue(null);

    const response = await POST(request());

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toEqual({
      error: 'Failed to determine target date',
    });
    expect(mocks.acquireLock).not.toHaveBeenCalled();
    expect(mocks.startJobRun).not.toHaveBeenCalled();
    expect(mocks.updateHeartbeat).not.toHaveBeenCalled();
    expect(mocks.handleCronB).not.toHaveBeenCalled();
  });

  it('targetDate取得が例外で失敗してもfail closedする', async () => {
    mocks.getNextBusinessDay.mockRejectedValue(new Error('calendar unavailable'));

    const response = await POST(request());

    expect(response.status).toBe(500);
    expect(mocks.acquireLock).not.toHaveBeenCalled();
    expect(mocks.startJobRun).not.toHaveBeenCalled();
  });

  it('ハンドラーが例外で失敗したfinal heartbeatにもtargetDateを残す', async () => {
    mocks.handleCronB.mockRejectedValue(new Error('handler crashed'));

    const response = await POST(request());

    expect(response.status).toBe(500);
    expect(mocks.completeJobRun).toHaveBeenCalledWith(
      ingestClient,
      'run-123',
      'failed',
      'handler crashed',
      'attempt-123',
      {}
    );
    expect(mocks.updateHeartbeat).not.toHaveBeenCalled();
    expect(mocks.releaseLock).toHaveBeenCalledWith(
      ingestClient,
      'cron_b',
      'lock-token'
    );
  });

  it('ハンドラーの意味的失敗をHTTP 500で呼び出し元へ伝える', async () => {
    mocks.handleCronB.mockResolvedValue({
      success: false,
      announcementDate: null,
      fetched: 10,
      inserted: 0,
      error: 'target date mismatch',
    });

    const response = await POST(request());

    expect(response.status).toBe(500);
    expect(mocks.completeJobRun).toHaveBeenCalledWith(
      ingestClient,
      'run-123',
      'failed',
      'target date mismatch',
      'attempt-123',
      { fetched: 10, inserted: 0 }
    );
    await expect(response.json()).resolves.toMatchObject({
      success: false,
      error: 'target date mismatch',
    });
  });

  it('job runのDB障害を実行済みと誤認せずHTTP 500を返す', async () => {
    mocks.startJobRun.mockResolvedValue({
      runId: '',
      error: 'database unavailable',
    });

    const response = await POST(request());

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toEqual({
      error: 'Failed to start job run',
      detail: 'database unavailable',
    });
    expect(mocks.handleCronB).not.toHaveBeenCalled();
  });

  it('同一targetの実行済みだけは冪等なHTTP 200を返す', async () => {
    mocks.startJobRun.mockResolvedValue({
      runId: '',
      error: 'Job already executed for this target date',
      alreadyExecuted: true,
    });

    const response = await POST(request());

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      error: 'Job already executed',
    });
    expect(mocks.handleCronB).not.toHaveBeenCalled();
  });

  it('reclaim済みattemptの完了を拒否しfinal heartbeatを上書きしない', async () => {
    mocks.completeJobRun.mockResolvedValue({
      completed: false,
      reason: 'superseded',
    });

    const response = await POST(request());

    expect(response.status).toBe(409);
    await expect(response.json()).resolves.toEqual({
      error: 'Job attempt superseded',
      runId: 'run-123',
    });
    expect(mocks.updateHeartbeat).not.toHaveBeenCalled();
  });

  it('job completion RPCのDB障害はsupersededと誤認せずHTTP 500にする', async () => {
    mocks.completeJobRun.mockResolvedValue({
      completed: false,
      reason: 'db_error',
      error: 'database unavailable',
    });

    const response = await POST(request());

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toEqual({
      error: 'Failed to complete job run',
      detail: 'database unavailable',
      runId: 'run-123',
    });
  });
});
