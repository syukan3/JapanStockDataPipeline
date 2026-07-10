/** Cron B handlerのattempt fenceと失敗coverage伝播を検証する。 */

import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => {
  class MockEarningsCalendarSyncError extends Error {
    readonly fetched: number;
    readonly inserted: number;
    readonly sourceObservedAt: string | null;

    constructor(
      message: string,
      details: {
        fetched: number;
        inserted: number;
        sourceObservedAt: string | null;
      }
    ) {
      super(message);
      this.name = 'EarningsCalendarSyncError';
      this.fetched = details.fetched;
      this.inserted = details.inserted;
      this.sourceObservedAt = details.sourceObservedAt;
    }
  }

  return {
    syncEarningsCalendar: vi.fn(),
    sendJobFailureEmail: vi.fn(),
    getSupabaseAdmin: vi.fn(),
    rpc: vi.fn(),
    EarningsCalendarSyncError: MockEarningsCalendarSyncError,
  };
});

vi.mock('@/lib/jquants/endpoints/earnings-calendar', () => ({
  EarningsCalendarSyncError: mocks.EarningsCalendarSyncError,
  syncEarningsCalendar: mocks.syncEarningsCalendar,
}));

vi.mock('@/lib/notification/email', () => ({
  sendJobFailureEmail: mocks.sendJobFailureEmail,
}));

vi.mock('@/lib/supabase/admin', () => ({
  getSupabaseAdmin: mocks.getSupabaseAdmin,
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

import { handleCronB } from '@/lib/cron/handlers/cron-b';

const RUN_ID = 'run-123';
const ATTEMPT_ID = 'attempt-123';
const TARGET_DATE = '2024-01-16';

function successResult(fetched = 1) {
  return {
    fetched,
    inserted: fetched,
    announcementDate: fetched === 0 ? null : TARGET_DATE,
    errors: [],
    sourceObservedAt: '2024-01-15T10:00:00.000Z',
  };
}

describe('cron/handlers/cron-b.ts', () => {
  beforeEach(() => {
    mocks.syncEarningsCalendar.mockReset();
    mocks.sendJobFailureEmail.mockReset();
    mocks.getSupabaseAdmin.mockReset();
    mocks.rpc.mockReset();

    mocks.sendJobFailureEmail.mockResolvedValue(undefined);
    mocks.getSupabaseAdmin.mockReturnValue({ rpc: mocks.rpc });
    mocks.rpc.mockImplementation((name: string) =>
      Promise.resolve(
        name === 'fail_earnings_coverage_attempt'
          ? { data: true, error: null }
          : { data: null, error: null }
      )
    );
  });

  it('claim済みattemptをfenced syncへ渡して成功を返す', async () => {
    mocks.syncEarningsCalendar.mockResolvedValue(successResult(50));

    const result = await handleCronB(RUN_ID, TARGET_DATE, ATTEMPT_ID);

    expect(result).toEqual({
      success: true,
      announcementDate: TARGET_DATE,
      fetched: 50,
      inserted: 50,
    });
    expect(mocks.syncEarningsCalendar).toHaveBeenCalledWith({
      logContext: { jobName: 'cron_b', runId: RUN_ID },
      expectedAnnouncementDate: TARGET_DATE,
      runId: RUN_ID,
      attemptId: ATTEMPT_ID,
    });
    expect(mocks.rpc).not.toHaveBeenCalled();
  });

  it('APIが0件でも期待targetDateの成功を返す', async () => {
    mocks.syncEarningsCalendar.mockResolvedValue(successResult(0));

    const result = await handleCronB(RUN_ID, TARGET_DATE, ATTEMPT_ID);

    expect(result).toEqual({
      success: true,
      announcementDate: TARGET_DATE,
      fetched: 0,
      inserted: 0,
    });
  });

  it('同期処理中にhandlerからcoverageを再公開しない', async () => {
    let resolveSync: ((value: ReturnType<typeof successResult>) => void) | undefined;
    mocks.syncEarningsCalendar.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveSync = resolve;
        })
    );

    const pending = handleCronB(RUN_ID, TARGET_DATE, ATTEMPT_ID);

    await vi.waitFor(() => {
      expect(mocks.syncEarningsCalendar).toHaveBeenCalledTimes(1);
    });
    expect(mocks.rpc).not.toHaveBeenCalled();

    resolveSync?.(successResult());
    await pending;
  });

  it('API取得失敗をcurrent attemptのfailed coverageへ記録する', async () => {
    mocks.syncEarningsCalendar.mockRejectedValue(new Error('API timeout'));

    const result = await handleCronB(RUN_ID, TARGET_DATE, ATTEMPT_ID);

    expect(result).toEqual({
      success: false,
      announcementDate: null,
      fetched: 0,
      inserted: 0,
      error: 'API timeout',
    });
    expect(mocks.rpc).toHaveBeenCalledWith(
      'fail_earnings_coverage_attempt',
      {
        p_target_date: TARGET_DATE,
        p_run_id: RUN_ID,
        p_attempt_id: ATTEMPT_ID,
        p_row_count: 0,
        p_error_count: 1,
        p_source_observed_at: null,
      }
    );
    expect(mocks.sendJobFailureEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        jobName: 'cron_b',
        error: 'API timeout',
        runId: RUN_ID,
      })
    );
  });

  it('API観測後の永続化失敗でも観測情報をfailed coverageへ伝える', async () => {
    mocks.syncEarningsCalendar.mockRejectedValue(
      new mocks.EarningsCalendarSyncError('commit failed', {
        fetched: 12,
        inserted: 0,
        sourceObservedAt: '2024-01-15T10:05:00.000Z',
      })
    );

    const result = await handleCronB(RUN_ID, TARGET_DATE, ATTEMPT_ID);

    expect(result).toMatchObject({
      success: false,
      fetched: 12,
      inserted: 0,
      error: 'commit failed',
    });
    expect(mocks.rpc).toHaveBeenCalledWith(
      'fail_earnings_coverage_attempt',
      expect.objectContaining({
        p_row_count: 0,
        p_source_observed_at: '2024-01-15T10:05:00.000Z',
      })
    );
  });

  it('superseded attemptは後続successをfailedへ上書きしない', async () => {
    mocks.syncEarningsCalendar.mockRejectedValue(new Error('old worker failed'));
    mocks.rpc.mockImplementation((name: string) =>
      Promise.resolve(
        name === 'fail_earnings_coverage_attempt'
          ? { data: false, error: null }
          : { data: null, error: null }
      )
    );

    const result = await handleCronB(RUN_ID, TARGET_DATE, ATTEMPT_ID);

    expect(result).toMatchObject({ success: false, error: 'old worker failed' });
    expect(mocks.rpc).toHaveBeenCalledWith(
      'fail_earnings_coverage_attempt',
      expect.any(Object)
    );
    expect(mocks.sendJobFailureEmail).not.toHaveBeenCalled();
  });

  it('failed coverage RPC障害をエラーへ追記する', async () => {
    mocks.syncEarningsCalendar.mockRejectedValue(new Error('API timeout'));
    mocks.rpc.mockImplementation((name: string) =>
      Promise.resolve(
        name === 'fail_earnings_coverage_attempt'
          ? { data: null, error: { message: 'coverage unavailable' } }
          : { data: null, error: null }
      )
    );

    const result = await handleCronB(RUN_ID, TARGET_DATE, ATTEMPT_ID);

    expect(result.error).toBe(
      'API timeout; Failed to persist failed dataset coverage: coverage unavailable'
    );
  });

  it('メール送信失敗でもhandler結果を維持する', async () => {
    mocks.syncEarningsCalendar.mockRejectedValue(new Error('Sync error'));
    mocks.sendJobFailureEmail.mockRejectedValue(new Error('SMTP error'));

    const result = await handleCronB(RUN_ID, TARGET_DATE, ATTEMPT_ID);

    expect(result).toMatchObject({ success: false, error: 'Sync error' });
  });
});
