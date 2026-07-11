/** earnings calendarの事前検証とfenced atomic publishを検証する。 */

import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { JQuantsClient } from '@/lib/jquants/client';
import type { EarningsCalendarItem } from '@/lib/jquants/types';

const mocks = vi.hoisted(() => ({
  getSupabaseAdmin: vi.fn(),
  rpc: vi.fn(),
}));

vi.mock('@/lib/supabase/admin', () => ({
  getSupabaseAdmin: mocks.getSupabaseAdmin,
}));

import {
  EarningsCalendarSyncError,
  syncEarningsCalendar,
} from '@/lib/jquants/endpoints/earnings-calendar';

const TARGET_DATE = '2024-01-18';
const RUN_ID = 'run-123';
const ATTEMPT_ID = 'attempt-123';

function item(date: string, code: string): EarningsCalendarItem {
  return {
    Date: date,
    Code: code,
    CoName: `Company ${code}`,
    FY: '2024',
    FQ: 'FY',
    SectorNm: 'Services',
  };
}

function client(items: EarningsCalendarItem[]): JQuantsClient {
  return {
    getEarningsCalendar: vi.fn().mockResolvedValue({ data: items }),
  } as unknown as JQuantsClient;
}

function options(items: EarningsCalendarItem[]) {
  return {
    client: client(items),
    expectedAnnouncementDate: TARGET_DATE,
    runId: RUN_ID,
    attemptId: ATTEMPT_ID,
  };
}

describe('syncEarningsCalendar fenced publish', () => {
  beforeEach(() => {
    mocks.rpc.mockReset();
    mocks.getSupabaseAdmin.mockReset();
    mocks.rpc.mockResolvedValue({ data: 0, error: null });
    mocks.getSupabaseAdmin.mockReturnValue({ rpc: mocks.rpc });
  });

  it('expectedAnnouncementDateなしの永続化経路を拒否する', async () => {
    await expect(syncEarningsCalendar(undefined as never)).rejects.toThrow(
      'expectedAnnouncementDate is required'
    );

    expect(mocks.rpc).not.toHaveBeenCalled();
  });

  it('runIdまたはattemptIdなしの永続化経路を拒否する', async () => {
    await expect(
      syncEarningsCalendar({
        ...options([]),
        attemptId: '',
      })
    ).rejects.toThrow('runId and attemptId are required');

    expect(mocks.rpc).not.toHaveBeenCalled();
  });

  it('期待日より未来のAPI行はpublish RPCを呼ばない', async () => {
    await expect(
      syncEarningsCalendar(options([item('2024-01-19', '13010')]))
    ).rejects.toThrow(
      'Earnings calendar target date mismatch: expected 2024-01-18, got 2024-01-19'
    );

    expect(mocks.rpc).not.toHaveBeenCalled();
  });

  it('期待日より過去のAPI行（新規disclosure未確定の閑散期）は対象日を0件でpublishする', async () => {
    const result = await syncEarningsCalendar(options([item('2024-01-10', '13010')]));

    expect(mocks.rpc).toHaveBeenCalledWith(
      'commit_earnings_calendar_attempt',
      {
        p_target_date: TARGET_DATE,
        p_run_id: RUN_ID,
        p_attempt_id: ATTEMPT_ID,
        p_source_observed_at: expect.any(String),
        p_records: [],
      }
    );
    expect(result).toMatchObject({
      fetched: 1,
      inserted: 0,
      announcementDate: null,
      errors: [],
      sourceObservedAt: expect.any(String),
    });
  });

  it('複数日を含むAPI行はpublish RPCを呼ばない', async () => {
    await expect(
      syncEarningsCalendar(
        options([
          item('2024-01-18', '13010'),
          item('2024-01-19', '13020'),
        ])
      )
    ).rejects.toThrow('contains multiple announcement dates');

    expect(mocks.rpc).not.toHaveBeenCalled();
  });

  it('0件再実行も空配列をatomic publishして旧行削除をDBへ委ねる', async () => {
    const result = await syncEarningsCalendar(options([]));

    expect(mocks.rpc).toHaveBeenCalledWith(
      'commit_earnings_calendar_attempt',
      {
        p_target_date: TARGET_DATE,
        p_run_id: RUN_ID,
        p_attempt_id: ATTEMPT_ID,
        p_source_observed_at: expect.any(String),
        p_records: [],
      }
    );
    expect(result).toMatchObject({
      fetched: 0,
      inserted: 0,
      announcementDate: null,
      errors: [],
      sourceObservedAt: expect.any(String),
    });
  });

  it('検証済み全行を1回のatomic publish RPCへ渡す', async () => {
    mocks.rpc.mockResolvedValue({ data: 2, error: null });

    const result = await syncEarningsCalendar(
      options([
        item(TARGET_DATE, '13010'),
        item(TARGET_DATE, '13020'),
      ])
    );

    expect(result).toMatchObject({
      fetched: 2,
      inserted: 2,
      announcementDate: TARGET_DATE,
      errors: [],
    });
    expect(mocks.rpc).toHaveBeenCalledTimes(1);
    expect(mocks.rpc).toHaveBeenCalledWith(
      'commit_earnings_calendar_attempt',
      expect.objectContaining({
        p_target_date: TARGET_DATE,
        p_run_id: RUN_ID,
        p_attempt_id: ATTEMPT_ID,
        p_records: [
          expect.objectContaining({
            announcement_date: TARGET_DATE,
            local_code: '13010',
          }),
          expect.objectContaining({
            announcement_date: TARGET_DATE,
            local_code: '13020',
          }),
        ],
      })
    );
  });

  it('DBが返した実件数がAPI件数と異なれば成功扱いしない', async () => {
    mocks.rpc.mockResolvedValue({ data: 1, error: null });

    await expect(
      syncEarningsCalendar(
        options([
          item(TARGET_DATE, '13010'),
          item(TARGET_DATE, '13020'),
        ])
      )
    ).rejects.toThrow(
      'Earnings calendar row count mismatch for 2024-01-18: expected 2, got 1'
    );
  });

  it('lease喪失後の旧attempt publish拒否を観測情報付き失敗にする', async () => {
    mocks.rpc.mockResolvedValue({
      data: null,
      error: { message: 'stale Cron B attempt cannot commit earnings calendar' },
    });

    let thrown: unknown;
    try {
      await syncEarningsCalendar(options([item(TARGET_DATE, '13010')]));
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(EarningsCalendarSyncError);
    expect(thrown).toMatchObject({
      message:
        'Failed to commit earnings calendar for 2024-01-18: stale Cron B attempt cannot commit earnings calendar',
      fetched: 1,
      inserted: 0,
      sourceObservedAt: expect.any(String),
    });
    expect(mocks.rpc).toHaveBeenCalledTimes(1);
  });

  it('外部fetch中にnew attemptへreclaimされた旧workerは集合を変更できない', async () => {
    let activeAttempt = 'old-attempt';
    let publishedCodes = ['existing-code'];
    let resolveFetch:
      | ((value: { data: EarningsCalendarItem[] }) => void)
      | undefined;
    const delayedClient = {
      getEarningsCalendar: vi.fn(
        () =>
          new Promise<{ data: EarningsCalendarItem[] }>((resolve) => {
            resolveFetch = resolve;
          })
      ),
    } as unknown as JQuantsClient;

    mocks.rpc.mockImplementation(
      (
        _name: string,
        params: { p_attempt_id: string; p_records: Array<{ local_code: string }> }
      ) => {
        if (params.p_attempt_id !== activeAttempt) {
          return Promise.resolve({
            data: null,
            error: {
              message: 'stale Cron B attempt cannot commit earnings calendar',
            },
          });
        }
        publishedCodes = params.p_records.map((record) => record.local_code);
        return Promise.resolve({ data: params.p_records.length, error: null });
      }
    );

    const oldPublish = syncEarningsCalendar({
      client: delayedClient,
      expectedAnnouncementDate: TARGET_DATE,
      runId: RUN_ID,
      attemptId: 'old-attempt',
    });
    await vi.waitFor(() => {
      expect(delayedClient.getEarningsCalendar).toHaveBeenCalledOnce();
    });

    activeAttempt = 'new-attempt';
    resolveFetch?.({ data: [item(TARGET_DATE, 'old-worker-code')] });

    await expect(oldPublish).rejects.toThrow('stale Cron B attempt');
    expect(publishedCodes).toEqual(['existing-code']);

    await syncEarningsCalendar({
      ...options([item(TARGET_DATE, 'new-worker-code')]),
      attemptId: 'new-attempt',
    });
    expect(publishedCodes).toEqual(['new-worker-code']);
  });
});
