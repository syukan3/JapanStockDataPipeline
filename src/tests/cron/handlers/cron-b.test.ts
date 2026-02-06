/**
 * cron/handlers/cron-b.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const { mockSyncEarningsCalendar, mockSendJobFailureEmail } = vi.hoisted(() => ({
  mockSyncEarningsCalendar: vi.fn(),
  mockSendJobFailureEmail: vi.fn(),
}));

vi.mock('@/lib/jquants/endpoints/earnings-calendar', () => ({
  syncEarningsCalendar: mockSyncEarningsCalendar,
}));

vi.mock('@/lib/notification/email', () => ({
  sendJobFailureEmail: mockSendJobFailureEmail,
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

describe('cron/handlers/cron-b.ts', () => {
  beforeEach(() => {
    mockSyncEarningsCalendar.mockReset();
    mockSendJobFailureEmail.mockReset();
  });

  it('同期成功時にsuccess=trueを返す', async () => {
    mockSyncEarningsCalendar.mockResolvedValue({
      fetched: 50,
      inserted: 50,
      announcementDate: '2024-01-16',
    });

    const result = await handleCronB('run-123');

    expect(result.success).toBe(true);
    expect(result.fetched).toBe(50);
    expect(result.inserted).toBe(50);
  });

  it('announcementDateを正しく返す', async () => {
    mockSyncEarningsCalendar.mockResolvedValue({
      fetched: 10,
      inserted: 10,
      announcementDate: '2024-01-17',
    });

    const result = await handleCronB('run-456');

    expect(result.announcementDate).toBe('2024-01-17');
  });

  it('エラー発生時にsuccess=falseを返す', async () => {
    mockSyncEarningsCalendar.mockRejectedValue(new Error('API timeout'));

    const result = await handleCronB('run-789');

    expect(result.success).toBe(false);
    expect(result.error).toBe('API timeout');
    expect(result.fetched).toBe(0);
    expect(result.inserted).toBe(0);
    expect(result.announcementDate).toBeNull();
  });

  it('エラー時にメール通知を送信する', async () => {
    mockSyncEarningsCalendar.mockRejectedValue(new Error('API error'));
    mockSendJobFailureEmail.mockResolvedValue(undefined);

    await handleCronB('run-error');

    expect(mockSendJobFailureEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        jobName: 'cron_b',
        error: 'API error',
        runId: 'run-error',
      })
    );
  });

  it('メール送信失敗でもクラッシュしない', async () => {
    mockSyncEarningsCalendar.mockRejectedValue(new Error('Sync error'));
    mockSendJobFailureEmail.mockRejectedValue(new Error('SMTP error'));

    const result = await handleCronB('run-mail-fail');

    expect(result.success).toBe(false);
    expect(result.error).toBe('Sync error');
  });
});
