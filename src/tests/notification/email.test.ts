/**
 * notification/email.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { JobFailureNotification, JobSuccessNotification } from '@/lib/notification/email';

// モック用のsend関数
const mockSend = vi.fn();

// Resendをモック（コンストラクタとして機能するように）
vi.mock('resend', () => ({
  Resend: class MockResend {
    emails = { send: mockSend };
  },
}));

// モジュールをモック後にインポート
import {
  sendJobFailureEmail,
  sendJobSuccessEmail,
  sendConsecutiveFailureAlert,
  sendWorkflowFailureEmail,
  sendCapacityReportEmail,
} from '@/lib/notification/email';

describe('notification/email.ts', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('sendJobFailureEmail', () => {
    it('RESEND_API_KEY未設定の場合はfalseを返す', async () => {
      delete process.env.RESEND_API_KEY;
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      const data: JobFailureNotification = {
        jobName: 'cron_a',
        runId: 'run-123',
        error: 'Error',
        timestamp: new Date(),
      };

      const result = await sendJobFailureEmail(data);

      expect(result).toBe(false);
    });

    it('ALERT_EMAIL_TO未設定の場合はfalseを返す', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      delete process.env.ALERT_EMAIL_TO;

      const data: JobFailureNotification = {
        jobName: 'cron_a',
        runId: 'run-123',
        error: 'Error',
        timestamp: new Date(),
      };

      const result = await sendJobFailureEmail(data);

      expect(result).toBe(false);
    });

    it('設定が揃っている場合はメール送信する', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({ data: { id: 'email-123' }, error: null });

      const data: JobFailureNotification = {
        jobName: 'cron_a',
        runId: 'run-123',
        error: 'Error',
        timestamp: new Date(),
      };

      const result = await sendJobFailureEmail(data);

      expect(result).toBe(true);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          to: ['test@example.com'],
          subject: expect.stringContaining('cron_a'),
        })
      );
    });

    it('Resendエラーの場合はfalseを返す', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({
        data: null,
        error: { message: 'Rate limit exceeded' },
      });

      const data: JobFailureNotification = {
        jobName: 'cron_a',
        runId: 'run-123',
        error: 'Error',
        timestamp: new Date(),
      };

      const result = await sendJobFailureEmail(data);

      expect(result).toBe(false);
    });

    it('例外が発生してもfalseを返す（クラッシュしない）', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockRejectedValue(new Error('Network error'));

      const data: JobFailureNotification = {
        jobName: 'cron_a',
        runId: 'run-123',
        error: 'Error',
        timestamp: new Date(),
      };

      const result = await sendJobFailureEmail(data);

      expect(result).toBe(false);
    });
  });

  describe('sendJobSuccessEmail', () => {
    it('NOTIFY_ON_SUCCESS未設定の場合はfalseを返す', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';
      delete process.env.NOTIFY_ON_SUCCESS;

      const data: JobSuccessNotification = {
        jobName: 'cron_a',
        runId: 'run-123',
        timestamp: new Date(),
      };

      const result = await sendJobSuccessEmail(data);

      expect(result).toBe(false);
    });

    it('NOTIFY_ON_SUCCESS=trueの場合はメール送信する', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';
      process.env.NOTIFY_ON_SUCCESS = 'true';

      mockSend.mockResolvedValue({ data: { id: 'email-123' }, error: null });

      const data: JobSuccessNotification = {
        jobName: 'cron_a',
        runId: 'run-123',
        timestamp: new Date(),
      };

      const result = await sendJobSuccessEmail(data);

      expect(result).toBe(true);
    });

    it('NOTIFY_ON_SUCCESS=false（文字列）の場合はfalseを返す', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';
      process.env.NOTIFY_ON_SUCCESS = 'false';

      const data: JobSuccessNotification = {
        jobName: 'cron_a',
        runId: 'run-123',
        timestamp: new Date(),
      };

      const result = await sendJobSuccessEmail(data);

      expect(result).toBe(false);
    });
  });

  describe('sendConsecutiveFailureAlert', () => {
    it('連続失敗アラートを送信する', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({ data: { id: 'email-123' }, error: null });

      const result = await sendConsecutiveFailureAlert('cron_a', 3, [
        'Error 1',
        'Error 2',
        'Error 3',
      ]);

      expect(result).toBe(true);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          subject: expect.stringContaining('3 回連続失敗'),
        })
      );
    });

    it('エラーメッセージをHTMLエスケープする', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({ data: { id: 'email-123' }, error: null });

      await sendConsecutiveFailureAlert('cron_a', 1, ['<script>alert("XSS")</script>']);

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          html: expect.not.stringContaining('<script>alert'),
        })
      );
    });

    it('設定不足の場合はfalseを返す', async () => {
      delete process.env.RESEND_API_KEY;

      const result = await sendConsecutiveFailureAlert('cron_a', 3, ['Error']);

      expect(result).toBe(false);
    });
  });

  describe('sendCapacityReportEmail', () => {
    const report = {
      subject: '[DB容量] 421MB / 500MB (84%)',
      html: '<p>report</p>',
    };

    it('RESEND_API_KEY未設定の場合はfalseを返す', async () => {
      delete process.env.RESEND_API_KEY;
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      const result = await sendCapacityReportEmail(report);

      expect(result).toBe(false);
      expect(mockSend).not.toHaveBeenCalled();
    });

    it('ALERT_EMAIL_TO未設定の場合はfalseを返す', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      delete process.env.ALERT_EMAIL_TO;

      const result = await sendCapacityReportEmail(report);

      expect(result).toBe(false);
    });

    it('設定が揃っている場合は渡された件名・本文でメール送信する', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({ data: { id: 'email-123' }, error: null });

      const result = await sendCapacityReportEmail(report);

      expect(result).toBe(true);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          to: ['test@example.com'],
          subject: report.subject,
          html: report.html,
        })
      );
    });

    it('Resendエラーの場合はfalseを返す', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({ data: null, error: { message: 'Rate limit exceeded' } });

      const result = await sendCapacityReportEmail(report);

      expect(result).toBe(false);
    });

    it('例外が発生してもfalseを返す（クラッシュしない）', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockRejectedValue(new Error('Network error'));

      const result = await sendCapacityReportEmail(report);

      expect(result).toBe(false);
    });
  });

  describe('sendWorkflowFailureEmail', () => {
    it('設定が揃っている場合はメール送信し、run URLを含める', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({ data: { id: 'email-123' }, error: null });

      const result = await sendWorkflowFailureEmail({
        job: 'cron_b',
        workflowRunId: '123456789',
        timestamp: new Date(),
      });

      expect(result).toBe(true);
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          to: ['test@example.com'],
          subject: expect.stringContaining('cron_b'),
          html: expect.stringContaining(
            'https://github.com/syukan3/JapanStockDataPipeline/actions/runs/123456789'
          ),
        })
      );
    });

    it('メール本文にこの通知経路の限界（CRON_SECRET不一致・Vercel全体障害は非対応）を明記する', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({ data: { id: 'email-123' }, error: null });

      await sendWorkflowFailureEmail({
        job: 'cron_b',
        workflowRunId: null,
        timestamp: new Date(),
      });

      const html = mockSend.mock.calls[0][0].html.replace(/\s+/g, '');
      expect(html).toContain(
        'この通知自体も本体と同じVercel/CRON_SECRETに依存するため、CRON_SECRET不一致やVercel全体障害時はこの通知も届きません'.replace(
          /\s+/g,
          ''
        )
      );
    });

    it('workflowRunIdがnullの場合はrun URLを含めない', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({ data: { id: 'email-123' }, error: null });

      await sendWorkflowFailureEmail({
        job: 'cron_b',
        workflowRunId: null,
        timestamp: new Date(),
      });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          html: expect.not.stringContaining('github.com'),
        })
      );
    });

    it('RESEND_API_KEY未設定の場合はfalseを返す', async () => {
      delete process.env.RESEND_API_KEY;
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      const result = await sendWorkflowFailureEmail({
        job: 'cron_b',
        workflowRunId: '123',
        timestamp: new Date(),
      });

      expect(result).toBe(false);
    });

    it('Resendエラーの場合はfalseを返す', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockResolvedValue({ data: null, error: { message: 'Rate limit exceeded' } });

      const result = await sendWorkflowFailureEmail({
        job: 'cron_b',
        workflowRunId: '123',
        timestamp: new Date(),
      });

      expect(result).toBe(false);
    });

    it('例外が発生してもfalseを返す（クラッシュしない）', async () => {
      process.env.RESEND_API_KEY = 'test-key';
      process.env.ALERT_EMAIL_TO = 'test@example.com';

      mockSend.mockRejectedValue(new Error('Network error'));

      const result = await sendWorkflowFailureEmail({
        job: 'cron_b',
        workflowRunId: '123',
        timestamp: new Date(),
      });

      expect(result).toBe(false);
    });
  });
});
