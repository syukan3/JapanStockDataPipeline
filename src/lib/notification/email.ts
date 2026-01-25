/**
 * メール通知（Resend）
 *
 * @description ジョブ失敗時のメール通知
 * @see https://resend.com/docs
 */

import { Resend } from 'resend';
import { createLogger } from '../utils/logger';
import { escapeHtml } from '../utils/html';
import { getJobFailureEmailTemplate, getJobSuccessEmailTemplate } from './templates';
import type { JobName } from '../cron/job-run';

const logger = createLogger({ module: 'email' });

/**
 * Resend クライアントを取得
 */
function getResendClient(): Resend | null {
  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) {
    logger.warn('RESEND_API_KEY is not set, email notifications disabled');
    return null;
  }
  return new Resend(apiKey);
}

/**
 * 通知先メールアドレスを取得
 */
function getAlertEmailTo(): string | null {
  const email = process.env.ALERT_EMAIL_TO;
  if (!email) {
    logger.warn('ALERT_EMAIL_TO is not set');
    return null;
  }
  return email;
}

/**
 * 送信元メールアドレスを取得
 */
function getEmailFrom(): string {
  return process.env.EMAIL_FROM ?? 'JapanStockAnalyzer <noreply@resend.dev>';
}

export interface JobFailureNotification {
  /** ジョブ名 */
  jobName: JobName;
  /** 実行ID */
  runId: string;
  /** 対象日付 */
  targetDate?: string;
  /** エラーメッセージ */
  error: string;
  /** 発生時刻 */
  timestamp: Date;
  /** データセット */
  dataset?: string;
  /** 追加情報 */
  meta?: Record<string, unknown>;
}

export interface JobSuccessNotification {
  /** ジョブ名 */
  jobName: JobName;
  /** 実行ID */
  runId: string;
  /** 対象日付 */
  targetDate?: string;
  /** 処理行数 */
  rowCount?: number;
  /** 処理時間（ミリ秒） */
  durationMs?: number;
  /** 完了時刻 */
  timestamp: Date;
}

/**
 * ジョブ失敗通知メールを送信
 *
 * @param data 通知データ
 * @returns 送信成功した場合 true
 */
export async function sendJobFailureEmail(
  data: JobFailureNotification
): Promise<boolean> {
  const resend = getResendClient();
  const to = getAlertEmailTo();

  if (!resend || !to) {
    logger.info('Email notification skipped (not configured)', { jobName: data.jobName });
    return false;
  }

  const { subject, html } = getJobFailureEmailTemplate(data);

  try {
    const result = await resend.emails.send({
      from: getEmailFrom(),
      to: [to],
      subject,
      html,
    });

    if (result.error) {
      logger.error('Failed to send failure notification email', {
        jobName: data.jobName,
        error: result.error,
      });
      return false;
    }

    logger.info('Failure notification email sent', {
      jobName: data.jobName,
      runId: data.runId,
      emailId: result.data?.id,
    });
    return true;
  } catch (error) {
    // 通知失敗はログに記録するが、ジョブ自体は続行
    logger.error('Error sending failure notification email', {
      jobName: data.jobName,
      error,
    });
    return false;
  }
}

/**
 * ジョブ成功通知メールを送信（オプション）
 *
 * @description 通常は不要だが、キャッチアップ完了時などに使用
 */
export async function sendJobSuccessEmail(
  data: JobSuccessNotification
): Promise<boolean> {
  // 成功通知は環境変数で有効にした場合のみ
  if (process.env.NOTIFY_ON_SUCCESS !== 'true') {
    return false;
  }

  const resend = getResendClient();
  const to = getAlertEmailTo();

  if (!resend || !to) {
    return false;
  }

  const { subject, html } = getJobSuccessEmailTemplate(data);

  try {
    const result = await resend.emails.send({
      from: getEmailFrom(),
      to: [to],
      subject,
      html,
    });

    if (result.error) {
      logger.error('Failed to send success notification email', {
        jobName: data.jobName,
        error: result.error,
      });
      return false;
    }

    logger.info('Success notification email sent', {
      jobName: data.jobName,
      runId: data.runId,
    });
    return true;
  } catch (error) {
    logger.error('Error sending success notification email', {
      jobName: data.jobName,
      error,
    });
    return false;
  }
}

/**
 * 連続失敗時の警告通知
 */
export async function sendConsecutiveFailureAlert(
  jobName: JobName,
  failureCount: number,
  recentErrors: string[]
): Promise<boolean> {
  const resend = getResendClient();
  const to = getAlertEmailTo();

  if (!resend || !to) {
    return false;
  }

  try {
    const result = await resend.emails.send({
      from: getEmailFrom(),
      to: [to],
      subject: `[CRITICAL] ${jobName} が ${failureCount} 回連続失敗`,
      html: `
        <h2 style="color: #dc2626;">連続失敗アラート</h2>
        <p><strong>ジョブ名:</strong> ${jobName}</p>
        <p><strong>連続失敗回数:</strong> ${failureCount}</p>
        <h3>直近のエラー:</h3>
        <ul>
          ${recentErrors.map((err) => `<li><pre>${escapeHtml(err)}</pre></li>`).join('')}
        </ul>
        <p style="color: #6b7280;">
          このアラートは ${failureCount} 回連続でジョブが失敗したため送信されました。
          Supabase Dashboard で詳細を確認してください。
        </p>
      `,
    });

    return !result.error;
  } catch (error) {
    logger.error('Error sending consecutive failure alert', { jobName, error });
    return false;
  }
}
