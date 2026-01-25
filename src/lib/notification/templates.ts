/**
 * メールテンプレート
 *
 * @description ジョブ通知用のHTMLメールテンプレート
 */

import type { JobFailureNotification, JobSuccessNotification } from './email';
import { escapeHtml } from '../utils/html';

/**
 * ジョブ名を日本語に変換
 */
function getJobNameLabel(jobName: string): string {
  const labels: Record<string, string> = {
    cron_a: 'Cron A (日次確定データ)',
    cron_b: 'Cron B (決算発表予定)',
    cron_c: 'Cron C (投資部門別)',
  };
  return labels[jobName] ?? jobName;
}

/**
 * 失敗通知メールテンプレート
 */
export function getJobFailureEmailTemplate(
  data: JobFailureNotification
): { subject: string; html: string } {
  const jobLabel = getJobNameLabel(data.jobName);
  const timestamp = data.timestamp.toISOString();
  const targetDate = data.targetDate ?? '未指定';
  const errorMessage = escapeHtml(data.error);

  const subject = `[ALERT] ${data.jobName} 失敗 - ${targetDate}`;

  const html = `
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ジョブ失敗通知</title>
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
  <div style="background-color: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 20px; margin-bottom: 20px;">
    <h1 style="color: #dc2626; margin: 0 0 10px 0; font-size: 24px;">
      ジョブ失敗通知
    </h1>
    <p style="color: #991b1b; margin: 0;">
      ${jobLabel} の実行に失敗しました
    </p>
  </div>

  <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold; width: 140px;">
        ジョブ名
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
        ${escapeHtml(data.jobName)}
      </td>
    </tr>
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold;">
        Run ID
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-family: monospace; font-size: 14px;">
        ${escapeHtml(data.runId)}
      </td>
    </tr>
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold;">
        対象日付
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
        ${escapeHtml(targetDate)}
      </td>
    </tr>
    ${data.dataset ? `
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold;">
        データセット
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
        ${escapeHtml(data.dataset)}
      </td>
    </tr>
    ` : ''}
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold;">
        発生時刻
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
        ${timestamp}
      </td>
    </tr>
  </table>

  <div style="background-color: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; margin-bottom: 20px;">
    <h3 style="margin: 0 0 10px 0; color: #374151; font-size: 16px;">
      エラー内容
    </h3>
    <pre style="margin: 0; font-size: 13px; white-space: pre-wrap; word-break: break-word; color: #dc2626; background-color: #fff; padding: 12px; border-radius: 4px; border: 1px solid #fecaca;">
${errorMessage}
    </pre>
  </div>

  <div style="color: #6b7280; font-size: 14px;">
    <p style="margin: 0 0 10px 0;">
      <strong>対応方法:</strong>
    </p>
    <ol style="margin: 0; padding-left: 20px;">
      <li>Supabase Dashboard で <code>job_runs</code> テーブルを確認</li>
      <li>Vercel Logs でエラー詳細を確認</li>
      <li>必要に応じて手動で再実行</li>
    </ol>
  </div>

  <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 20px 0;">

  <p style="color: #9ca3af; font-size: 12px; margin: 0;">
    このメールは JapanStockAnalyzer のジョブ監視システムから自動送信されています。
  </p>
</body>
</html>
`;

  return { subject, html };
}

/**
 * 成功通知メールテンプレート
 */
export function getJobSuccessEmailTemplate(
  data: JobSuccessNotification
): { subject: string; html: string } {
  const jobLabel = getJobNameLabel(data.jobName);
  const timestamp = data.timestamp.toISOString();
  const targetDate = data.targetDate ?? '未指定';
  const rowCount = data.rowCount?.toLocaleString() ?? '不明';
  const duration = data.durationMs
    ? `${(data.durationMs / 1000).toFixed(2)}秒`
    : '不明';

  const subject = `[OK] ${data.jobName} 完了 - ${targetDate}`;

  const html = `
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ジョブ完了通知</title>
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
  <div style="background-color: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; padding: 20px; margin-bottom: 20px;">
    <h1 style="color: #16a34a; margin: 0 0 10px 0; font-size: 24px;">
      ジョブ完了通知
    </h1>
    <p style="color: #15803d; margin: 0;">
      ${jobLabel} が正常に完了しました
    </p>
  </div>

  <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold; width: 140px;">
        ジョブ名
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
        ${escapeHtml(data.jobName)}
      </td>
    </tr>
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold;">
        Run ID
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-family: monospace; font-size: 14px;">
        ${escapeHtml(data.runId)}
      </td>
    </tr>
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold;">
        対象日付
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
        ${escapeHtml(targetDate)}
      </td>
    </tr>
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold;">
        処理行数
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
        ${rowCount} 行
      </td>
    </tr>
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold;">
        処理時間
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
        ${duration}
      </td>
    </tr>
    <tr>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb; font-weight: bold;">
        完了時刻
      </td>
      <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
        ${timestamp}
      </td>
    </tr>
  </table>

  <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 20px 0;">

  <p style="color: #9ca3af; font-size: 12px; margin: 0;">
    このメールは JapanStockAnalyzer のジョブ監視システムから自動送信されています。
  </p>
</body>
</html>
`;

  return { subject, html };
}

/**
 * サマリー通知テンプレート（日次レポート用）
 */
export function getDailySummaryEmailTemplate(
  data: {
    date: string;
    jobs: Array<{
      jobName: string;
      status: 'success' | 'failed' | 'not_run';
      rowCount?: number;
      error?: string;
    }>;
  }
): { subject: string; html: string } {
  const allSuccess = data.jobs.every((j) => j.status === 'success');
  const hasFailure = data.jobs.some((j) => j.status === 'failed');

  const statusIcon = allSuccess ? '✓' : hasFailure ? '✗' : '○';
  const statusColor = allSuccess ? '#16a34a' : hasFailure ? '#dc2626' : '#f59e0b';

  const subject = `[${statusIcon}] 日次サマリー - ${data.date}`;

  const jobRows = data.jobs
    .map((job) => {
      const statusBadge =
        job.status === 'success'
          ? '<span style="background-color: #dcfce7; color: #16a34a; padding: 2px 8px; border-radius: 4px; font-size: 12px;">成功</span>'
          : job.status === 'failed'
          ? '<span style="background-color: #fef2f2; color: #dc2626; padding: 2px 8px; border-radius: 4px; font-size: 12px;">失敗</span>'
          : '<span style="background-color: #fef3c7; color: #d97706; padding: 2px 8px; border-radius: 4px; font-size: 12px;">未実行</span>';

      return `
      <tr>
        <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
          ${escapeHtml(getJobNameLabel(job.jobName))}
        </td>
        <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
          ${statusBadge}
        </td>
        <td style="padding: 12px; border-bottom: 1px solid #e5e7eb;">
          ${job.rowCount?.toLocaleString() ?? '-'}
        </td>
      </tr>
    `;
    })
    .join('');

  const html = `
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <title>日次サマリー</title>
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
  <h1 style="color: ${statusColor}; font-size: 24px; margin-bottom: 20px;">
    日次実行サマリー - ${escapeHtml(data.date)}
  </h1>

  <table style="width: 100%; border-collapse: collapse;">
    <thead>
      <tr style="background-color: #f9fafb;">
        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #e5e7eb;">ジョブ</th>
        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #e5e7eb;">ステータス</th>
        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #e5e7eb;">処理行数</th>
      </tr>
    </thead>
    <tbody>
      ${jobRows}
    </tbody>
  </table>

  <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 20px 0;">

  <p style="color: #9ca3af; font-size: 12px; margin: 0;">
    JapanStockAnalyzer 日次レポート
  </p>
</body>
</html>
`;

  return { subject, html };
}
