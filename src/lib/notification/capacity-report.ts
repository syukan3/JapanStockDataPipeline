/**
 * DB容量レポート
 *
 * @description db-archival ジョブが毎回（週1回）送信する容量レポートの組み立て。
 * 閾値超過時のみ動くアーカイブとは独立に、容量の推移を常時可視化する。
 * @see docs/PLANS-supabase-pro-ai-2026-07.md §6-5
 */
import { escapeHtml } from '../utils/html';

/**
 * Supabase Free プランのDB容量上限 (MB)
 *
 * @see https://supabase.com/pricing （Free プランの Database size 上限）
 */
export const FREE_PLAN_DB_LIMIT_MB = 500;

/** この使用率(%)以上で件名に警告プレフィックスを付与 */
export const CAPACITY_WARNING_THRESHOLD_PCT = 80;

/** この使用率(%)以上で件名に危険プレフィックスを付与 */
export const CAPACITY_CRITICAL_THRESHOLD_PCT = 90;

export interface CapacityTopTable {
  schemaName: string;
  tableName: string;
  totalSizeBytes: number;
}

/** 今回のジョブ実行でのアーカイブ実施有無 */
export type CapacityArchivalOutcome =
  | { executed: false }
  | {
      executed: true;
      archiveRange: string;
      rowsArchived: number;
      savedMb: number;
      storagePath: string;
    };

export interface CapacityReportInput {
  /** DB全体サイズ (MB) */
  dbSizeMb: number;
  /** サイズ上位テーブル（大きい順） */
  topTables: CapacityTopTable[];
  /** jquants_core.equity_bar_daily のデッドタプル数 */
  equityBarDeadTupleCount: number;
  /** 今回のアーカイブ実施有無・実施時の詳細 */
  archival: CapacityArchivalOutcome;
  /** レポート生成時刻 */
  generatedAt: Date;
  /** Free プラン上限 (MB)。省略時は FREE_PLAN_DB_LIMIT_MB */
  freePlanLimitMb?: number;
}

export interface CapacityReport {
  subject: string;
  html: string;
  /** 上限に対する使用率 (%、四捨五入) */
  usagePct: number;
}

function formatBytesAsMb(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function subjectPrefix(rawPct: number): string {
  if (rawPct >= CAPACITY_CRITICAL_THRESHOLD_PCT) return '🚨 ';
  if (rawPct >= CAPACITY_WARNING_THRESHOLD_PCT) return '⚠️ ';
  return '';
}

function severityColor(rawPct: number): string {
  if (rawPct >= CAPACITY_CRITICAL_THRESHOLD_PCT) return '#dc2626';
  if (rawPct >= CAPACITY_WARNING_THRESHOLD_PCT) return '#d97706';
  return '#16a34a';
}

function buildTopTableRows(topTables: CapacityTopTable[]): string {
  if (topTables.length === 0) {
    return '<tr><td colspan="3" style="padding: 8px; color: #6b7280;">データなし</td></tr>';
  }

  return topTables
    .map(
      (t, i) => `
      <tr>
        <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">${i + 1}</td>
        <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; font-family: monospace; font-size: 13px;">${escapeHtml(t.schemaName)}.${escapeHtml(t.tableName)}</td>
        <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">${formatBytesAsMb(t.totalSizeBytes)}</td>
      </tr>`
    )
    .join('');
}

function buildArchivalSection(archival: CapacityArchivalOutcome): string {
  if (!archival.executed) {
    return `
    <div style="background-color: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; margin-bottom: 20px;">
      <h3 style="margin: 0 0 6px 0; color: #374151; font-size: 16px;">アーカイブ実施</h3>
      <p style="margin: 0; color: #6b7280;">今回は未実施（閾値未達）</p>
    </div>`;
  }

  return `
    <div style="background-color: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; padding: 16px; margin-bottom: 20px;">
      <h3 style="margin: 0 0 10px 0; color: #15803d; font-size: 16px;">アーカイブ実施</h3>
      <p style="margin: 4px 0;"><strong>退避範囲:</strong> ${escapeHtml(archival.archiveRange)}</p>
      <p style="margin: 4px 0;"><strong>削除行数:</strong> ${archival.rowsArchived.toLocaleString()} 行</p>
      <p style="margin: 4px 0;"><strong>削減容量:</strong> 約 ${archival.savedMb} MB</p>
      <p style="margin: 4px 0;"><strong>退避先:</strong> ${escapeHtml(archival.storagePath)}</p>
    </div>`;
}

/**
 * DB容量レポートの件名・本文を組み立てる（純関数）
 */
export function buildCapacityReport(input: CapacityReportInput): CapacityReport {
  const limitMb = input.freePlanLimitMb ?? FREE_PLAN_DB_LIMIT_MB;
  // 閾値判定は丸め誤差で境界がずれないよう、丸める前の実数値で行う
  // （例: 79.9% は Math.round すると 80 になり誤って警告扱いになってしまう）
  const rawPct = (input.dbSizeMb / limitMb) * 100;
  const usagePct = Math.round(rawPct);
  const dbSizeMbRounded = Math.round(input.dbSizeMb);

  const subject = `${subjectPrefix(rawPct)}[DB容量] ${dbSizeMbRounded}MB / ${limitMb}MB (${usagePct}%)`;
  const color = severityColor(rawPct);

  const html = `
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DB容量レポート</title>
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
  <h1 style="color: ${color}; margin: 0 0 10px 0; font-size: 24px;">DB容量レポート</h1>
  <p style="color: #6b7280; margin: 0 0 20px 0;">${input.generatedAt.toISOString()}</p>

  <div style="background-color: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 20px; margin-bottom: 20px;">
    <p style="margin: 0; font-size: 22px; font-weight: bold; color: ${color};">
      ${dbSizeMbRounded} MB / ${limitMb} MB (${usagePct}%)
    </p>
  </div>

  <h3 style="margin: 0 0 10px 0; color: #374151; font-size: 16px;">サイズ上位テーブル (Top ${input.topTables.length})</h3>
  <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
    <thead>
      <tr style="background-color: #f9fafb;">
        <th style="padding: 8px; text-align: left; border-bottom: 2px solid #e5e7eb;">#</th>
        <th style="padding: 8px; text-align: left; border-bottom: 2px solid #e5e7eb;">テーブル</th>
        <th style="padding: 8px; text-align: right; border-bottom: 2px solid #e5e7eb;">サイズ</th>
      </tr>
    </thead>
    <tbody>
      ${buildTopTableRows(input.topTables)}
    </tbody>
  </table>

  <p style="margin: 0 0 20px 0; color: #374151;">
    <strong>jquants_core.equity_bar_daily デッドタプル数:</strong> ${input.equityBarDeadTupleCount.toLocaleString()} 行
  </p>

  ${buildArchivalSection(input.archival)}

  <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 20px 0;">

  <p style="color: #9ca3af; font-size: 12px; margin: 0;">
    このメールは JapanStockDataPipeline の DB容量監視（db-archival ジョブ、毎週日曜03:00 JST）から自動送信されています。
  </p>
</body>
</html>
`;

  return { subject, html, usagePct };
}
