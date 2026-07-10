/**
 * Workflow 失敗通知 API Route
 *
 * @description GitHub Actions の各 Cron ワークフロー（cron-a/b/c/d, db-archival）が
 * 本体ステップ失敗時（`if: failure()`）に呼び出すフォールバック通知。
 * このエンドポイント自身も本体と同じ VERCEL_URL / CRON_SECRET に依存するため、
 * CRON_SECRET不一致やVercel全体障害はここでも通知できない。カバーするのは、
 * ジョブ内部の失敗メール送信に到達する前にVercel関数がタイムアウト/クラッシュ
 * した場合や、db-archivalのようにGH Actionsランナー上のスクリプト単体では
 * 未捕捉の異常終了で内部メール送信自体に到達できなかった場合の安全網。
 *
 * POST /api/notify/failure
 * Body: { "job": string, "workflow_run_id": string }
 * Headers: Authorization: Bearer <CRON_SECRET>
 */

import { NextResponse } from 'next/server';
import { requireCronAuth } from '@/lib/cron/auth';
import { sendWorkflowFailureEmail } from '@/lib/notification/email';
import { createLogger } from '@/lib/utils/logger';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const maxDuration = 10;

const logger = createLogger({ module: 'route/notify-failure' });

interface NotifyFailureBody {
  job?: unknown;
  workflow_run_id?: unknown;
}

export async function POST(request: Request): Promise<Response> {
  // 1. CRON_SECRET 認証
  const authError = requireCronAuth(request);
  if (authError) {
    return authError;
  }

  // 2. リクエストボディのパース
  let parsed: unknown;
  try {
    parsed = await request.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  const body = parsed as NotifyFailureBody;
  const job = typeof body.job === 'string' && body.job ? body.job : 'unknown';
  // GitHub Actions の run_id は常に数字のみ。URL組み立て前に形式を絞る。
  const workflowRunId =
    typeof body.workflow_run_id === 'string' && /^\d+$/.test(body.workflow_run_id)
      ? body.workflow_run_id
      : null;

  logger.warn('Workflow step failure reported', { job, workflowRunId });

  // 3. メール通知（失敗してもレスポンスには影響させない）
  const notified = await sendWorkflowFailureEmail({
    job,
    workflowRunId,
    timestamp: new Date(),
  });

  return NextResponse.json({ received: true, notified });
}
