/**
 * app/api/notify/failure/route.ts のテスト
 */

import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  requireCronAuth: vi.fn(),
  sendWorkflowFailureEmail: vi.fn(),
}));

vi.mock('@/lib/cron/auth', () => ({
  requireCronAuth: mocks.requireCronAuth,
}));

vi.mock('@/lib/notification/email', () => ({
  sendWorkflowFailureEmail: mocks.sendWorkflowFailureEmail,
}));

vi.mock('@/lib/utils/logger', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
}));

import { POST } from '@/app/api/notify/failure/route';

function makeRequest(body: unknown): Request {
  return new Request('https://example.com/api/notify/failure', {
    method: 'POST',
    headers: { Authorization: 'Bearer test-secret', 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

describe('POST /api/notify/failure', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.requireCronAuth.mockReturnValue(null);
    mocks.sendWorkflowFailureEmail.mockResolvedValue(true);
  });

  it('CRON_SECRET認証に失敗した場合はそのままエラーレスポンスを返す', async () => {
    const authResponse = new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401 });
    mocks.requireCronAuth.mockReturnValue(authResponse);

    const response = await POST(makeRequest({ job: 'cron_b', workflow_run_id: '1' }));

    expect(response.status).toBe(401);
    expect(mocks.sendWorkflowFailureEmail).not.toHaveBeenCalled();
  });

  it('不正なJSONボディの場合は400を返す', async () => {
    const request = new Request('https://example.com/api/notify/failure', {
      method: 'POST',
      headers: { Authorization: 'Bearer test-secret' },
      body: '{invalid',
    });

    const response = await POST(request);

    expect(response.status).toBe(400);
    expect(mocks.sendWorkflowFailureEmail).not.toHaveBeenCalled();
  });

  it('JSONがnullの場合は400を返す', async () => {
    const response = await POST(makeRequest(null));

    expect(response.status).toBe(400);
    expect(mocks.sendWorkflowFailureEmail).not.toHaveBeenCalled();
  });

  it('JSONが配列の場合は400を返す', async () => {
    const response = await POST(makeRequest(['cron_b', '999']));

    expect(response.status).toBe(400);
    expect(mocks.sendWorkflowFailureEmail).not.toHaveBeenCalled();
  });

  it('workflow_run_idが数字以外の場合はnullとして扱う', async () => {
    const response = await POST(makeRequest({ job: 'cron_b', workflow_run_id: '123; DROP TABLE' }));
    const json = await response.json();

    expect(response.status).toBe(200);
    expect(json).toEqual({ received: true, notified: true });
    expect(mocks.sendWorkflowFailureEmail).toHaveBeenCalledWith(
      expect.objectContaining({ job: 'cron_b', workflowRunId: null })
    );
  });

  it('正常なリクエストでメール通知を送信し200を返す', async () => {
    const response = await POST(makeRequest({ job: 'cron_b', workflow_run_id: '999' }));
    const json = await response.json();

    expect(response.status).toBe(200);
    expect(json).toEqual({ received: true, notified: true });
    expect(mocks.sendWorkflowFailureEmail).toHaveBeenCalledWith(
      expect.objectContaining({ job: 'cron_b', workflowRunId: '999' })
    );
  });

  it('job/workflow_run_idが欠落していてもクラッシュせずデフォルト値で処理する', async () => {
    const response = await POST(makeRequest({}));
    const json = await response.json();

    expect(response.status).toBe(200);
    expect(json).toEqual({ received: true, notified: true });
    expect(mocks.sendWorkflowFailureEmail).toHaveBeenCalledWith(
      expect.objectContaining({ job: 'unknown', workflowRunId: null })
    );
  });

  it('メール送信に失敗してもレスポンスは200でnotified:falseを返す', async () => {
    mocks.sendWorkflowFailureEmail.mockResolvedValue(false);

    const response = await POST(makeRequest({ job: 'cron_a', workflow_run_id: '1' }));
    const json = await response.json();

    expect(response.status).toBe(200);
    expect(json).toEqual({ received: true, notified: false });
  });
});
