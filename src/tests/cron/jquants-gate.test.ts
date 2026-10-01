/**
 * cron/jquants-gate.ts のユニットテスト（J-Quants 系ルートの OFF ゲート）
 */

import { describe, it, expect, vi } from 'vitest';
import { jquantsGate } from '@/lib/cron/jquants-gate';
import { getJQuantsMode } from '@/lib/data-source/jquants-mode';

const offMode = {
  enabled: false,
  lastOfficialTradeDate: '2026-09-30',
  skippedWorkflows: ['cron-b.yml'],
  changedAt: null,
  reason: '解約',
};

describe('jquantsGate', () => {
  it('ON なら null（ルートはそのまま続行）', async () => {
    await expect(jquantsGate('/api/cron/jquants/a')).resolves.toBeNull();
  });

  it('OFF なら 200 skipped を返す', async () => {
    vi.mocked(getJQuantsMode).mockResolvedValue(offMode);
    const res = await jquantsGate('/api/cron/jquants/b');
    expect(res).not.toBeNull();
    expect(res!.status).toBe(200);
    expect(await res!.json()).toEqual({ skipped: 'jquants_off', route: '/api/cron/jquants/b' });
  });

  it('モードが読めないときは 503（J-Quants を呼ばない）', async () => {
    vi.mocked(getJQuantsMode).mockRejectedValue(new Error('rpc down'));
    const res = await jquantsGate('/api/cron/jquants/c');
    expect(res!.status).toBe(503);
    expect((await res!.json()).detail).toContain('rpc down');
  });
});
