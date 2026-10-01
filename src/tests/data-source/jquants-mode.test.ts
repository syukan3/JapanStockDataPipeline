/**
 * data-source/jquants-mode.ts のユニットテスト
 *
 * setup.ts は既定で getJQuantsMode / assertJQuantsEnabled を「ON」に差し替えているので、
 * ここでは vi.importActual で実体を読み、Supabase クライアントだけを差し替える。
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const rpc = vi.hoisted(() => vi.fn());

vi.mock('@/lib/supabase/admin', () => ({
  createAdminClient: () => ({ rpc }),
}));

type ModeModule = typeof import('@/lib/data-source/jquants-mode');

async function loadActual(): Promise<ModeModule> {
  // setup.ts の importOriginal が実体を先に評価しており、そのとき本物の admin を掴んでいる。
  // モジュールキャッシュを捨てて、このファイルの admin モックで評価し直す。
  vi.resetModules();
  const mod = await vi.importActual<ModeModule>('@/lib/data-source/jquants-mode');
  mod.resetJQuantsModeCache();
  return mod;
}

const onRow = {
  enabled: true,
  last_official_trade_date: null,
  skipped_workflows: [],
  changed_at: '2026-10-01T00:00:00Z',
  reason: '初期値',
};

const offRow = {
  enabled: false,
  last_official_trade_date: '2026-09-30',
  skipped_workflows: ['cron-b.yml', 'cron-f.yml'],
  changed_at: '2026-10-02T00:00:00Z',
  reason: '解約',
};

describe('parseModeRows', () => {
  it('1行目をモードへ変換する', async () => {
    const { parseModeRows } = await loadActual();
    expect(parseModeRows([offRow])).toEqual({
      enabled: false,
      lastOfficialTradeDate: '2026-09-30',
      skippedWorkflows: ['cron-b.yml', 'cron-f.yml'],
      changedAt: '2026-10-02T00:00:00Z',
      reason: '解約',
    });
  });

  it('行が無いときは例外（ON/OFF を推測しない）', async () => {
    const { parseModeRows } = await loadActual();
    expect(() => parseModeRows([])).toThrow(/取得できません/);
    expect(() => parseModeRows(null)).toThrow(/取得できません/);
  });

  it('enabled が真偽値でないときは例外', async () => {
    const { parseModeRows } = await loadActual();
    expect(() => parseModeRows([{ ...onRow, enabled: 'false' }])).toThrow(/enabled が不正/);
  });

  it('skipped_workflows が null なら空配列', async () => {
    const { parseModeRows } = await loadActual();
    expect(parseModeRows([{ ...onRow, skipped_workflows: null }]).skippedWorkflows).toEqual([]);
  });
});

describe('getJQuantsMode / assertJQuantsEnabled', () => {
  beforeEach(() => {
    rpc.mockReset();
  });

  it('RPC を p_provider=jquants で呼ぶ', async () => {
    const { getJQuantsMode } = await loadActual();
    rpc.mockResolvedValue({ data: [onRow], error: null });
    const mode = await getJQuantsMode();
    expect(rpc).toHaveBeenCalledWith('get_data_source_mode', { p_provider: 'jquants' });
    expect(mode.enabled).toBe(true);
  });

  it('RPC エラーは例外', async () => {
    const { getJQuantsMode } = await loadActual();
    rpc.mockResolvedValue({ data: null, error: { message: 'permission denied' } });
    await expect(getJQuantsMode()).rejects.toThrow(/permission denied/);
  });

  it('キャッシュが効き、fresh で取り直す', async () => {
    const { getJQuantsMode } = await loadActual();
    rpc.mockResolvedValueOnce({ data: [onRow], error: null });
    rpc.mockResolvedValueOnce({ data: [offRow], error: null });
    expect((await getJQuantsMode()).enabled).toBe(true);
    expect((await getJQuantsMode()).enabled).toBe(true);
    expect(rpc).toHaveBeenCalledTimes(1);
    expect((await getJQuantsMode({ fresh: true })).enabled).toBe(false);
    expect(rpc).toHaveBeenCalledTimes(2);
  });

  it('OFF なら JQuantsDisabledError', async () => {
    const { assertJQuantsEnabled, JQuantsDisabledError } = await loadActual();
    rpc.mockResolvedValue({ data: [offRow], error: null });
    await expect(assertJQuantsEnabled('/equities/bars/daily')).rejects.toBeInstanceOf(JQuantsDisabledError);
  });

  it('ON なら何もしない', async () => {
    const { assertJQuantsEnabled } = await loadActual();
    rpc.mockResolvedValue({ data: [onRow], error: null });
    await expect(assertJQuantsEnabled()).resolves.toBeUndefined();
  });
});
