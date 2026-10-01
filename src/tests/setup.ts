import { vi, beforeEach, afterEach } from 'vitest';

// 環境変数
(process.env as Record<string, string>).NODE_ENV = 'test';

// J-Quants の ON/OFF スイッチ（00132）は DB を読むので、既定では ON として扱う。
// mockReset: true で実装が消えるため、beforeEach で毎回既定（ON）を入れ直す。
// OFF の挙動を見るテストは、そのテスト内で vi.mocked(getJQuantsMode).mockResolvedValue(...) 等で上書きする。
// スイッチ自体のテストは vi.importActual で実体を使う（src/tests/data-source/jquants-mode.test.ts）。
vi.mock('@/lib/data-source/jquants-mode', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/data-source/jquants-mode')>();
  return {
    ...actual,
    getJQuantsMode: vi.fn(),
    assertJQuantsEnabled: vi.fn(),
  };
});

const jquantsMode = await import('@/lib/data-source/jquants-mode');

beforeEach(() => {
  vi.mocked(jquantsMode.getJQuantsMode).mockResolvedValue({
    enabled: true,
    lastOfficialTradeDate: null,
    skippedWorkflows: [],
    changedAt: null,
    reason: null,
  });
  vi.mocked(jquantsMode.assertJQuantsEnabled).mockResolvedValue(undefined);
});

// 各テスト前にモックをクリア
beforeEach(() => {
  vi.clearAllMocks();
});

// 各テスト後にタイマーをリセット
afterEach(() => {
  vi.useRealTimers();
});

// console出力を抑制（エラー以外）
vi.spyOn(console, 'log').mockImplementation(() => {});
vi.spyOn(console, 'info').mockImplementation(() => {});
vi.spyOn(console, 'debug').mockImplementation(() => {});
