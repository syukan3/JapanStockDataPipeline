import { vi, beforeEach, afterEach } from 'vitest';

// 環境変数
(process.env as Record<string, string>).NODE_ENV = 'test';

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
