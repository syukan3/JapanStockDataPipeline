import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    // 環境: Node.js（ブラウザAPIは不使用）
    environment: 'node',

    // グローバルAPI有効（describe, it, expect をimport不要に）
    globals: true,

    // テストファイルパターン
    include: ['src/tests/**/*.test.ts'],

    // セットアップファイル
    setupFiles: ['./src/tests/setup.ts'],

    // タイムアウト（ms）
    testTimeout: 10000,

    // モック設定
    mockReset: true,
    restoreMocks: true,

    // カバレッジ
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: [
        // utils
        'src/lib/utils/date.ts',
        'src/lib/utils/html.ts',
        'src/lib/utils/retry.ts',
        'src/lib/utils/logger.ts',
        'src/lib/utils/batch.ts',
        // jquants
        'src/lib/jquants/rate-limiter.ts',
        'src/lib/jquants/client.ts',
        // supabase
        'src/lib/supabase/errors.ts',
        // cron
        'src/lib/cron/auth.ts',
        'src/lib/cron/job-run.ts',
        'src/lib/cron/job-lock.ts',
        'src/lib/cron/heartbeat.ts',
        'src/lib/cron/business-day.ts',
        // notification
        'src/lib/notification/email.ts',
        'src/lib/notification/templates.ts',
        // cron handlers
        'src/lib/cron/catch-up.ts',
        'src/lib/cron/handlers/cron-a.ts',
        'src/lib/cron/handlers/cron-b.ts',
        'src/lib/cron/handlers/cron-c.ts',
        'src/lib/cron/handlers/cron-a-chunk.ts',
        'src/lib/cron/handlers/macro.ts',
        // jquants endpoints
        'src/lib/jquants/endpoints/equity-master.ts',
        'src/lib/jquants/endpoints/equity-bars-daily.ts',
        // external API clients
        'src/lib/fred/client.ts',
        'src/lib/fred/series-config.ts',
        'src/lib/estat/client.ts',
      ],
      thresholds: {
        statements: 80,
        branches: 80,
        functions: 80,
        lines: 80,
      },
    },
  },

  // パスエイリアス
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
});
