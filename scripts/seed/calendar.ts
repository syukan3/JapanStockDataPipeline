#!/usr/bin/env tsx
/**
 * 取引カレンダー Seed スクリプト
 *
 * @description J-Quants APIから取引カレンダーを取得してDBに保存
 *
 * @example
 * ```
 * npm run seed:calendar
 * npm run seed:calendar -- --from 2023-01-01 --to 2025-12-31
 * ```
 */

import { loadEnv, parseArgs, createProgress, logResult, startTimer, type SeedResult } from './_shared';

async function main(): Promise<SeedResult> {
  loadEnv();

  const { from, to } = parseArgs();

  console.log('Starting Trading Calendar Seed');
  console.log(`  From: ${from}`);
  console.log(`  To:   ${to}`);

  const timer = startTimer();

  // 動的インポート（環境変数ロード後）
  const { syncTradingCalendar } = await import('../../src/lib/jquants/endpoints/trading-calendar');

  // 取引カレンダーは1リクエストで全期間取得可能
  const progress = createProgress(1, 'calendar');

  try {
    const result = await syncTradingCalendar({ from, to });
    progress.done();

    const seedResult: SeedResult = {
      name: 'Trading Calendar',
      fetched: result.fetched,
      inserted: result.inserted,
      errors: result.errors,
      durationMs: timer(),
    };

    logResult(seedResult);
    return seedResult;
  } catch (error) {
    progress.done();
    console.error('Failed to sync trading calendar:', error);
    throw error;
  }
}

// 直接実行時のみmain()を呼ぶ（import時は実行しない）
const isDirectRun = process.argv[1]?.endsWith('calendar.ts') || process.argv[1]?.endsWith('calendar');
if (isDirectRun) {
  main()
    .then((result) => {
      if (result.errors.length > 0) {
        process.exit(1);
      }
    })
    .catch(() => {
      process.exit(1);
    });
}

export { main as seedCalendar };
