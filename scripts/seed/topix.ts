#!/usr/bin/env tsx
/**
 * TOPIX Seed スクリプト
 *
 * @description J-Quants APIからTOPIX日足データを取得してDBに保存
 *
 * @example
 * ```
 * npm run seed:topix
 * npm run seed:topix -- --from 2024-01-01 --to 2024-12-31
 * ```
 */

import { loadEnv, parseArgs, createProgress, logResult, startTimer, type SeedResult } from './_shared';

async function main(): Promise<SeedResult> {
  loadEnv();

  const { from, to } = parseArgs();

  console.log('Starting TOPIX Seed');
  console.log(`  From: ${from}`);
  console.log(`  To:   ${to}`);

  const timer = startTimer();

  // 動的インポート（環境変数ロード後）
  const { syncTopixBarsDailyForRange } = await import('../../src/lib/jquants/endpoints/index-topix');

  // TOPIXは期間指定で一括取得可能
  const progress = createProgress(1, 'topix');

  try {
    const result = await syncTopixBarsDailyForRange(from, to);
    progress.done();

    const seedResult: SeedResult = {
      name: 'TOPIX',
      fetched: result.fetched,
      inserted: result.inserted,
      errors: result.errors,
      durationMs: timer(),
    };

    logResult(seedResult);
    return seedResult;
  } catch (error) {
    progress.done();
    console.error('Failed to sync TOPIX:', error);
    throw error;
  }
}

// 直接実行時のみmain()を呼ぶ（import時は実行しない）
const isDirectRun = process.argv[1]?.endsWith('topix.ts') || process.argv[1]?.endsWith('topix');
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

export { main as seedTopix };
