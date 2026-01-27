#!/usr/bin/env tsx
/**
 * 投資部門別売買状況 Seed スクリプト
 *
 * @description J-Quants APIから投資部門別売買状況を取得してDBに保存
 *
 * NOTE: 週次データのため、1年分でも約52週×8セクション程度のリクエスト数
 *
 * @example
 * ```
 * npm run seed:investors
 * npm run seed:investors -- --from 2024-01-01 --to 2024-12-31
 * ```
 */

import { loadEnv, parseArgs, createProgress, logResult, startTimer, type SeedResult } from './_shared';

async function main(): Promise<SeedResult> {
  loadEnv();

  const { from, to } = parseArgs();

  console.log('Starting Investor Types Seed');
  console.log(`  From: ${from}`);
  console.log(`  To:   ${to}`);

  const timer = startTimer();

  // 動的インポート（環境変数ロード後）
  const { syncInvestorTypes, SECTIONS } = await import(
    '../../src/lib/jquants/endpoints/investor-types'
  );

  let totalFetched = 0;
  let totalInserted = 0;
  const allErrors: Error[] = [];

  // セクションごとに取得（並列ではなく順次でレート制限対応）
  const progress = createProgress(SECTIONS.length, 'investor_types');

  for (const section of SECTIONS) {
    try {
      const result = await syncInvestorTypes({ from, to, section });
      totalFetched += result.fetched;
      totalInserted += result.inserted;
      allErrors.push(...result.errors);
      progress.increment(section);
    } catch (error) {
      // データがないセクションはスキップ
      if (error instanceof Error && error.message.includes('No data')) {
        progress.increment(`${section} (no data)`);
        continue;
      }
      allErrors.push(error instanceof Error ? error : new Error(String(error)));
      progress.increment(`${section} (error)`);
    }
  }

  progress.done();

  const seedResult: SeedResult = {
    name: 'Investor Types',
    fetched: totalFetched,
    inserted: totalInserted,
    errors: allErrors,
    durationMs: timer(),
  };

  logResult(seedResult);
  return seedResult;
}

// 直接実行時のみmain()を呼ぶ（import時は実行しない）
const isDirectRun = process.argv[1]?.endsWith('investor-types.ts') || process.argv[1]?.endsWith('investor-types');
if (isDirectRun) {
  main()
    .then((result) => {
      if (result.errors.length > 0) {
        process.exit(1);
      }
    })
    .catch((error) => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

export { main as seedInvestorTypes };
