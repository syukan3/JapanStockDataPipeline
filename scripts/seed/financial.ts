#!/usr/bin/env tsx
/**
 * 財務データ Seed スクリプト
 *
 * @description J-Quants APIから財務サマリーを取得してDBに保存
 *
 * 戦略: 日付単位で取得（1日分の開示を1リクエスト）
 *
 * @example
 * ```
 * npm run seed:financial
 * npm run seed:financial -- --from 2024-01-01 --to 2024-12-31
 * ```
 */

import {
  loadEnv,
  parseArgs,
  createProgress,
  logResult,
  startTimer,
  getBusinessDays,
  type SeedResult,
} from './_shared';

async function main(): Promise<SeedResult> {
  loadEnv();

  const { from, to } = parseArgs();

  console.log('Starting Financial Summary Seed');
  console.log(`  From: ${from}`);
  console.log(`  To:   ${to}`);

  const timer = startTimer();

  // 動的インポート（環境変数ロード後）
  const { syncFinancialSummaryForDate } = await import(
    '../../src/lib/jquants/endpoints/fins-summary'
  );

  // 営業日リストを取得（財務データは営業日のみ開示される）
  console.log('Fetching business days from DB...');
  let businessDays: string[];

  try {
    businessDays = await getBusinessDays(from, to);
  } catch (error) {
    // 取引カレンダーがDBにない場合は、全日付で試行
    console.warn('Failed to fetch business days from DB:', error instanceof Error ? error.message : error);
    console.log('Falling back to all dates in range.');
    const { generateDateRange } = await import('../../src/lib/utils/date');
    businessDays = generateDateRange(from, to);
  }

  console.log(`  Business days: ${businessDays.length}`);

  let totalFetched = 0;
  let totalInserted = 0;
  const allErrors: Error[] = [];

  const progress = createProgress(businessDays.length, 'financial');

  // 日付単位で順次処理
  for (const date of businessDays) {
    try {
      const result = await syncFinancialSummaryForDate(date);
      totalFetched += result.fetched;
      totalInserted += result.inserted;
      allErrors.push(...result.errors);
      progress.increment(date);
    } catch (error) {
      // データがない日はスキップ
      if (error instanceof Error && error.message.includes('No data')) {
        progress.increment(`${date} (no data)`);
        continue;
      }
      allErrors.push(error instanceof Error ? error : new Error(String(error)));
      progress.increment(`${date} (error)`);
    }
  }

  progress.done();

  const seedResult: SeedResult = {
    name: 'Financial Summary',
    fetched: totalFetched,
    inserted: totalInserted,
    errors: allErrors,
    durationMs: timer(),
  };

  logResult(seedResult);
  return seedResult;
}

// 直接実行時のみmain()を呼ぶ（import時は実行しない）
const isDirectRun = process.argv[1]?.endsWith('financial.ts') || process.argv[1]?.endsWith('financial');
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

export { main as seedFinancial };
