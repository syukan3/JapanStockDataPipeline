#!/usr/bin/env tsx
/**
 * 全データセット一括 Seed スクリプト
 *
 * @description J-Quants APIから全データセットを取得してDBに保存
 *
 * 実行順序（依存関係考慮）:
 * 1. trading_calendar - 営業日判定に必要
 * 2. equity_master - 銘柄マスタ
 * 3. topix - TOPIX
 * 4. equity_bars - 株価（最も時間がかかる）
 * 5. financial - 財務データ
 * 6. earnings - 決算発表予定
 * 7. investors - 投資部門別
 *
 * @example
 * ```
 * npm run seed:all
 * npm run seed:all -- --from 2024-01-01 --to 2024-12-31
 * npm run seed:all -- --days 30
 * ```
 */

import {
  loadEnv,
  parseArgs,
  logSummary,
  runSeedTask,
  type SeedTask,
  type SeedResult,
} from './_shared';

// 各seedスクリプトをimport
import { seedCalendar } from './calendar';
import { seedEquityMaster } from './equity-master';
import { seedTopix } from './topix';
import { seedEquityBars } from './equity-bars';
import { seedFinancial } from './financial';
import { seedEarnings } from './earnings';
import { seedInvestorTypes } from './investor-types';

async function main(): Promise<void> {
  loadEnv();

  const { from, to } = parseArgs();

  console.log('========================================');
  console.log('SEED ALL DATASETS');
  console.log('========================================');
  console.log(`  From: ${from}`);
  console.log(`  To:   ${to}`);
  console.log('========================================\n');

  // タスク定義（依存関係順）
  const tasks: SeedTask[] = [
    { name: 'Trading Calendar', fn: seedCalendar },
    { name: 'Equity Master', fn: seedEquityMaster },
    { name: 'TOPIX', fn: seedTopix },
    { name: 'Equity Bars Daily', fn: seedEquityBars },
    { name: 'Financial Summary', fn: seedFinancial },
    { name: 'Earnings Calendar', fn: seedEarnings },
    { name: 'Investor Types', fn: seedInvestorTypes },
  ];

  // 順次実行
  const results: SeedResult[] = [];
  for (let i = 0; i < tasks.length; i++) {
    const result = await runSeedTask(tasks[i], i, tasks.length);
    results.push(result);
  }

  // サマリー出力
  logSummary(results);

  // エラーがあった場合は終了コード1
  const hasErrors = results.some((r) => r.errors.length > 0);
  if (hasErrors) {
    process.exit(1);
  }
}

// 直接実行時のみmain()を呼ぶ（import時は実行しない）
const isDirectRun = process.argv[1]?.endsWith('index.ts') || process.argv[1]?.endsWith('seed/index');
if (isDirectRun) {
  main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}
