#!/usr/bin/env tsx
/**
 * 週足（追跡銘柄限定） Seed スクリプト
 *
 * @description
 * 保有 ∪ ウォッチリストの銘柄について、10年分の日足を J-Quants から取得し、
 * 週足へ集計して analytics.equity_bar_weekly に投入する。
 * **jquants_core.equity_bar_daily には書かない**（アーカイブ対象を膨らませないため）。
 *
 * 設計正本: ../../../docs/PLANS-longterm-log-chart-2026-08.md §4.2
 *
 * @example
 * ```
 * npm run seed:weekly-bars
 * npm run seed:weekly-bars -- --dry-run
 * npm run seed:weekly-bars -- --code=72030,285A0
 * ```
 */

import { loadEnv, createProgress, logResult, startTimer, type SeedResult } from './_shared';
// 取得期間の算出は Cron A の新規追跡銘柄バックフィル（scripts/cron/refresh-weekly-bars.ts）と共用する。
// weekly-bars.ts は依存ゼロの純関数モジュールなので loadEnv 前の静的 import でも副作用が無い。
import { BACKFILL_YEARS, getBackfillRange } from '../../src/lib/analytics/weekly-bars';

export { BACKFILL_YEARS, getBackfillRange };

export interface WeeklyBarsSeedArgs {
  /** --code で指定された対象銘柄（未指定なら追跡銘柄全件） */
  codes?: string[];
  /** 対象銘柄と取得予定期間の列挙のみ（API 呼び出し・DB書込なし） */
  dryRun: boolean;
}

/**
 * CLI 引数をパースする（process.argv.slice(2) を渡す）
 */
export function parseWeeklyBarsSeedArgs(argv: string[]): WeeklyBarsSeedArgs {
  let codes: string[] | undefined;
  let dryRun = false;

  for (const arg of argv) {
    if (arg === '--dry-run') {
      dryRun = true;
    } else if (arg.startsWith('--code=')) {
      const values = arg
        .slice('--code='.length)
        .split(',')
        .map((c) => c.trim().toUpperCase())
        .filter((c) => c.length > 0);
      if (values.length === 0) {
        throw new Error('--code に銘柄コードが指定されていません（例: --code=72030,285A0）');
      }
      codes = [...new Set(values)];
    } else {
      throw new Error(`不明な引数です: ${arg}`);
    }
  }

  return { codes, dryRun };
}

async function main(): Promise<SeedResult> {
  loadEnv();

  const args = parseWeeklyBarsSeedArgs(process.argv.slice(2));

  // 動的インポート（環境変数ロード後）
  const { getJSTDate } = await import('../../src/lib/utils/date');
  const { getTrackedLocalCodes } = await import('../../src/lib/analytics/tracked-codes');
  const { backfillWeeklyBarsForCode } = await import('../../src/lib/analytics/weekly-bars-backfill');

  const { from, to } = getBackfillRange(getJSTDate());

  console.log('Starting Weekly Bars Seed (tracked codes only)');
  console.log(`  From: ${from}`);
  console.log(`  To:   ${to}`);

  const timer = startTimer();

  const codes = args.codes ?? (await getTrackedLocalCodes());
  console.log(`  Codes: ${codes.length}${args.codes ? ' (--code)' : ' (holdings + watchlist)'}`);

  if (args.dryRun) {
    console.log('\n[dry-run] 対象銘柄と取得予定期間（API・DB書込なし）');
    for (const code of codes) {
      console.log(`  - ${code}: ${from} .. ${to}`);
    }
    const dryResult: SeedResult = {
      name: 'Weekly Bars (dry-run)',
      fetched: 0,
      inserted: 0,
      errors: [],
      durationMs: timer(),
    };
    logResult(dryResult);
    return dryResult;
  }

  let totalFetched = 0;
  let totalInserted = 0;
  const allErrors: Error[] = [];

  const progress = createProgress(codes.length, 'weekly_bars');

  // 銘柄単位で順次処理（60req/min のレート制御は J-Quants クライアント側のトークンバケット）
  for (const code of codes) {
    try {
      const result = await backfillWeeklyBarsForCode(code, from, to);
      totalFetched += result.fetched;
      totalInserted += result.upserted;
      progress.increment(`${code} (${result.weeks}w)`);
    } catch (error) {
      allErrors.push(
        error instanceof Error
          ? new Error(`${code}: ${error.message}`)
          : new Error(`${code}: ${String(error)}`)
      );
      progress.increment(`${code} (error)`);
    }
  }

  progress.done();

  const seedResult: SeedResult = {
    name: 'Weekly Bars',
    fetched: totalFetched,
    inserted: totalInserted,
    errors: allErrors,
    durationMs: timer(),
  };

  logResult(seedResult);
  return seedResult;
}

// 直接実行時のみmain()を呼ぶ（import時は実行しない）
const isDirectRun =
  process.argv[1]?.endsWith('seed-weekly-bars.ts') || process.argv[1]?.endsWith('seed-weekly-bars');
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

export { main as seedWeeklyBars };
