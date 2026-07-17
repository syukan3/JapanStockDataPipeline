#!/usr/bin/env tsx
/**
 * 信用取引週末残高 Seed スクリプト（J-Quants Standard 契約後に実行）
 *
 * @description J-Quants APIから信用取引週末残高を取得してDBに保存
 * - --mode=universe:  直近1年を週末営業日ごとに date= 指定で全銘柄取得（デフォルト）
 * - --mode=protected: 保護リスト銘柄（保有+ウォッチ）を code= 指定で全期間取得（〜10年）
 *
 * @example
 * ```
 * npm run seed:weekly-margin -- --mode=universe
 * npm run seed:weekly-margin -- --mode=protected
 * npm run seed:weekly-margin -- --mode=protected --codes 13010,57130
 * npm run seed:weekly-margin -- --mode=universe --from 2025-01-01 --to 2025-12-31 --dry-run
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

const MODES = ['universe', 'protected'] as const;
type SeedMode = typeof MODES[number];

interface ExtraOptions {
  mode: SeedMode;
  codes?: string[];
  dryRun: boolean;
  /** --from/--to が明示指定されたか（protected モードは未指定なら全期間） */
  hasExplicitRange: boolean;
}

function parseExtraArgs(): ExtraOptions {
  const args = process.argv.slice(2);
  const options: ExtraOptions = {
    mode: 'universe',
    dryRun: false,
    hasExplicitRange: args.includes('--from') || args.includes('--to') || args.includes('--days'),
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const nextArg = args[i + 1];

    if (arg.startsWith('--mode=')) {
      const value = arg.split('=')[1];
      if (!(MODES as readonly string[]).includes(value)) {
        console.error(`Invalid --mode: ${value}. Valid values: ${MODES.join(', ')}`);
        process.exit(1);
      }
      options.mode = value as SeedMode;
    } else if (arg === '--codes' && nextArg) {
      options.codes = nextArg.split(',').map((c) => c.trim()).filter((c) => c.length > 0);
      i++;
    } else if (arg === '--dry-run') {
      options.dryRun = true;
    }
  }

  return options;
}

/**
 * 営業日リストから週末営業日（カレンダー週 月〜日 の最終営業日）を抽出
 */
export function toWeekEndDates(businessDays: string[]): string[] {
  const lastByWeek = new Map<string, string>();

  for (const day of businessDays) {
    const [year, month, dayOfMonth] = day.split('-').map(Number);
    const date = new Date(Date.UTC(year, month - 1, dayOfMonth));
    // 週の月曜日をキーにする（日曜=0 を週末尾に回す）
    const offset = (date.getUTCDay() + 6) % 7;
    date.setUTCDate(date.getUTCDate() - offset);
    const weekKey = date.toISOString().slice(0, 10);

    const current = lastByWeek.get(weekKey);
    if (!current || day > current) {
      lastByWeek.set(weekKey, day);
    }
  }

  return [...lastByWeek.values()].sort();
}

async function main(): Promise<SeedResult> {
  loadEnv();

  const { from, to } = parseArgs();
  const { mode, codes, dryRun, hasExplicitRange } = parseExtraArgs();

  console.log('Starting Weekly Margin Interest Seed');
  console.log(`  Mode:    ${mode}`);
  if (mode === 'universe' || hasExplicitRange) {
    console.log(`  From:    ${from}`);
    console.log(`  To:      ${to}`);
  } else {
    console.log('  Range:   full history (~10 years)');
  }
  if (codes) {
    console.log(`  Codes:   ${codes.join(', ')}`);
  }
  if (dryRun) {
    console.log('  Dry run: no DB writes');
  }

  const timer = startTimer();

  // 動的インポート（環境変数ロード後）
  const { syncWeeklyMarginInterest, fetchProtectedLocalCodes } = await import(
    '../../src/lib/jquants/endpoints/weekly-margin-interest'
  );

  let totalFetched = 0;
  let totalInserted = 0;
  const allErrors: Error[] = [];

  if (mode === 'universe') {
    // 週末営業日ごとに date= 指定で全銘柄取得（週次データのため約52リクエスト/年）
    const businessDays = await getBusinessDays(from, to);
    const weekEndDates = toWeekEndDates(businessDays);
    const progress = createProgress(weekEndDates.length, 'weekly_margin_interest');

    for (const date of weekEndDates) {
      try {
        const result = await syncWeeklyMarginInterest({ date }, { dryRun });
        totalFetched += result.fetched;
        totalInserted += result.inserted;
        allErrors.push(...result.errors);
        progress.increment(date);
      } catch (error) {
        allErrors.push(error instanceof Error ? error : new Error(String(error)));
        progress.increment(`${date} (error)`);
      }
    }

    progress.done();
  } else {
    // 保護リスト銘柄を code= 指定で取得（未指定期間は全期間が返る）
    const targetCodes = codes ?? (await fetchProtectedLocalCodes());
    if (targetCodes.length === 0) {
      console.log('No protected codes found. Nothing to seed.');
    }
    const progress = createProgress(targetCodes.length, 'weekly_margin_interest');

    for (const code of targetCodes) {
      try {
        const params = hasExplicitRange ? { code, from, to } : { code };
        const result = await syncWeeklyMarginInterest(params, { dryRun });
        totalFetched += result.fetched;
        totalInserted += result.inserted;
        allErrors.push(...result.errors);
        progress.increment(code);
      } catch (error) {
        allErrors.push(error instanceof Error ? error : new Error(String(error)));
        progress.increment(`${code} (error)`);
      }
    }

    progress.done();
  }

  const seedResult: SeedResult = {
    name: 'Weekly Margin Interest',
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
  process.argv[1]?.endsWith('weekly-margin-interest.ts') ||
  process.argv[1]?.endsWith('weekly-margin-interest');
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

export { main as seedWeeklyMarginInterest };
