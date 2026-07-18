#!/usr/bin/env tsx
/**
 * 業種別空売り比率 Seed スクリプト（契約後バックフィル）
 *
 * @description
 * J-Quants /v2/markets/short-ratio から業種別空売り比率を期間分割で取得し
 * analytics.short_selling_sector に投入したうえで、全期間の市場全体2成分
 * （market_indicators.short_selling_ratio_restricted/unrestricted）を公式値で再計算する
 * （overwrite=true。過去の daily2 由来値は公式値で上書きする）。
 *
 * データ保持は10年（2008-11-05〜）。それ以前は取得対象外。
 *
 * @example
 * ```
 * npm run seed:short-ratio                              # 直近10年
 * npm run seed:short-ratio -- --from 2020-01-01 --to 2020-12-31
 * npm run seed:short-ratio -- --dry-run
 * ```
 */

import { loadEnv, startTimer } from './_shared';
import type { IndicatorRow } from '../../src/lib/market/indicators-sync';

/** short-ratio データの提供開始日（これ以前は取得対象外） */
const SHORT_RATIO_DATA_START = '2008-11-05';

interface Args {
  from: string;
  to: string;
  dryRun: boolean;
}

function formatJstDate(d: Date): string {
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Tokyo' }).format(d);
}

function parseSeedArgs(): Args {
  const argv = process.argv.slice(2);
  const now = new Date();
  const tenYearsAgo = new Date(now);
  tenYearsAgo.setFullYear(tenYearsAgo.getFullYear() - 10);
  let from = formatJstDate(tenYearsAgo);
  if (from < SHORT_RATIO_DATA_START) from = SHORT_RATIO_DATA_START;

  const args: Args = { from, to: formatJstDate(now), dryRun: argv.includes('--dry-run') };
  for (let i = 0; i < argv.length; i++) {
    const next = argv[i + 1];
    if (argv[i] === '--from' && next) args.from = next;
    if (argv[i] === '--to' && next) args.to = next;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(args.from) || !/^\d{4}-\d{2}-\d{2}$/.test(args.to)) {
    console.error('Invalid --from/--to format. Expected YYYY-MM-DD');
    process.exit(1);
  }
  if (args.from < SHORT_RATIO_DATA_START) args.from = SHORT_RATIO_DATA_START;
  if (args.from > args.to) {
    console.error(`--from (${args.from}) must be before or equal to --to (${args.to})`);
    process.exit(1);
  }
  return args;
}

/** [from, to] を暦年単位のウィンドウに分割（1リクエストが巨大化するのを避ける） */
function yearlyWindows(from: string, to: string): Array<{ from: string; to: string }> {
  const windows: Array<{ from: string; to: string }> = [];
  const startYear = Number(from.slice(0, 4));
  const endYear = Number(to.slice(0, 4));
  for (let y = startYear; y <= endYear; y++) {
    const wFrom = y === startYear ? from : `${y}-01-01`;
    const wTo = y === endYear ? to : `${y}-12-31`;
    windows.push({ from: wFrom, to: wTo });
  }
  return windows;
}

async function main(): Promise<void> {
  loadEnv();
  const args = parseSeedArgs();
  console.log('Starting Short Ratio Seed');
  console.log(`  From: ${args.from}`);
  console.log(`  To:   ${args.to}`);
  if (args.dryRun) console.log('  DRY RUN: DB書き込みなし');
  const timer = startTimer();

  // 動的インポート（環境変数ロード後）
  const { createAdminClient } = await import('../../src/lib/supabase/admin');
  const { syncShortRatio } = await import('../../src/lib/jquants/endpoints/short-ratio');
  const { fillShortSellingOfficial } = await import('../../src/lib/market/indicators-sync');

  const analytics = createAdminClient('analytics');
  const summary: Record<string, unknown> = {};
  const failures: string[] = [];

  // ---- 1) 業種別データを暦年ウィンドウで取得・投入 ----
  const windows = yearlyWindows(args.from, args.to);
  let totalFetched = 0;
  let totalInserted = 0;
  for (const w of windows) {
    if (args.dryRun) {
      // dryRun では short_selling_sector を変更しないため取得のみ行い件数を確認する
      const { fetchShortRatio } = await import('../../src/lib/jquants/endpoints/short-ratio');
      const { createJQuantsClient } = await import('../../src/lib/jquants/client');
      const items = await fetchShortRatio(createJQuantsClient(), { from: w.from, to: w.to });
      totalFetched += items.length;
      console.log(`  [${w.from}..${w.to}] fetched=${items.length} (dry-run: not written)`);
      continue;
    }
    try {
      const result = await syncShortRatio({ from: w.from, to: w.to, supabase: analytics });
      totalFetched += result.fetched;
      totalInserted += result.inserted;
      console.log(`  [${w.from}..${w.to}] fetched=${result.fetched} inserted=${result.inserted}`);
    } catch (e) {
      failures.push(`sector[${w.from}..${w.to}]: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  summary.sector = { fetched: totalFetched, inserted: totalInserted, windows: windows.length };

  // ---- 2) 全期間の市場全体2成分を公式値で再計算（daily2由来値を上書き）----
  try {
    const allDates = enumerateDates(args.from, args.to);
    const rowMap = new Map<string, IndicatorRow>();
    summary.official = await fillShortSellingOfficial(analytics, allDates, rowMap, args.dryRun, {
      overwrite: true,
    });
    console.log(`  official: ${JSON.stringify(summary.official)}`);
  } catch (e) {
    failures.push(`official: ${e instanceof Error ? e.message : String(e)}`);
  }

  summary.failures = failures;
  summary.durationMs = timer();
  console.log(JSON.stringify({ success: failures.length === 0, dryRun: args.dryRun, ...summary }, null, 2));
  if (failures.length > 0) process.exit(1);
}

/** [from, to] の暦日を昇順で列挙（fillShortSellingOfficial の走査対象。データのある日のみ書かれる） */
function enumerateDates(from: string, to: string): string[] {
  const out: string[] = [];
  const cur = new Date(`${from}T00:00:00Z`);
  const end = new Date(`${to}T00:00:00Z`);
  while (cur <= end) {
    out.push(cur.toISOString().slice(0, 10));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

const isDirectRun =
  process.argv[1]?.endsWith('short-ratio.ts') || process.argv[1]?.endsWith('short-ratio');
if (isDirectRun) {
  main().catch((error) => {
    console.error('Seed failed:', error);
    process.exit(1);
  });
}

export { main as seedShortRatio };
