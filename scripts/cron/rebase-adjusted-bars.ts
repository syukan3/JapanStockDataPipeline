/**
 * adj_* 再基準化スクリプト（GH Actions用）
 *
 * @description
 * 株式分割・併合（adjustment_factor ≠ 1）の発生銘柄を検知し、対象銘柄の全履歴 adj_* を
 * SQL 関数 jquants_core.rebase_adjusted_bars で raw から再計算する。
 * Cron A の equity_bars 同期後・analytics 再計算（factor/technical/breadth）前に実行し、
 * 再基準化済みデータで指標が計算される順序を保証する。
 *
 * - 冪等（値が変わる行のみ UPDATE。分割の無い日は検知0件で即終了）
 * - job_runs / job_locks は使わない（refresh-technical.ts と同方針）
 *
 * 実行: npx tsx scripts/cron/rebase-adjusted-bars.ts [--date=YYYY-MM-DD] [--code=XXXXX[,YYYYY]] [--all] [--dry-run]
 *   引数なし        : 最新 trade_date の factor≠1 銘柄を検知して rebase
 *   --date=YYYY-MM-DD: 検知対象日を指定（検知モード専用）
 *   --code=X[,Y]    : 指定銘柄を強制 rebase
 *   --all           : 履歴に factor≠1 を一度でも持つ全銘柄を rebase（バックフィル用）
 *   --dry-run       : 検知結果と対象銘柄の列挙のみ（書き込みなし）
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import {
  parseRebaseArgs,
  getLatestTradeDate,
  detectEventsOnDate,
  detectEventsAll,
  uniqueCodes,
  rebaseCodes,
  type AdjustmentEvent,
} from '../../src/lib/analytics/rebase-adjusted-bars';

function validateEnv(): void {
  const required = ['NEXT_PUBLIC_SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

async function main(): Promise<void> {
  validateEnv();
  const logger = createLogger({ module: 'rebase-adjusted-bars' });
  const args = parseRebaseArgs(process.argv.slice(2));
  if (args.dryRun) logger.info('DRY RUN: 検知と対象列挙のみ。書き込みは行わない');

  const core = createAdminClient('jquants_core');

  // 1) 対象銘柄を決定
  let targetDate: string | undefined;
  let events: AdjustmentEvent[] = [];
  let codes: string[];

  if (args.mode === 'codes') {
    codes = args.codes ?? [];
    logger.info('Forced rebase codes', { mode: args.mode, codes });
  } else if (args.mode === 'all') {
    events = await detectEventsAll(core);
    codes = uniqueCodes(events);
    logger.info('Detected adjustment events (all history)', {
      mode: args.mode,
      events: events.length,
      codes: codes.length,
    });
  } else {
    targetDate = args.date ?? (await getLatestTradeDate(core));
    events = await detectEventsOnDate(core, targetDate);
    codes = uniqueCodes(events);
    logger.info('Detected adjustment events', {
      mode: args.mode,
      targetDate,
      events: events.length,
      codes: codes.length,
    });
  }

  for (const e of events) {
    logger.info('Adjustment event', {
      localCode: e.local_code,
      tradeDate: e.trade_date,
      factor: e.adjustment_factor,
    });
  }

  if (args.dryRun) {
    console.log(
      JSON.stringify({ dryRun: true, mode: args.mode, targetDate, targetCodes: codes.length, codes, events }, null, 2)
    );
    return;
  }

  if (codes.length === 0) {
    logger.info('No rebase targets; nothing to do', { mode: args.mode, targetDate });
    console.log(JSON.stringify({ success: true, mode: args.mode, targetDate, targetCodes: 0, totalUpdated: 0 }));
    return;
  }

  // 2) 銘柄ごとに全履歴を再基準化（値が変わる行のみ UPDATE され、更新行数が返る）
  const results = await rebaseCodes(core, codes);
  let totalUpdated = 0;
  for (const r of results) {
    totalUpdated += r.updated_rows;
    const factors = events.filter((e) => e.local_code === r.local_code).map((e) => e.adjustment_factor);
    logger.info('Rebased', { localCode: r.local_code, factors, updatedRows: r.updated_rows });
  }

  console.log(
    JSON.stringify({
      success: true,
      mode: args.mode,
      targetDate,
      targetCodes: codes.length,
      totalUpdated,
      results,
    })
  );
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    const logger = createLogger({ module: 'rebase-adjusted-bars' });
    logger.error('Script failed', { error: error instanceof Error ? error.message : String(error) });
    process.exit(1);
  });
