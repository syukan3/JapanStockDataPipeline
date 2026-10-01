/**
 * J-Quants ON 復帰時の復元: OFF 期間の代替データを公式データへ置き換える
 *
 * @description
 * `scripts/ops/jquants-toggle.sh on` の直後に、**OFF 期間の長さにかかわらず必ず**実行する。
 * 通常の Cron A の前方フィルは各テーブルの max(trade_date) から先しか取らないので、
 * - TOPIX は推計行が今日まであるため、OFF 期間が取り直されない
 * - 投資部門別は、JPX の規則で決めた公表日と J-Quants の公表日がずれる週で二重行が残る
 * - 市場指標の騰落レシオ等は nikkei225jp の値が入っているため、自前計算で埋め直されない
 * これらを消してから、期間を指定して取り直し、派生指標を再計算する。
 * 設計正本: ../../../docs/PLANS-jquants-off-switch-2026-10.md §8
 *
 * 実行: npx tsx scripts/ops/jquants-restore.ts [--dry-run] [--from YYYY-MM-DD]
 *   --from を省略すると ops.data_source_switches.last_official_trade_date を起点にする。
 *   途中で失敗したら、そこで止まる。同じコマンドを再実行してよい（各ステップは冪等）。
 */

import { config } from 'dotenv';
import { resolve } from 'path';
import { spawnSync } from 'child_process';
import { createAdminClient } from '../../src/lib/supabase/admin';
import { getJQuantsMode } from '../../src/lib/data-source/jquants-mode';
import { addDays, getJSTDate } from '../../src/lib/utils/date';

config({ path: resolve(process.cwd(), '.env.local') });

function parseFrom(): string | null {
  const i = process.argv.indexOf('--from');
  if (i < 0) return null;
  const v = process.argv[i + 1];
  if (!v || !/^\d{4}-\d{2}-\d{2}$/.test(v)) throw new Error('--from は YYYY-MM-DD で指定してください');
  return v;
}

function run(label: string, args: string[], dryRun: boolean): void {
  console.log(`\n=== ${label} ===\n$ npx tsx ${args.join(' ')}`);
  if (dryRun) return;
  const r = spawnSync('npx', ['tsx', ...args], { stdio: 'inherit', env: process.env });
  if (r.status !== 0) {
    throw new Error(`${label} が失敗しました（exit ${r.status}）。原因を直してから同じコマンドを再実行してください`);
  }
}

async function countRows(
  client: ReturnType<typeof createAdminClient>,
  table: string,
  build: (q: any) => any // eslint-disable-line @typescript-eslint/no-explicit-any
): Promise<number> {
  const { count, error } = await build(client.from(table).select('*', { count: 'exact', head: true }));
  if (error) throw new Error(`${table} の件数取得に失敗しました: ${error.message}`);
  return count ?? 0;
}

async function main(): Promise<void> {
  const dryRun = process.argv.includes('--dry-run');
  const mode = await getJQuantsMode({ fresh: true });
  if (!mode.enabled) {
    throw new Error('J-Quants が OFF のままです。先に scripts/ops/jquants-toggle.sh on を実行してください');
  }
  const from = parseFrom() ?? mode.lastOfficialTradeDate;
  if (!from) {
    throw new Error('起点日が分かりません（last_official_trade_date が空）。--from YYYY-MM-DD を指定してください');
  }
  const today = getJSTDate();
  const core = createAdminClient('jquants_core');
  const analytics = createAdminClient('analytics');

  console.log(`J-Quants 復元: ${from} 〜 ${today}${dryRun ? '（dry-run: 件数の確認とコマンドの表示のみ）' : ''}`);

  // 1) 代替データの件数
  const proxyTopix = await countRows(core, 'topix_bar_daily', (q) => q.neq('source', 'jquants'));
  const jpxRows = await countRows(core, 'investor_type_trading', (q) => q.eq('source', 'jpx'));
  const extBreadth = await countRows(analytics, 'market_indicators', (q) => q.eq('breadth_source', 'nikkei225jp'));
  const derivedAfter = await countRows(analytics, 'market_indicators', (q) => q.gt('as_of_date', from).not('topix_close', 'is', null));
  console.log({ proxyTopix, jpxRows, extBreadth, derivedTopixAfterFrom: derivedAfter });

  if (!dryRun) {
    // 2) 代替データを消す（公式値で取り直すため）
    let r = await core.from('topix_bar_daily').delete().neq('source', 'jquants');
    if (r.error) throw new Error(`TOPIX 推計行の削除に失敗しました: ${r.error.message}`);
    r = await core.from('investor_type_trading').delete().eq('source', 'jpx');
    if (r.error) throw new Error(`JPX 行の削除に失敗しました: ${r.error.message}`);
    r = await analytics
      .from('market_indicators')
      .update({ adv_dec_ratio_25d: null, new_highs: null, new_lows: null, breadth_source: null })
      .eq('breadth_source', 'nikkei225jp');
    if (r.error) throw new Error(`市場指標の代替 breadth の消去に失敗しました: ${r.error.message}`);
    // TOPIX 終値・NT倍率は推計 TOPIX から作られているので、公式 TOPIX で作り直させる
    r = await analytics.from('market_indicators').update({ topix_close: null, nt_ratio: null }).gt('as_of_date', from);
    if (r.error) throw new Error(`市場指標の TOPIX 系列の消去に失敗しました: ${r.error.message}`);
  }

  // 3) 期間を指定して取り直す（公式の同期は source='jquants' を明示的に書く）
  const range = ['--from', from, '--to', today];
  run('営業日カレンダー（生成行を公式行で上書き）', ['scripts/seed/calendar.ts', '--from', from, '--to', addDays(today, 370)], dryRun);
  run('TOPIX', ['scripts/seed/topix.ts', ...range], dryRun);
  run('投資部門別', ['scripts/seed/investor-types.ts', ...range], dryRun);
  run('株価日足', ['scripts/seed/equity-bars.ts', ...range], dryRun);
  run('財務', ['scripts/seed/financial.ts', ...range], dryRun);
  run('銘柄マスタ', ['scripts/seed/equity-master.ts', '--from', today, '--to', today], dryRun);
  run('銘柄別信用残（週次・全銘柄1年窓）', ['scripts/seed/weekly-margin-interest.ts', '--mode=universe', ...range], dryRun);
  run('業種別空売り比率', ['scripts/seed/short-ratio.ts', ...range], dryRun);

  // 4) 派生指標の再計算
  run('分割の再基準化', ['scripts/cron/rebase-adjusted-bars.ts'], dryRun);
  console.log('\n=== refresh_stock_metrics（RPC） ===');
  if (!dryRun) {
    const { error } = await analytics.rpc('refresh_stock_metrics');
    if (error) throw new Error(`refresh_stock_metrics に失敗しました: ${error.message}`);
  }
  run('テクニカル指標', ['scripts/cron/refresh-technical.ts'], dryRun);
  run('長期週足（OFF 中に追加したウォッチ銘柄の10年分もここで埋まる）', ['scripts/cron/refresh-weekly-bars.ts'], dryRun);
  run('バスケット', ['scripts/cron/refresh-basket-metrics.ts'], dryRun);
  run('市場指標（全期間の NULL を埋め直す）', ['scripts/cron/refresh-market-indicators.ts', '--full'], dryRun);

  if (dryRun) return;

  // 5) 検証（満たさなければ exit 1）
  const leftTopix = await countRows(core, 'topix_bar_daily', (q) => q.neq('source', 'jquants'));
  const leftJpx = await countRows(core, 'investor_type_trading', (q) => q.eq('source', 'jpx'));
  const leftExt = await countRows(analytics, 'market_indicators', (q) => q.eq('breadth_source', 'nikkei225jp'));
  const { data: latest } = await core
    .from('equity_bar_daily')
    .select('trade_date')
    .eq('session', 'DAY')
    .order('trade_date', { ascending: false })
    .limit(1);
  const latestBar = (latest?.[0] as { trade_date: string } | undefined)?.trade_date ?? null;
  const { data: cal } = await core
    .from('trading_calendar')
    .select('calendar_date')
    .eq('is_business_day', true)
    .lte('calendar_date', today)
    .order('calendar_date', { ascending: false })
    .limit(2);
  const businessDays = ((cal ?? []) as Array<{ calendar_date: string }>).map((c) => c.calendar_date);
  const problems: string[] = [];
  if (leftTopix > 0) problems.push(`TOPIX の代替行が ${leftTopix} 行残っている`);
  if (leftJpx > 0) problems.push(`投資部門別の JPX 行が ${leftJpx} 行残っている`);
  if (leftExt > 0) problems.push(`市場指標の nikkei225jp 由来 breadth が ${leftExt} 行残っている`);
  // 当日の株価は18時台まで来ないので、直近2営業日のどちらかなら可
  if (!latestBar || !businessDays.includes(latestBar)) {
    problems.push(`株価の最新日が ${latestBar ?? 'なし'}（直近の営業日 ${businessDays.join(' / ')} ではない）`);
  }
  console.log({ leftTopix, leftJpx, leftExt, latestBar });
  if (problems.length > 0) {
    throw new Error(`復元の検証に失敗しました: ${problems.join(' / ')}`);
  }
  console.log('\n✅ 復元が完了しました。supabase-data-check スキルで各テーブルの最新日も確認してください。');
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
