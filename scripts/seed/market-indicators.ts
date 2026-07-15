#!/usr/bin/env tsx
/**
 * 市場全体指標 5年バックフィルスクリプト
 *
 * @description
 * analytics.market_indicators へ SERIES_START(2021-07-01) 以降の履歴を投入する。
 *
 * データソース:
 * - breadth系（騰落数/騰落レシオ/新高値新安値/売買代金/SMA上回り比率pct_above_sma25・200）:
 *   本番DBの equity_bar_daily は 2025-01-27 以降しか無いため、
 *   Scouter リポジトリの backtest.sqlite（2021-02-08〜、adj系・turnover_value・
 *   月次時点ユニバース equity_master 保持）から計算する。
 *   sqlite 収録最終日以降の端数は日次 refresh-market-indicators.ts が DB から埋める。
 * - external系（日経終値/PER/空売り比率/信用評価損益率）: Yahoo + nikkei225jp.com
 * - derived系（EPS/TOPIX/NT倍率）: 先に `npm run seed:topix -- --from 2021-07-01` で
 *   jquants_core.topix_bar_daily を5年化しておくこと（TOPIX欠損分は NT が NULL のまま残り、
 *   後から seed を再実行すれば埋まる）。
 *
 * 制限（計画書 docs/PLANS-market-indicators.md 参照）:
 * - 2021-07-01〜2022-03-31 の新高値/新安値は基準期間先頭（2021-01-01〜02-07）が
 *   sqlite に無いため近似値
 * - ユニバースは月次スナップショットの直近値（市場再編月等で公式値と数銘柄ズレうる）
 *
 * 実行:
 *   npx tsx scripts/seed/market-indicators.ts [--from 2021-07-01] [--to YYYY-MM-DD]
 *     [--sqlite ../JapanStockScouter/data/backtest.sqlite] [--dry-run]
 *
 * 再seed（ソース破損時のリカバリ）:
 *   delete from analytics.market_indicators where as_of_date between '..' and '..';
 *   の後に本スクリプトを再実行（全列が再生成可能）。
 */

import { resolve } from 'path';
import { loadEnv, startTimer } from './_shared';
import { addDays } from '../../src/lib/utils/date';
import {
  BreadthAccumulator,
  computeAdvDecRatio25,
  includesPreviousYear,
  isPctSma25Pending,
  isPctSma200Pending,
  PRIME_MARKET_CODES,
  SMA_LONG_WARMUP_CALENDAR_DAYS,
  type BreadthBar,
} from '../../src/lib/analytics/market-breadth';

/** breadth の公開最低カバレッジ（当日バーのあるプライム銘柄割合） */
const MIN_COVERAGE = 0.8;

interface Args {
  from: string;
  to: string;
  sqlitePath: string;
  dryRun: boolean;
}

function parseSeedArgs(seriesStart: string): Args {
  const argv = process.argv.slice(2);
  const today = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Tokyo' }).format(new Date());
  const args: Args = {
    from: seriesStart,
    to: today,
    sqlitePath: resolve(process.cwd(), '../JapanStockScouter/data/backtest.sqlite'),
    dryRun: argv.includes('--dry-run'),
  };
  for (let i = 0; i < argv.length; i++) {
    const next = argv[i + 1];
    if (argv[i] === '--from' && next) args.from = next;
    if (argv[i] === '--to' && next) args.to = next;
    if (argv[i] === '--sqlite' && next) args.sqlitePath = resolve(process.cwd(), next);
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(args.from) || !/^\d{4}-\d{2}-\d{2}$/.test(args.to)) {
    console.error('Invalid --from/--to format. Expected YYYY-MM-DD');
    process.exit(1);
  }
  return args;
}

async function main(): Promise<void> {
  loadEnv();
  const { SERIES_START } = await import('../../src/lib/market/indicators-sync');
  const args = parseSeedArgs(SERIES_START);
  console.log('Starting Market Indicators Seed');
  console.log(`  From:   ${args.from}`);
  console.log(`  To:     ${args.to}`);
  console.log(`  Sqlite: ${args.sqlitePath}`);
  if (args.dryRun) console.log('  DRY RUN: DB書き込みなし');
  const timer = startTimer();

  // 動的インポート（環境変数ロード後）
  const { createAdminClient } = await import('../../src/lib/supabase/admin');
  const sync = await import('../../src/lib/market/indicators-sync');
  const { fetchNikkei225jpDaily } = await import('../../src/lib/market/nikkei225jp-client');

  const core = createAdminClient('jquants_core');
  const analytics = createAdminClient('analytics');
  const summary: Record<string, unknown> = {};
  const failures: string[] = [];

  // 既存行（冪等: 既に値がある列はスキップされる）
  const rowMap = await sync.loadExistingRows(analytics, args.from);
  console.log(`Existing rows loaded: ${rowMap.size}`);

  // ---- 1) 計算と参照取得（この段階では一切書き込まない）----
  // breadth を backtest.sqlite から計算し、daily2（参照値かつ external の日付軸）を取得
  const breadthRows = computeBreadthFromSqlite(args);
  console.log(`breadth: computed=${breadthRows.length}`);
  const daily2Rows = await fetchNikkei225jpDaily();
  const businessDays = daily2Rows
    .map((r) => r.date)
    .filter((d) => d >= args.from && d <= args.to)
    .sort();
  summary.businessDays = businessDays.length;

  // ---- 2) 書き込み前ゲート: breadth を nikkei225jp 参照値と突合 ----
  // sqlite の営業日丸ごと欠落・列マッピング破壊は騰落レシオの系統的乖離として現れる。
  // 閾値超過なら一切 upsert せずに失敗させる（壊れた5年分を本テーブルに残さない）。
  const { validateAgainstReference } = await import('../../src/lib/market/seed-validation');
  const validation = validateAgainstReference(breadthRows, daily2Rows);
  summary.validation = validation;
  console.log(`validation: ${JSON.stringify(validation)}`);
  if (!validation.passed) {
    console.error(`validation failed: ${validation.failureReasons.join(' / ')}`);
    console.log(JSON.stringify({ success: false, dryRun: args.dryRun, ...summary }, null, 2));
    process.exit(1);
  }

  // ---- 3) breadth upsert ----
  try {
    const upserts = breadthRows
      .filter((r) => {
        const existing = rowMap.get(r.as_of_date);
        if (!existing) return true;
        return (
          existing.advancers == null ||
          existing.decliners == null ||
          existing.unchanged == null ||
          existing.new_highs == null ||
          existing.new_lows == null ||
          existing.prime_turnover_value == null ||
          existing.adv_dec_ratio_25d == null ||
          // 境界日より前は「原理的に算出不能な恒久null」でありmissing扱いしない
          // （cron側 fillBreadth と同じ境界・同じ判定関数）
          isPctSma25Pending(r.as_of_date, existing.pct_above_sma25) ||
          isPctSma200Pending(r.as_of_date, existing.pct_above_sma200)
        );
      })
      .map((r) => ({ ...r, updated_at: new Date().toISOString() }));
    const upserted = await sync.upsertRows(analytics, upserts, args.dryRun);
    // rowMap への反映は upsert 成功後
    for (const r of upserts) {
      const row = sync.getOrCreate(rowMap, r.as_of_date);
      row.advancers = r.advancers;
      row.decliners = r.decliners;
      row.unchanged = r.unchanged;
      row.adv_dec_ratio_25d = r.adv_dec_ratio_25d;
      row.new_highs = r.new_highs;
      row.new_lows = r.new_lows;
      row.prime_turnover_value = r.prime_turnover_value;
      row.pct_above_sma25 = r.pct_above_sma25;
      row.pct_above_sma200 = r.pct_above_sma200;
    }
    summary.breadth = { computed: breadthRows.length, upserted };
    console.log(`breadth: upserted=${upserted}`);
  } catch (e) {
    failures.push(`breadth: ${e instanceof Error ? e.message : String(e)}`);
  }

  // ---- 4) external（yahoo → daily2 → weekly）----
  try {
    summary.yahoo = await sync.fillYahoo(analytics, businessDays, rowMap, args.dryRun);
    console.log(`yahoo: ${JSON.stringify(summary.yahoo)}`);
  } catch (e) {
    failures.push(`yahoo: ${e instanceof Error ? e.message : String(e)}`);
  }
  try {
    summary.daily2 = await sync.fillDaily2(analytics, businessDays, rowMap, args.dryRun, daily2Rows);
    console.log(`daily2: ${JSON.stringify(summary.daily2)}`);
  } catch (e) {
    failures.push(`daily2: ${e instanceof Error ? e.message : String(e)}`);
  }
  try {
    summary.weekly = await sync.fillWeekly(analytics, args.from, args.to, rowMap, args.dryRun);
    console.log(`weekly: ${JSON.stringify(summary.weekly)}`);
  } catch (e) {
    failures.push(`weekly: ${e instanceof Error ? e.message : String(e)}`);
  }

  // ---- 5) derived（EPS / TOPIX / NT倍率）----
  try {
    const allDates = Array.from(rowMap.keys())
      .filter((d) => d >= args.from && d <= args.to)
      .sort();
    summary.derived = await sync.fillDerived(core, analytics, allDates, rowMap, args.dryRun);
    console.log(`derived: ${JSON.stringify(summary.derived)}`);
  } catch (e) {
    failures.push(`derived: ${e instanceof Error ? e.message : String(e)}`);
  }

  summary.failures = failures;
  summary.durationMs = timer();
  console.log(JSON.stringify({ success: failures.length === 0, dryRun: args.dryRun, ...summary }, null, 2));
  if (failures.length > 0) process.exit(1);
}

interface BreadthSeedRow {
  as_of_date: string;
  advancers: number;
  decliners: number;
  unchanged: number;
  adv_dec_ratio_25d: number | null;
  new_highs: number;
  new_lows: number;
  prime_turnover_value: number;
  pct_above_sma25: number | null;
  pct_above_sma200: number | null;
}

/**
 * backtest.sqlite から breadth 指標を一括計算（日付昇順ストリーミング）
 */
function computeBreadthFromSqlite(args: Args): BreadthSeedRow[] {
  // better-sqlite3 は seed 専用の devDependency（cron 実行経路では読み込まない）
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const Database = require('better-sqlite3') as typeof import('better-sqlite3');
  const db = new Database(args.sqlitePath, { readonly: true, fileMustExist: true });
  try {
    // 月次時点ユニバース（プライム＝旧一部）
    const masterRows = db
      .prepare(
        `SELECT as_of_date, local_code, market_code FROM equity_master
         WHERE market_code IN (${Array.from(PRIME_MARKET_CODES).map(() => '?').join(',')})
         ORDER BY as_of_date`
      )
      .all(...Array.from(PRIME_MARKET_CODES)) as Array<{
      as_of_date: string;
      local_code: string;
      market_code: string;
    }>;
    const snapshotDates: string[] = [];
    const primeSets = new Map<string, Set<string>>();
    for (const r of masterRows) {
      let set = primeSets.get(r.as_of_date);
      if (!set) {
        set = new Set();
        primeSets.set(r.as_of_date, set);
        snapshotDates.push(r.as_of_date);
      }
      set.add(r.local_code);
    }
    if (snapshotDates.length === 0) throw new Error('sqlite: equity_master snapshots not found');
    const primeSetFor = (date: string): Set<string> | null => {
      let latest: string | null = null;
      for (const d of snapshotDates) {
        if (d <= date) latest = d;
        else break;
      }
      return latest ? primeSets.get(latest)! : null;
    };

    const range = db
      .prepare('SELECT MIN(trade_date) AS min, MAX(trade_date) AS max FROM equity_bar_daily')
      .get() as { min: string | null; max: string | null };
    if (!range.min || !range.max) throw new Error('sqlite: equity_bar_daily is empty');

    // 基準期間の先頭（from の年初/前年初）から流し込み（sqlite収録開始でクランプ）。
    // SMA200(200営業日)の暖機用に、baselineJan1だけでは足りない場合（4-12月起点）は
    // 暦日ベースでも遡る（早い方を採用。範囲はsqlite収録開始でクランプ）。
    const baselineYear = Number(args.from.slice(0, 4)) - (includesPreviousYear(args.from) ? 1 : 0);
    const baselineJan1 = `${baselineYear}-01-01`;
    const smaWarmupStart = addDays(args.from, -SMA_LONG_WARMUP_CALENDAR_DAYS);
    const desiredStart = smaWarmupStart < baselineJan1 ? smaWarmupStart : baselineJan1;
    const feedStart = desiredStart < range.min ? range.min : desiredStart;
    const feedEnd = args.to < range.max ? args.to : range.max;
    console.log(`sqlite feed: ${feedStart} -> ${feedEnd} (coverage ${range.min}..${range.max})`);

    const acc = new BreadthAccumulator();
    const out: BreadthSeedRow[] = [];
    const rollAdv: number[] = [];
    const rollDec: number[] = [];
    let currentDate: string | null = null;
    let currentBars: BreadthBar[] = [];
    let fedDays = 0;
    let haltedAt: string | null = null;

    // NOTE: sqlite に営業日が丸ごと欠けている場合は行ストリームからは検出できない。
    // その完全性は本スクリプト末尾の検証（騰落レシオを nikkei225jp 参照値と全期間突合）で
    // 担保する（欠損日があれば以降の騰落数がズレて参照値と乖離し検出される）。
    const flushDay = (): void => {
      if (!currentDate || haltedAt) return;
      const date = currentDate;
      const isOutputDay = date >= args.from && date <= args.to;
      const primeSet = primeSetFor(date);
      if (!primeSet) return;
      const day = acc.addDay(date, currentBars, primeSet);
      fedDays++;
      // 出力対象日（args.from..args.to）のみカバレッジ不足で停止する（不完全な前日状態で
      // 計算した後続日を固定化しない。cron 側 refresh-market-indicators.ts の halt 方針と同じ）。
      // 出力対象外（SMA200暖機専用の過去分）はカバレッジ不足でも継続する（暖機区間の
      // 一時的な欠損1件で以降の全期間が失敗しないようにする）。
      if (isOutputDay && day.primeBarCount < Math.floor(primeSet.size * MIN_COVERAGE)) {
        haltedAt = date;
        console.warn(
          `coverage below threshold, halt: ${date} (${day.primeBarCount}/${primeSet.size})`
        );
        return;
      }
      // 初日は prevClose が無く騰落数が空振りするため、レシオ窓には2日目以降のみ積む
      if (fedDays > 1) {
        rollAdv.push(day.advancers);
        rollDec.push(day.decliners);
        if (rollAdv.length > 25) {
          rollAdv.shift();
          rollDec.shift();
        }
      }
      if (!isOutputDay) return;
      out.push({
        as_of_date: date,
        advancers: day.advancers,
        decliners: day.decliners,
        unchanged: day.unchanged,
        adv_dec_ratio_25d:
          rollAdv.length === 25 ? computeAdvDecRatio25(rollAdv, rollDec) : null,
        new_highs: day.newHighs,
        new_lows: day.newLows,
        prime_turnover_value: Math.round(day.turnoverValue),
        pct_above_sma25: day.pctAboveSma25,
        pct_above_sma200: day.pctAboveSma200,
      });
    };

    const stmt = db.prepare(
      `SELECT trade_date, local_code, adj_close, adj_high, adj_low, turnover_value
       FROM equity_bar_daily
       WHERE trade_date >= ? AND trade_date <= ?
       ORDER BY trade_date, local_code`
    );
    for (const raw of stmt.iterate(feedStart, feedEnd)) {
      const r = raw as {
        trade_date: string;
        local_code: string;
        adj_close: number | null;
        adj_high: number | null;
        adj_low: number | null;
        turnover_value: number | null;
      };
      if (currentDate !== r.trade_date) {
        flushDay();
        currentDate = r.trade_date;
        currentBars = [];
      }
      currentBars.push({
        code: r.local_code,
        adjClose: r.adj_close,
        adjHigh: r.adj_high,
        adjLow: r.adj_low,
        turnoverValue: r.turnover_value,
      });
    }
    flushDay();

    if (haltedAt) {
      throw new Error(
        `breadth halted at ${haltedAt} (low coverage). ソースデータの完全性を確認し、修復後に再seedすること`
      );
    }
    return out;
  } finally {
    db.close();
  }
}


const isDirectRun =
  process.argv[1]?.endsWith('market-indicators.ts') || process.argv[1]?.endsWith('market-indicators');
if (isDirectRun) {
  main().catch((error) => {
    console.error('Seed failed:', error);
    process.exit(1);
  });
}

export { main as seedMarketIndicators };
