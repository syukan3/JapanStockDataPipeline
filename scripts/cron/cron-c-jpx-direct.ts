/**
 * Cron C の JPX 経路: 投資部門別 株式売買状況（週次Excel）の取り込み
 *
 * @description
 * J-Quants OFF 中は J-Quants の /equities/investor-types の代わりに、JPX が公表する週次Excel
 * （新形式 stock_1_w_*.xlsx）を取り込んで investor_type_trading に source='jpx' で書く。
 * 設計正本: ../../../docs/PLANS-jquants-off-switch-2026-10.md §4.4
 *
 * モード別の挙動:
 * - OFF: 直近の週次ファイルを取り込み、job_runs('cron_c') を記録する（未達検知 00061 を生かす）。
 * - ON（シャドー運転）: 書き込まず、同じ週の J-Quants の行と突き合わせて差分をログに出す。
 *   差分があれば ::warning:: を出す（解約前の受け入れ基準 §10 の確認用）。
 *
 * 実行: npx tsx scripts/cron/cron-c-jpx-direct.ts [--dry-run]
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { addDays } from '../../src/lib/utils/date';
import { fetchWithRetry } from '../../src/lib/utils/retry';
import { getBusinessDaysOrThrow } from '../../src/lib/cron/business-day';
import { getJQuantsMode } from '../../src/lib/data-source/jquants-mode';
import { startJobRun, completeJobRun } from '../../src/lib/cron/job-run';
import { BROWSER_USER_AGENT } from '../../src/lib/market/yahoo-chart-client';
import { readFirstSheetCells } from '../../src/lib/jpx/xlsx-lite';
import {
  JPX_INVESTOR_PAGE_URL,
  computeJpxPublishedDate,
  findWeeklyXlsxLinks,
  parseJpxWeeklyInvestorCells,
} from '../../src/lib/jpx/investor-types';
import type { InvestorTypeTradingRecord } from '../../src/lib/jquants/types';

const logger = createLogger({ module: 'cron-c-jpx-direct' });

/** 1回に取り込む週数（ページに並ぶ新形式ファイルの新しい順） */
const MAX_WEEKS = 4;

const ON_CONFLICT = 'published_date,section,start_date,end_date,investor_type,metric';

async function fetchText(url: string): Promise<string> {
  const res = await fetchWithRetry(
    url,
    { headers: { 'User-Agent': BROWSER_USER_AGENT, Accept: 'text/html' }, signal: AbortSignal.timeout(30_000) },
    { maxRetries: 3, baseDelayMs: 2000 }
  );
  return res.text();
}

async function fetchBuffer(url: string): Promise<Buffer> {
  const res = await fetchWithRetry(
    url,
    { headers: { 'User-Agent': BROWSER_USER_AGENT, Accept: '*/*' }, signal: AbortSignal.timeout(30_000) },
    { maxRetries: 3, baseDelayMs: 2000 }
  );
  return Buffer.from(await res.arrayBuffer());
}

/** ON 中の突き合わせ: 同じ週の J-Quants の行との差分（キー → [jpx, jquants]） */
async function compareWithJQuants(
  core: ReturnType<typeof createAdminClient>,
  rows: InvestorTypeTradingRecord[]
): Promise<{ compared: number; mismatches: string[]; missingInJQuants: boolean }> {
  const { startDate, endDate } = { startDate: rows[0].start_date, endDate: rows[0].end_date };
  const { data, error } = await core
    .from('investor_type_trading')
    .select('published_date, section, investor_type, metric, value_kjpy')
    .eq('start_date', startDate)
    .eq('end_date', endDate)
    .eq('source', 'jquants');
  if (error) throw new Error(`investor_type_trading の取得に失敗しました: ${error.message}`);
  const jq = (data ?? []) as Array<{ published_date: string; section: string; investor_type: string; metric: string; value_kjpy: string | number }>;
  if (jq.length === 0) return { compared: 0, mismatches: [], missingInJQuants: true };

  // 同じ週に訂正版があれば published_date が新しい方を採用（Portfolio の getInvestorFlowTrend と同じ規約）
  const latest = new Map<string, { pub: string; v: number }>();
  for (const r of jq) {
    const k = `${r.section}|${r.investor_type}|${r.metric}`;
    const cur = latest.get(k);
    if (!cur || r.published_date > cur.pub) latest.set(k, { pub: r.published_date, v: Number(r.value_kjpy) });
  }
  const mismatches: string[] = [];
  for (const r of rows) {
    const k = `${r.section}|${r.investor_type}|${r.metric}`;
    const j = latest.get(k);
    if (!j || j.v !== r.value_kjpy) mismatches.push(`${k}: jpx=${r.value_kjpy} jquants=${j?.v ?? 'なし'}`);
  }
  const pubs = new Set(Array.from(latest.values()).map((x) => x.pub));
  if (!pubs.has(rows[0].published_date)) {
    mismatches.push(`published_date: jpx=${rows[0].published_date} jquants=${Array.from(pubs).join(',')}`);
  }
  return { compared: rows.length, mismatches, missingInJQuants: false };
}

async function main(): Promise<void> {
  const dryRun = process.argv.includes('--dry-run');
  const mode = await getJQuantsMode({ fresh: true });
  const core = createAdminClient('jquants_core');
  const ingest = createAdminClient('jquants_ingest');
  const writes = !mode.enabled && !dryRun;

  let runId: string | undefined;
  if (writes) {
    const started = await startJobRun(ingest, {
      jobName: 'cron_c',
      meta: { dataset: 'investor_types', mode: 'jquants_off', source: 'jpx' },
    });
    if (started.error) {
      logger.info('cron_c の job_run を開始できず（実行済みの可能性）。取り込みは続行する', { reason: started.error });
    } else {
      runId = started.runId;
    }
  }

  try {
    const links = findWeeklyXlsxLinks(await fetchText(JPX_INVESTOR_PAGE_URL)).slice(0, MAX_WEEKS);
    if (links.length === 0) {
      throw new Error('JPX の投資部門別ページに週次 xlsx（stock_1_w_*.xlsx）が見つかりません（ページ構成の変更の可能性）');
    }

    const summary: Array<Record<string, unknown>> = [];
    for (const link of links) {
      const businessDays = await getBusinessDaysOrThrow(core, addDays(link.endDate, 1), addDays(link.endDate, 20));
      const publishedDate = computeJpxPublishedDate(link.endDate, businessDays);
      const cells = readFirstSheetCells(await fetchBuffer(link.url));
      const rows = parseJpxWeeklyInvestorCells(cells, { ...link, publishedDate });

      if (mode.enabled) {
        const cmp = await compareWithJQuants(core, rows);
        summary.push({ week: `${link.startDate}..${link.endDate}`, publishedDate, ...cmp, mismatches: cmp.mismatches.slice(0, 10), mismatchCount: cmp.mismatches.length });
        if (cmp.mismatches.length > 0) {
          console.log(`::warning::JPX と J-Quants の投資部門別が一致しない週があります: ${link.startDate}..${link.endDate}（${cmp.mismatches.length}件）`);
        }
        continue;
      }

      if (writes) {
        const { error } = await core.from('investor_type_trading').upsert(rows, { onConflict: ON_CONFLICT });
        if (error) throw new Error(`investor_type_trading への書き込みに失敗しました: ${error.message}`);
      }
      summary.push({ week: `${link.startDate}..${link.endDate}`, publishedDate, rows: rows.length, written: writes });
    }

    logger.info('JPX 投資部門別の処理完了', { jquants: mode.enabled ? 'ON（突き合わせのみ）' : 'OFF', dryRun, summary });
    if (runId) await completeJobRun(ingest, runId, 'success');
  } catch (error) {
    if (runId) await completeJobRun(ingest, runId, 'failed', error instanceof Error ? error.message : String(error));
    throw error;
  }
}

main().catch((error) => {
  logger.error('JPX 投資部門別の取り込みに失敗しました', {
    error: error instanceof Error ? error.message : String(error),
  });
  process.exit(1);
});
