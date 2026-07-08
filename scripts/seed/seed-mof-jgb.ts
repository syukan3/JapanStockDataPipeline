#!/usr/bin/env tsx
/**
 * JGB超長期利回り（20年・30年）初回バックフィルスクリプト
 *
 * @description
 * 財務省「国債金利情報」の全期間CSV（jgbcm_all.csv）を取得し、
 * jquants_core.macro_indicator_daily に mof_jgb_20y / mof_jgb_30y として投入する。
 * 日次cron（Cron D source=mof）は当月分CSVのみを見るため、過去分はこのスクリプトで別途投入する。
 *
 * 実行:
 *   npx tsx scripts/seed/seed-mof-jgb.ts [--dry-run] [--from 2004-01-01]
 */

import { loadEnv } from './_shared';
import { createClient } from '@supabase/supabase-js';
import { ALL_HISTORY_CSV_URL, parseMofJgbCsv, tenorForSourceSeriesId, releasedAtForJgbDate } from '../../src/lib/mof/client';

const TARGET_SERIES = ['mof_jgb_20y', 'mof_jgb_30y'] as const;
const SOURCE_SERIES_ID: Record<(typeof TARGET_SERIES)[number], string> = {
  mof_jgb_20y: 'jgbcm_20y',
  mof_jgb_30y: 'jgbcm_30y',
};

async function main() {
  loadEnv();

  const argv = process.argv.slice(2);
  const dryRun = argv.includes('--dry-run');
  const fromIdx = argv.indexOf('--from');
  const from = fromIdx >= 0 ? argv[fromIdx + 1] : undefined;

  const supabase = createClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!,
    { db: { schema: 'jquants_core' } }
  );

  console.log(`Fetching ${ALL_HISTORY_CSV_URL} ...`);
  const res = await fetch(ALL_HISTORY_CSV_URL);
  if (!res.ok) throw new Error(`Fetch failed: HTTP ${res.status}`);
  const buf = await res.arrayBuffer();
  const curve = parseMofJgbCsv(buf);
  console.log(`Parsed ${curve.length} rows`);

  const filtered = from ? curve.filter((row) => row.date >= from) : curve;
  const fetchedAt = new Date().toISOString();

  for (const seriesId of TARGET_SERIES) {
    const tenor = tenorForSourceSeriesId(SOURCE_SERIES_ID[seriesId])!;
    const rows = filtered
      .filter((row) => row.tenors[tenor] != null)
      .map((row) => ({
        indicator_date: row.date,
        series_id: seriesId,
        source: 'mof' as const,
        value: row.tenors[tenor] as number,
        released_at: releasedAtForJgbDate(row.date),
        updated_at: fetchedAt,
      }));

    console.log(`${seriesId} (${tenor}): ${rows.length} rows`);

    if (dryRun) {
      console.log('  [dry-run] skip upsert. sample:', rows.slice(0, 2));
      continue;
    }

    const batchSize = 1000;
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      const { error } = await supabase
        .from('macro_indicator_daily')
        .upsert(batch, { onConflict: 'indicator_date,series_id' });
      if (error) throw new Error(`Upsert failed for ${seriesId} batch ${i}: ${error.message}`);
    }

    const maxDate = rows.reduce((max, r) => (r.indicator_date > max ? r.indicator_date : max), rows[0]?.indicator_date ?? '');
    if (maxDate && !dryRun) {
      await supabase
        .from('macro_series_metadata')
        .update({ last_fetched_at: fetchedAt, last_value_date: maxDate })
        .eq('series_id', seriesId);
    }
  }

  console.log('Done.');
}

main().catch((error) => {
  console.error('seed-mof-jgb failed:', error instanceof Error ? error.message : error);
  process.exit(1);
});
