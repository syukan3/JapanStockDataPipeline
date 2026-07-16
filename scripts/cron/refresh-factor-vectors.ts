/**
 * 類似銘柄検索用ファクターベクトル 再計算スクリプト（GH Actions用）
 *
 * @description
 * analytics.stock_screen の最新スナップショットから全銘柄の13次元ファクターベクトルを計算し、
 * analytics.stock_factor_vectors へ upsert する。最新 as_of_date のみ保持（過去日は削除）。
 * Cron A（生データ同期）の refresh-technical 完了後に cron-a.yml の
 * continue-on-error ステップとして実行する想定。
 *
 * - 計算パイプラインは src/lib/analytics/factor-vectors.ts の純関数群（テスト対象）。
 * - market_cap が NULL、または per と pbr が両方 NULL の銘柄は除外（除外理由は factor-vectors.ts 参照）。
 * - job_runs / job_locks は使わない（upsert は冪等。refresh-technical.ts と同方針）。
 * - 読みは analytics.stock_screen（VIEW）のみ、書きは analytics.stock_factor_vectors のみ → 既存に非干渉。
 * - RPC analytics.similar_stocks は本テーブルを直接参照するため、公開マーカーのような
 *   段階公開の仕組みは持たない（upsert完了までの短時間、新旧行が混在し得るが実害は小さいため許容）。
 *
 * 実行:
 *   npx tsx scripts/cron/refresh-factor-vectors.ts [--dry-run]
 *   npx tsx scripts/cron/refresh-factor-vectors.ts --similar=CODE [--limit=N]   # 書き込みなし、検証用
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { batchUpsert, batchSelect } from '../../src/lib/utils/batch';
import {
  DIMENSIONS,
  buildVectors,
  cosineSimilarity,
  distributionStats,
  formatEmbeddingLiteral,
  parseScreenRow,
  shouldExcludeStock,
  type RawScreenRow,
} from '../../src/lib/analytics/factor-vectors';

const SELECT_COLUMNS =
  'as_of_date, local_code, sector17_code, sector17_name, market_cap, per, pbr, ' +
  'dividend_yield, roe, value_pct, quality_pct, momentum_pct, dev_25, dev_200, rsi_14, atr_pct, vol_ratio_20';

interface FactorVectorRow {
  as_of_date: string;
  local_code: string;
  embedding: string;
  coverage: number;
  updated_at: string;
}

function validateEnv(): void {
  const required = ['NEXT_PUBLIC_SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

async function main(): Promise<void> {
  validateEnv();
  const dryRun = process.argv.includes('--dry-run');
  const similarArg = process.argv.find((a) => a.startsWith('--similar='));
  const limitArg = process.argv.find((a) => a.startsWith('--limit='));
  const limit = limitArg ? Number(limitArg.split('=')[1]) : 5;
  const logger = createLogger({ module: 'refresh-factor-vectors' });

  const analytics = createAdminClient('analytics');

  const rawRows = await batchSelect<RawScreenRow>(analytics, 'stock_screen', {
    pageSize: 1000,
    columns: SELECT_COLUMNS,
    orderBy: { column: 'local_code', ascending: true },
  });

  if (rawRows.length === 0) {
    logger.warn('stock_screen returned 0 rows; nothing to do');
    console.log(JSON.stringify({ success: true, targetCodes: 0, included: 0, excluded: 0, upserted: 0 }));
    return;
  }

  const parsed = rawRows.map(parseScreenRow);
  const asOfDate = parsed[0].as_of_date;

  const included = parsed.filter((r) => !shouldExcludeStock(r));
  const excluded = parsed.length - included.length;
  logger.info('Screened universe', { total: parsed.length, included: included.length, excluded });

  const { rawDimensions, coverage, vectors } = buildVectors(included);

  // --similar=CODE: 書き込みなし。メモリ内でコサイン類似度を計算し上位N件を表示（レビュー時の品質検証用）。
  if (similarArg) {
    const targetCode = similarArg.split('=')[1];
    const targetIdx = included.findIndex((r) => r.local_code === targetCode);
    if (targetIdx === -1) {
      logger.warn('Target code not found in included universe', { targetCode });
      console.log(JSON.stringify({ similar: true, targetCode, found: false, results: [] }));
      return;
    }
    const targetVector = vectors[targetIdx];
    const ranked = included
      .map((r, i) => ({
        local_code: r.local_code,
        sector17_name: r.sector17_name,
        similarity: cosineSimilarity(targetVector, vectors[i]),
      }))
      .filter((r) => r.local_code !== targetCode)
      .sort((a, b) => b.similarity - a.similarity)
      .slice(0, Math.max(1, Math.min(limit, 50)));
    console.log(
      JSON.stringify(
        { similar: true, targetCode, found: true, asOfDate, coverage: coverage[targetIdx], results: ranked },
        null,
        2
      )
    );
    return;
  }

  if (dryRun) {
    const dimStats = DIMENSIONS.map((d, i) => ({
      key: d.key,
      weight: d.weight,
      ...distributionStats(rawDimensions.map((r) => r[i])),
    }));
    console.log(
      JSON.stringify(
        {
          dryRun: true,
          asOfDate,
          targetCodes: parsed.length,
          included: included.length,
          excluded,
          dimensions: dimStats,
          sample: included.slice(0, 3).map((r, i) => ({
            local_code: r.local_code,
            coverage: coverage[i],
            embedding: vectors[i],
          })),
        },
        null,
        2
      )
    );
    return;
  }

  const upsertRows: FactorVectorRow[] = included.map((r, i) => ({
    as_of_date: asOfDate,
    local_code: r.local_code,
    embedding: formatEmbeddingLiteral(vectors[i]),
    coverage: coverage[i],
    updated_at: new Date().toISOString(),
  }));

  const result = await batchUpsert(analytics, 'stock_factor_vectors', upsertRows, 'local_code', {
    batchSize: 500,
  });
  logger.info('Upsert done', { inserted: result.inserted, batches: result.batchCount });

  // 公開後にのみ旧 as_of_date を削除（最新スナップショットのみ保持）。失敗は非クリティカル。
  const { error: delErr, count: delCount } = await analytics
    .from('stock_factor_vectors')
    .delete({ count: 'exact' })
    .lt('as_of_date', asOfDate);
  if (delErr) {
    logger.warn('Failed to prune old vectors', { error: delErr.message });
  } else {
    logger.info('Pruned old vectors', { deleted: delCount ?? 0 });
  }

  console.log(
    JSON.stringify(
      {
        success: true,
        asOfDate,
        targetCodes: parsed.length,
        included: included.length,
        excluded,
        upserted: result.inserted,
        prunedOld: delCount ?? 0,
      },
      null,
      2
    )
  );
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    const logger = createLogger({ module: 'refresh-factor-vectors' });
    logger.error('Script failed', { error: error instanceof Error ? error.message : String(error) });
    process.exit(1);
  });
