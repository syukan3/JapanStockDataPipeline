/**
 * kabu STATION API 経由在庫取得（ローカル実行用）
 *
 * @description Windows PC 上で kabu STATION が起動している状態で実行
 * - CLI引数: --from-db（yutai_benefit から銘柄リスト取得）or --symbols=file.txt
 * - 環境変数: KABU_API_PASSWORD, NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { KabuStationClient } from '../../src/lib/yutai/kabu-station-client';
import { batchSelect } from '../../src/lib/utils/batch';
import * as fs from 'fs';

const logger = createLogger({ module: 'fetch-inventory-local' });

interface CliArgs {
  fromDb: boolean;
  symbolsFile: string | null;
  baseUrl: string | null;
}

function parseArgs(): CliArgs {
  let fromDb = false;
  let symbolsFile: string | null = null;
  let baseUrl: string | null = null;

  for (const arg of process.argv.slice(2)) {
    if (arg === '--from-db') {
      fromDb = true;
    } else if (arg.startsWith('--symbols=')) {
      symbolsFile = arg.split('=')[1];
    } else if (arg.startsWith('--base-url=')) {
      baseUrl = arg.split('=')[1];
    }
  }

  if (!fromDb && !symbolsFile) {
    console.error('Usage: npx tsx scripts/yutai/fetch-inventory-local.ts --from-db | --symbols=file.txt');
    process.exit(1);
  }

  return { fromDb, symbolsFile, baseUrl };
}

function validateEnv(): { apiPassword: string } {
  const apiPassword = process.env.KABU_API_PASSWORD;
  if (!apiPassword) {
    throw new Error('KABU_API_PASSWORD environment variable is required');
  }

  const required = ['NEXT_PUBLIC_SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'];
  const missing = required.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }

  return { apiPassword };
}

/**
 * yutai_benefit テーブルから銘柄コード一覧を取得
 */
async function getSymbolsFromDb(): Promise<string[]> {
  const supabaseCore = createAdminClient('jquants_core');

  const rows = await batchSelect<{ local_code: string }>(supabaseCore, 'yutai_benefit', {
    columns: 'local_code',
    orderBy: { column: 'local_code' },
  });

  // 重複除去して4桁コードに変換
  const codes = [...new Set(rows.map((r) => r.local_code.slice(0, 4)))];
  return codes;
}

/**
 * ファイルから銘柄コード一覧を読み込み
 */
function getSymbolsFromFile(filePath: string): string[] {
  const content = fs.readFileSync(filePath, 'utf-8');
  return content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => /^\d{4}$/.test(line));
}

async function main(): Promise<void> {
  const args = parseArgs();
  const { apiPassword } = validateEnv();

  // 銘柄リスト取得
  const symbols = args.fromDb
    ? await getSymbolsFromDb()
    : getSymbolsFromFile(args.symbolsFile!);

  logger.info('Symbols to process', { count: symbols.length });

  if (symbols.length === 0) {
    console.log('No symbols to process');
    return;
  }

  // kabu STATION API クライアント（localhost のみ許可）
  if (args.baseUrl) {
    const url = new URL(args.baseUrl);
    if (url.hostname !== 'localhost' && url.hostname !== '127.0.0.1') {
      throw new Error(`--base-url must be localhost or 127.0.0.1 (got: ${url.hostname}). Credential leak risk.`);
    }
  }

  const client = new KabuStationClient({
    apiPassword,
    ...(args.baseUrl ? { baseUrl: args.baseUrl } : {}),
  });

  await client.authenticate();

  // バッチで在庫情報取得
  const premiumMap = await client.getMarginPremiumBatch(symbols);
  logger.info('Fetched margin premiums', { total: premiumMap.size });

  // Supabase に UPSERT
  const supabaseCore = createAdminClient('jquants_core');
  const inventoryDate = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Tokyo' });
  const now = new Date().toISOString();

  const rows = [...premiumMap.entries()].map(([symbol, premium]) => {
    const gm = premium.GeneralMargin;
    const isAvailable = gm.MarginPremiumType > 0 || gm.MarginPremium != null;

    return {
      local_code: symbol.length === 4 ? symbol + '0' : symbol,
      broker: 'esmart',
      inventory_date: inventoryDate,
      inventory_qty: null, // API では在庫数量は非公開
      is_available: isAvailable,
      loan_type: 'general',
      loan_term: null,
      premium_fee: gm.MarginPremium,
      source: 'kabu_station',
      fetched_at: now,
    };
  });

  const BATCH_SIZE = 500;
  let upserted = 0;
  let failedBatches = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const { error } = await supabaseCore
      .from('margin_inventory')
      .upsert(batch, { onConflict: 'local_code,broker,inventory_date,loan_type' });

    if (error) {
      logger.error('Upsert failed', { batchIndex: i, error: error.message });
      failedBatches++;
    } else {
      upserted += batch.length;
    }
  }

  const success = failedBatches === 0;
  console.log(JSON.stringify({
    success,
    symbolsProcessed: symbols.length,
    premiumsFetched: premiumMap.size,
    rowsUpserted: upserted,
    failedBatches,
    inventoryDate,
  }, null, 2));

  if (!success) {
    process.exit(1);
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    logger.error('Script failed', { error: error instanceof Error ? error.message : String(error) });
    process.exit(1);
  });
