/**
 * eスマート証券 一般信用売り在庫CSVパーサー
 *
 * @description eスマート証券の公開CSV（ログイン不要）をダウンロードしてパース
 */

import { fetchWithRetry } from '../utils/retry';
import { createLogger } from '../utils/logger';
import type { MarginInventory } from './types';

const logger = createLogger({ module: 'kabu-csv-client' });

/**
 * eスマート証券 一般信用売建可能銘柄一覧 CSV URL
 * NOTE: URLは変更される可能性がある。取得失敗時はURL確認が必要
 */
const CSV_URL = 'https://kabu.com/pdf/misc/stock_list_general_margin.csv';

/**
 * CSVテキストを行ごとにパース（ダブルクォート対応）
 */
export function parseCsvLines(csvText: string): string[][] {
  const lines = csvText.split(/\r?\n/).filter((line) => line.trim().length > 0);
  return lines.map((line) => {
    const cells: string[] = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQuotes) {
        if (ch === '"' && line[i + 1] === '"') {
          current += '"';
          i++;
        } else if (ch === '"') {
          inQuotes = false;
        } else {
          current += ch;
        }
      } else {
        if (ch === '"') {
          inQuotes = true;
        } else if (ch === ',') {
          cells.push(current.trim());
          current = '';
        } else {
          current += ch;
        }
      }
    }
    cells.push(current.trim());
    return cells;
  });
}

/**
 * 銘柄コードを5桁に正規化
 */
function normalizeCode(code: string): string {
  const digits = code.replace(/[^\d]/g, '');
  return digits.length === 4 ? digits + '0' : digits;
}

/**
 * CSVのパース結果から MarginInventory[] を生成
 *
 * 想定CSV構造（ヘッダー付き）:
 * コード, 銘柄名, 売建区分, 貸借区分, 在庫数量, 貸借期限, ...
 *
 * 実際のカラム位置はヘッダーから動的に決定
 */
export function parseMarginInventoryCsv(
  csvText: string,
  inventoryDate: string,
): MarginInventory[] {
  const rows = parseCsvLines(csvText);
  if (rows.length < 2) return [];

  // ヘッダーからカラム位置を特定
  const header = rows[0].map((h) => h.replace(/["\s]/g, ''));
  const codeIdx = header.findIndex((h) => /コード|銘柄コード|code/i.test(h));
  const qtyIdx = header.findIndex((h) => /在庫|数量|残数/i.test(h));
  const termIdx = header.findIndex((h) => /期限|期間/i.test(h));
  const premiumIdx = header.findIndex((h) => /プレミアム|料/i.test(h));

  if (codeIdx < 0) {
    throw new Error(`CSV header does not contain code column. Header: ${header.join(', ')}`);
  }

  const results: MarginInventory[] = [];

  for (let i = 1; i < rows.length; i++) {
    const cells = rows[i];
    if (cells.length <= codeIdx) continue;

    const rawCode = cells[codeIdx];
    if (!rawCode || !/\d{4,5}/.test(rawCode)) continue;

    const localCode = normalizeCode(rawCode);

    // 在庫数量
    let inventoryQty: number | null = null;
    if (qtyIdx >= 0 && cells[qtyIdx]) {
      const qty = parseInt(cells[qtyIdx].replace(/[,\s]/g, ''), 10);
      if (!isNaN(qty)) inventoryQty = qty;
    }

    // 貸借期限
    let loanTerm: string | null = null;
    if (termIdx >= 0 && cells[termIdx]) {
      loanTerm = cells[termIdx] || null;
    }

    // プレミアム料
    let premiumFee: number | null = null;
    if (premiumIdx >= 0 && cells[premiumIdx]) {
      const fee = parseFloat(cells[premiumIdx].replace(/[,\s]/g, ''));
      if (!isNaN(fee)) premiumFee = fee;
    }

    // CSVに載っている = 在庫あり
    const isAvailable = inventoryQty === null || inventoryQty > 0;

    results.push({
      local_code: localCode,
      broker: 'esmart',
      inventory_date: inventoryDate,
      inventory_qty: inventoryQty,
      is_available: isAvailable,
      loan_type: 'general',
      loan_term: loanTerm,
      premium_fee: premiumFee,
      source: 'kabu_csv',
    });
  }

  return results;
}

/**
 * eスマート証券の一般信用売り在庫CSVを取得してパース
 */
export async function fetchMarginInventoryCsv(
  inventoryDate: string,
): Promise<MarginInventory[]> {
  logger.info('Fetching margin inventory CSV', { inventoryDate });

  try {
    const response = await fetchWithRetry(CSV_URL, {
      headers: {
        'User-Agent': 'JapanStockDataPipeline/1.0 (personal use)',
        Accept: 'text/csv,text/plain,*/*',
      },
    }, {
      maxRetries: 2,
      baseDelayMs: 3000,
    });

    // Shift_JIS でエンコードされている可能性があるため、arrayBuffer で取得後デコード
    const buffer = await response.arrayBuffer();
    const decoder = new TextDecoder('shift_jis');
    const csvText = decoder.decode(buffer);

    const inventory = parseMarginInventoryCsv(csvText, inventoryDate);

    logger.info('Parsed margin inventory CSV', { count: inventory.length });
    return inventory;
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    logger.error('Failed to fetch margin inventory CSV', { error: msg });
    throw error;
  }
}
