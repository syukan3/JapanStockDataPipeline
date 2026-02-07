/**
 * kabuyutai.com スクレイパー
 *
 * @description 月別優待一覧ページから優待情報を取得
 * - レート制限: 5リクエスト/分（礼儀正しいクロール）
 * - robots.txt なし、利用規約なし（調査済み）
 */

import { fetchWithRetry } from '../utils/retry';
import { createLogger } from '../utils/logger';
import type { YutaiBenefit } from './types';

const logger = createLogger({ module: 'kabuyutai-client' });

const BASE_URL = 'https://www.kabuyutai.com/kobetu';

/** リクエスト間隔（ms）: 5req/min = 12秒間隔 */
const REQUEST_INTERVAL_MS = 12_000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * 優待価値（円）を推定
 * 「1,000円相当」「2000円分」「3000円」「クオカード 500円」等から数値を抽出
 */
export function estimateBenefitValue(content: string): number | null {
  // 「○○円相当」「○○円分」「○○円」パターン
  const match = content.match(/([0-9,]+)\s*円/);
  if (match) {
    const value = parseInt(match[1].replace(/,/g, ''), 10);
    if (!isNaN(value) && value > 0 && value < 1_000_000) {
      return value;
    }
  }
  return null;
}

/**
 * 優待カテゴリを推定
 */
export function estimateCategory(content: string): string | null {
  const categories: Array<[string, RegExp]> = [
    ['食品', /食品|食事|お米|米|グルメ|飲料|お茶|コーヒー|ビール|ワイン|菓子|食料/],
    ['金券', /クオカード|QUOカード|図書カード|ギフトカード|商品券|お買物券|割引券|金券/],
    ['優待券', /優待券|食事券|飲食券|入場券|宿泊券|施設利用|レジャー|映画/],
    ['カタログ', /カタログ|選べる|ポイント/],
    ['自社製品', /自社製品|自社商品|自社サービス/],
    ['日用品', /日用品|化粧品|ヘルスケア|健康/],
    ['その他', /.*/],
  ];

  for (const [cat, re] of categories) {
    if (re.test(content)) return cat;
  }
  return null;
}

/**
 * HTMLテキストから銘柄コード（4桁 or 5桁）を抽出
 */
function extractLocalCode(text: string): string | null {
  const match = text.match(/(\d{4,5})/);
  if (!match) return null;
  const code = match[1];
  // 4桁なら末尾0を付けて5桁に
  return code.length === 4 ? code + '0' : code;
}

/**
 * HTML行から優待レコードをパース
 *
 * kabuyutai.com の個別ページ（/kobetu/{code}.html）をパース
 * ページ構造: テーブル行に企業名、優待内容、必要株数、権利確定月が含まれる
 */
export function parseYutaiListPage(html: string, month: number): YutaiBenefit[] {
  const results: YutaiBenefit[] = [];

  // テーブル行を抽出: <tr>...</tr> のパターン
  const trPattern = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let trMatch: RegExpExecArray | null;

  while ((trMatch = trPattern.exec(html)) !== null) {
    const rowHtml = trMatch[1];

    // <td> セルを抽出
    const tdPattern = /<td[^>]*>([\s\S]*?)<\/td>/gi;
    const cells: string[] = [];
    let tdMatch: RegExpExecArray | null;
    while ((tdMatch = tdPattern.exec(rowHtml)) !== null) {
      // HTMLタグを除去してテキストのみ取得
      cells.push(tdMatch[1].replace(/<[^>]+>/g, '').trim());
    }

    if (cells.length < 4) continue;

    // 銘柄コード抽出（最初のセルにコードが含まれる）
    const localCode = extractLocalCode(cells[0]);
    if (!localCode) continue;

    // 企業名（2番目のセル or コードの横）
    const companyName = cells[1] || '';
    if (!companyName) continue;

    // 必要株数を探す
    let minShares = 100; // デフォルト
    for (const cell of cells) {
      const sharesMatch = cell.match(/(\d{1,3}(?:,\d{3})*)\s*株/);
      if (sharesMatch) {
        minShares = parseInt(sharesMatch[1].replace(/,/g, ''), 10);
        break;
      }
    }

    // 優待内容（最も長いセルを使用、コード・企業名以外）
    const contentCandidates = cells.slice(2).filter((c) => c.length > 3);
    const benefitContent = contentCandidates.sort((a, b) => b.length - a.length)[0] || '';
    if (!benefitContent) continue;

    results.push({
      local_code: localCode,
      company_name: companyName.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>'),
      min_shares: minShares,
      benefit_content: benefitContent.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>'),
      benefit_value: estimateBenefitValue(benefitContent),
      record_month: month,
      record_day: 'end',
      category: estimateCategory(benefitContent),
    });
  }

  return results;
}

/**
 * kabuyutai.com から指定月の優待一覧を取得
 */
export async function fetchYutaiBenefitsForMonth(month: number): Promise<YutaiBenefit[]> {
  const url = `${BASE_URL}/${month}gatsu.html`;

  logger.info('Fetching kabuyutai page', { month, url });

  try {
    const response = await fetchWithRetry(url, {
      headers: {
        'User-Agent': 'JapanStockDataPipeline/1.0 (personal use)',
        Accept: 'text/html',
      },
    }, {
      maxRetries: 2,
      baseDelayMs: 5000,
    });

    const html = await response.text();
    const benefits = parseYutaiListPage(html, month);

    logger.info('Parsed kabuyutai benefits', { month, count: benefits.length });
    return benefits;
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    logger.error('Failed to fetch kabuyutai page', { month, error: msg });
    throw error;
  }
}

/**
 * 全月の優待情報を取得
 */
export async function fetchAllYutaiBenefits(): Promise<YutaiBenefit[]> {
  const allBenefits: YutaiBenefit[] = [];

  for (let month = 1; month <= 12; month++) {
    try {
      const benefits = await fetchYutaiBenefitsForMonth(month);
      allBenefits.push(...benefits);
    } catch (error) {
      logger.error('Failed to fetch month, continuing', { month });
    }

    // レート制限
    if (month < 12) {
      await sleep(REQUEST_INTERVAL_MS);
    }
  }

  logger.info('Fetched all kabuyutai benefits', { total: allBenefits.length });
  return allBenefits;
}
