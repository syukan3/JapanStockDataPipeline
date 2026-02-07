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

const BASE_URL = 'https://www.kabuyutai.com/yutai';

const MONTH_NAMES = [
  '', 'january', 'february', 'march', 'april', 'may', 'june',
  'july', 'august', 'september', 'october', 'november', 'december',
];

/** リクエスト間隔（ms）: 静的ページ取得なので3秒間隔で十分 */
const REQUEST_INTERVAL_MS = 3_000;

/** ページネーション上限（安全弁） */
const MAX_PAGES = 20;

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
 * 全角括弧 （1234） または半角括弧 (1234) に対応
 */
function extractLocalCode(text: string): string | null {
  const match = text.match(/[（(](\d{4,5})[）)]/);
  if (!match) return null;
  const code = match[1];
  return code.length === 4 ? code + '0' : code;
}

/**
 * HTML から優待レコードをパース
 *
 * kabuyutai.com の月別一覧ページ（/yutai/{month}.html）をパース
 * ページ構造: <div class="table_tr"> ブロックに企業名・コード・優待内容が含まれる
 */
export function parseYutaiListPage(html: string, month: number): YutaiBenefit[] {
  const results: YutaiBenefit[] = [];

  // <div class="table_tr"> ブロックを抽出
  const blockPattern = /<div\s+class="table_tr">([\s\S]*?)(?=<div\s+class="table_tr">|<div\s+class="pagination"|$)/gi;
  let blockMatch: RegExpExecArray | null;

  while ((blockMatch = blockPattern.exec(html)) !== null) {
    const block = blockMatch[1];

    // 銘柄コード: 「（1234）」パターン
    const localCode = extractLocalCode(block);
    if (!localCode) continue;

    // 企業名: <a href="...">企業名</a> から抽出
    const nameMatch = block.match(/<a\s+href="[^"]*">([^<]+)<\/a>/);
    const companyName = nameMatch ? nameMatch[1].trim() : '';
    if (!companyName) continue;

    // 優待内容: 【優待内容】の後のテキスト
    const benefitMatch = block.match(/【優待内容】([^<]*)/);
    const benefitContent = benefitMatch ? benefitMatch[1].trim() : '';
    if (!benefitContent) continue;

    // 必要株数: 優待内容から「○○株」を探す、なければデフォルト100
    let minShares = 100;
    const sharesMatch = block.match(/(\d{1,3}(?:,\d{3})*)\s*株/);
    if (sharesMatch) {
      minShares = parseInt(sharesMatch[1].replace(/,/g, ''), 10);
    }

    results.push({
      local_code: localCode,
      company_name: companyName.replace(/&amp;/g, '&'),
      min_shares: minShares,
      benefit_content: benefitContent.replace(/&amp;/g, '&'),
      benefit_value: estimateBenefitValue(benefitContent),
      record_month: month,
      record_day: 'end',
      category: estimateCategory(benefitContent),
    });
  }

  return results;
}

/**
 * ページネーションの最大ページ数を検出
 */
export function detectMaxPage(html: string): number {
  let maxPage = 1;
  const pagePattern = /href="[a-z]+(\d+)\.html"/gi;
  let m: RegExpExecArray | null;
  while ((m = pagePattern.exec(html)) !== null) {
    const page = parseInt(m[1], 10);
    if (page > maxPage) maxPage = page;
  }
  return maxPage;
}

/**
 * 単一ページを取得
 */
async function fetchPage(url: string): Promise<string> {
  const response = await fetchWithRetry(url, {
    headers: {
      'User-Agent': 'JapanStockDataPipeline/1.0 (personal use)',
      Accept: 'text/html',
    },
  }, {
    maxRetries: 2,
    baseDelayMs: 5000,
  });
  return response.text();
}

/**
 * kabuyutai.com から指定月の優待一覧を取得（全ページ巡回）
 */
export async function fetchYutaiBenefitsForMonth(month: number): Promise<YutaiBenefit[]> {
  const monthName = MONTH_NAMES[month];
  if (!monthName) throw new Error(`Invalid month: ${month}`);

  const firstUrl = `${BASE_URL}/${monthName}.html`;
  logger.info('Fetching kabuyutai page', { month, url: firstUrl });

  try {
    const firstHtml = await fetchPage(firstUrl);
    const allBenefits = parseYutaiListPage(firstHtml, month);
    const maxPage = detectMaxPage(firstHtml);

    for (let page = 2; page <= Math.min(maxPage, MAX_PAGES); page++) {
      await sleep(REQUEST_INTERVAL_MS);
      const pageUrl = `${BASE_URL}/${monthName}${page}.html`;
      logger.info('Fetching kabuyutai page', { month, page, url: pageUrl });
      const pageHtml = await fetchPage(pageUrl);
      allBenefits.push(...parseYutaiListPage(pageHtml, month));
    }

    logger.info('Parsed kabuyutai benefits', { month, pages: maxPage, count: allBenefits.length });
    return allBenefits;
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
