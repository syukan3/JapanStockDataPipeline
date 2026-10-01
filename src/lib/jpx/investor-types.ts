/**
 * JPX 投資部門別 株式売買状況（週次Excel・新形式）の取り込み（J-Quants OFF 中の代替）
 *
 * @description
 * JPX は 2026-09 第3週から週次の公表を `stock_1_w_YYYYMMDD_YYYYMMDD.xlsx`（1シート・4市場区分）に変えた。
 * 新形式では自己・個人が「現金／信用」、海外投資家が「法人／個人」に分かれているので、合算して
 * J-Quants の13区分（investor_type_trading と同じ値）に揃える。2026-09-14〜18週の全208行
 * （4区分×13部門×4指標）が J-Quants と完全一致することを 2026-10-01 に確認済み。
 * 設計正本: ../../../../docs/PLANS-jquants-off-switch-2026-10.md §4.4
 *
 * 見出しが想定と違えば例外（書き込まない = fail closed）。JPX は形式を変えたばかりなので、
 * 再変更で列がずれたまま黙って書き込むのを防ぐ。
 */

import type { InvestorTypeTradingRecord } from '../jquants/types';
import type { CellValue } from './xlsx-lite';

export const JPX_INVESTOR_PAGE_URL = 'https://www.jpx.co.jp/markets/statistics-equities/investor-type/index.html';

export interface JpxWeeklyFile {
  url: string;
  startDate: string;
  endDate: string;
}

const FILE_RE = /href="([^"]*stock_1_w_(\d{8})_(\d{8})\.xlsx)"/g;

function ymd(compact: string): string {
  return `${compact.slice(0, 4)}-${compact.slice(4, 6)}-${compact.slice(6, 8)}`;
}

/** 投資部門別ページの HTML から週次 xlsx（新形式）のリンクを集める。新しい週が先頭（純関数） */
export function findWeeklyXlsxLinks(html: string, pageUrl: string = JPX_INVESTOR_PAGE_URL): JpxWeeklyFile[] {
  const byEnd = new Map<string, JpxWeeklyFile>();
  for (const m of html.matchAll(FILE_RE)) {
    const file = { url: new URL(m[1], pageUrl).toString(), startDate: ymd(m[2]), endDate: ymd(m[3]) };
    byEnd.set(file.endDate, file);
  }
  return Array.from(byEnd.values()).sort((a, b) => b.endDate.localeCompare(a.endDate));
}

/**
 * 公表日（published_date）を J-Quants と同じ規則で決める（純関数）
 *
 * 「週末の翌営業日から数えて第4営業日」。J-Quants の published_date と 235週中232週一致
 * （例外3週は 2024-08 の訂正2週と 2024年末）。規則で決めるので何度取り込んでも同じ主キーになる。
 *
 * @param businessDaysAfter endDate より後の営業日（昇順）
 */
export function computeJpxPublishedDate(endDate: string, businessDaysAfter: string[]): string {
  const after = businessDaysAfter.filter((d) => d > endDate).sort();
  if (after.length < 4) {
    throw new Error(`公表日を決められません: ${endDate} の後の営業日が ${after.length} 日しかありません`);
  }
  return after[3];
}

function colToNum(col: string): number {
  let n = 0;
  for (const ch of col) n = n * 26 + (ch.charCodeAt(0) - 64);
  return n;
}

function numToCol(n: number): string {
  let s = '';
  while (n > 0) {
    const r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

/** 各グループの先頭列（以降4列が 売・買・差引・合計） */
const GROUP_COLUMNS = {
  proprietaryCash: 'D',
  proprietaryMargin: 'H',
  individualCash: 'L',
  individualMargin: 'P',
  foreignInstitutions: 'T',
  foreignIndividuals: 'X',
  securities_co: 'AB',
  investment_trust: 'AF',
  business_corp: 'AJ',
  other_corp: 'AN',
  insurance: 'AR',
  bank: 'AV',
  trust_bank: 'AZ',
  other_financial: 'BD',
} as const;

type GroupKey = keyof typeof GROUP_COLUMNS;

const METRICS = ['sales', 'purchases', 'balance', 'total'] as const;
type Metric = (typeof METRICS)[number];

/** 見出しの検査（セル → 含むべき語） */
const HEADER_EXPECTATIONS: Array<[string, string]> = [
  ['D3', '自己'],
  ['L3', '委託'],
  ['L4', '個人'],
  ['T4', '海外投資家'],
  ['AB4', '証券会社'],
  ['AF4', '法人'],
  ['D5', '現金'],
  ['H5', '信用'],
  ['L5', '現金'],
  ['P5', '信用'],
  ['T5', '法人'],
  ['X5', '個人'],
  ['AF5', '投資信託'],
  ['AJ5', '事業法人'],
  ['AN5', 'その他法人'],
  ['AR5', '金融機関'],
  ['AR6', '生保'],
  ['AV6', '都銀'],
  ['AZ6', '信託銀行'],
  ['BD6', 'その他金融'],
  ['D7', '売'],
  ['E7', '買'],
  ['F7', '差引'],
  ['G7', '合計'],
  ['BD7', '売'],
  ['BG7', '合計'],
];

/** 市場区分（金額の行・見出しの行・DB の section） */
const SECTIONS = [
  { valueRow: 9, labelRow: 8, label: 'プライム', section: 'TSEPrime' },
  { valueRow: 11, labelRow: 10, label: 'スタンダード', section: 'TSEStandard' },
  { valueRow: 13, labelRow: 12, label: 'グロース', section: 'TSEGrowth' },
  { valueRow: 15, labelRow: 14, label: '二市場', section: 'TokyoNagoya' },
] as const;

function text(cells: Map<string, CellValue>, ref: string): string {
  const v = cells.get(ref);
  return typeof v === 'string' ? v : v == null ? '' : String(v);
}

/** 見出しが想定どおりかを確かめる。違えば例外 */
export function assertJpxWeeklyLayout(cells: Map<string, CellValue>): void {
  const bad = HEADER_EXPECTATIONS.filter(([ref, word]) => !text(cells, ref).includes(word)).map(
    ([ref, word]) => `${ref}（期待: ${word} / 実際: ${text(cells, ref) || '空'}）`
  );
  for (const s of SECTIONS) {
    if (!text(cells, `B${s.labelRow}`).includes(s.label)) bad.push(`B${s.labelRow}（期待: ${s.label}）`);
    if (!text(cells, `C${s.valueRow}`).includes('金額')) bad.push(`C${s.valueRow}（期待: 金額）`);
  }
  if (bad.length > 0) {
    throw new Error(`JPX 投資部門別 Excel の形式が想定と違います（書き込みを中止）: ${bad.join(', ')}`);
  }
}

/**
 * 週次 Excel のセルを investor_type_trading の行へ変換する（純関数）
 *
 * 金額の行（千円）だけを使う。13区分×4指標×4市場区分 = 208行。
 */
export function parseJpxWeeklyInvestorCells(
  cells: Map<string, CellValue>,
  week: { startDate: string; endDate: string; publishedDate: string }
): InvestorTypeTradingRecord[] {
  assertJpxWeeklyLayout(cells);

  const records: InvestorTypeTradingRecord[] = [];
  for (const s of SECTIONS) {
    const read = (group: GroupKey): Record<Metric, number> => {
      const start = colToNum(GROUP_COLUMNS[group]);
      const out = {} as Record<Metric, number>;
      METRICS.forEach((m, i) => {
        const ref = `${numToCol(start + i)}${s.valueRow}`;
        const v = cells.get(ref);
        if (typeof v !== 'number') {
          throw new Error(`JPX 投資部門別 Excel: ${ref} が数値ではありません（${String(v)}）`);
        }
        out[m] = v;
      });
      return out;
    };
    const sum = (...parts: Array<Record<Metric, number>>): Record<Metric, number> =>
      Object.fromEntries(METRICS.map((m) => [m, parts.reduce((a, p) => a + p[m], 0)])) as Record<Metric, number>;

    const t: Record<string, Record<Metric, number>> = {
      proprietary: sum(read('proprietaryCash'), read('proprietaryMargin')),
      individual: sum(read('individualCash'), read('individualMargin')),
      foreign: sum(read('foreignInstitutions'), read('foreignIndividuals')),
      securities_co: read('securities_co'),
      investment_trust: read('investment_trust'),
      business_corp: read('business_corp'),
      other_corp: read('other_corp'),
      insurance: read('insurance'),
      bank: read('bank'),
      trust_bank: read('trust_bank'),
      other_financial: read('other_financial'),
    };
    // 委託計 = 個人＋海外投資家＋証券会社＋法人（投信・事業法人・その他法人・金融機関4区分）
    t.brokerage = sum(
      t.individual,
      t.foreign,
      t.securities_co,
      t.investment_trust,
      t.business_corp,
      t.other_corp,
      t.insurance,
      t.bank,
      t.trust_bank,
      t.other_financial
    );
    t.total = sum(t.proprietary, t.brokerage);

    for (const [investorType, metrics] of Object.entries(t)) {
      for (const m of METRICS) {
        records.push({
          published_date: week.publishedDate,
          start_date: week.startDate,
          end_date: week.endDate,
          section: s.section,
          investor_type: investorType,
          metric: m,
          value_kjpy: metrics[m],
          source: 'jpx',
        });
      }
    }
  }
  return records;
}
