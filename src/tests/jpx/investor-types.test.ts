/**
 * jpx/investor-types.ts と jpx/xlsx-lite.ts のユニットテスト
 */

import { describe, it, expect } from 'vitest';
import { deflateRawSync } from 'zlib';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import {
  findWeeklyXlsxLinks,
  computeJpxPublishedDate,
  parseJpxWeeklyInvestorCells,
} from '@/lib/jpx/investor-types';
import { readFirstSheetCells, readZipEntries, type CellValue } from '@/lib/jpx/xlsx-lite';

const fixture = JSON.parse(
  readFileSync(resolve(__dirname, '__fixtures__/stock_1_w_20260914_20260918.json'), 'utf8')
) as {
  cells: Record<string, CellValue>;
  expected: Array<{ section: string; investor_type: string; metric: string; value_kjpy: number }>;
};
const cells = new Map(Object.entries(fixture.cells));
const week = { startDate: '2026-09-14', endDate: '2026-09-18', publishedDate: '2026-09-29' };

describe('parseJpxWeeklyInvestorCells', () => {
  it('2026-09-14〜18週の全208行が J-Quants と完全一致する', () => {
    const rows = parseJpxWeeklyInvestorCells(cells, week);
    expect(rows).toHaveLength(208);
    const got = new Map(rows.map((r) => [`${r.section}|${r.investor_type}|${r.metric}`, r.value_kjpy]));
    for (const e of fixture.expected) {
      expect(got.get(`${e.section}|${e.investor_type}|${e.metric}`), `${e.section} ${e.investor_type} ${e.metric}`).toBe(
        e.value_kjpy
      );
    }
    expect(rows.every((r) => r.source === 'jpx' && r.published_date === '2026-09-29')).toBe(true);
  });

  it('海外投資家・プライム・差引は法人＋個人の合算（-6,719億円）', () => {
    const rows = parseJpxWeeklyInvestorCells(cells, week);
    const foreign = rows.find((r) => r.section === 'TSEPrime' && r.investor_type === 'foreign' && r.metric === 'balance');
    expect(foreign?.value_kjpy).toBe(-671952210);
  });

  it('見出しがずれていたら書き込まずに例外', () => {
    const broken = new Map(cells);
    broken.set('T4', '個人 Individuals');
    expect(() => parseJpxWeeklyInvestorCells(broken, week)).toThrow(/形式が想定と違います.*T4/);
  });

  it('数値が欠けていたら例外', () => {
    const broken = new Map(cells);
    broken.delete('V9');
    expect(() => parseJpxWeeklyInvestorCells(broken, week)).toThrow(/V9 が数値ではありません/);
  });
});

describe('findWeeklyXlsxLinks', () => {
  it('新形式の週次 xlsx だけを拾い、新しい週を先頭にして絶対URLにする', () => {
    const html = `
      <a href="/markets/statistics-equities/investor-type/aaa-att/stock_1_w_20260907_20260911.xlsx">x</a>
      <a href="/markets/statistics-equities/investor-type/bbb-att/stock_1_w_20260914_20260918.xlsx">x</a>
      <a href="/markets/statistics-equities/investor-type/ccc-att/stock_val_1_260902.xls">旧形式</a>`;
    const links = findWeeklyXlsxLinks(html);
    expect(links.map((l) => l.endDate)).toEqual(['2026-09-18', '2026-09-11']);
    expect(links[0]).toEqual({
      url: 'https://www.jpx.co.jp/markets/statistics-equities/investor-type/bbb-att/stock_1_w_20260914_20260918.xlsx',
      startDate: '2026-09-14',
      endDate: '2026-09-18',
    });
  });
});

describe('computeJpxPublishedDate', () => {
  it('週末の翌営業日から数えて第4営業日（9/21〜23 の連休をまたぐ）', () => {
    expect(computeJpxPublishedDate('2026-09-18', ['2026-09-24', '2026-09-25', '2026-09-28', '2026-09-29', '2026-09-30'])).toBe(
      '2026-09-29'
    );
  });

  it('営業日が足りなければ例外', () => {
    expect(() => computeJpxPublishedDate('2026-09-18', ['2026-09-24'])).toThrow(/公表日を決められません/);
  });
});

// ---- xlsx-lite: テスト内で最小の zip を組み立てて読む ----

function crc32(buf: Buffer): number {
  let c = ~0;
  for (const b of buf) {
    c ^= b;
    for (let k = 0; k < 8; k++) c = c & 1 ? (c >>> 1) ^ 0xedb88320 : c >>> 1;
  }
  return ~c >>> 0;
}

function buildZip(files: Array<{ name: string; data: string; deflate: boolean }>): Buffer {
  const locals: Buffer[] = [];
  const centrals: Buffer[] = [];
  let offset = 0;
  for (const f of files) {
    const raw = Buffer.from(f.data, 'utf8');
    const body = f.deflate ? deflateRawSync(raw) : raw;
    const name = Buffer.from(f.name, 'utf8');
    const lh = Buffer.alloc(30);
    lh.writeUInt32LE(0x04034b50, 0);
    lh.writeUInt16LE(f.deflate ? 8 : 0, 8);
    lh.writeUInt32LE(crc32(raw), 14);
    lh.writeUInt32LE(body.length, 18);
    lh.writeUInt32LE(raw.length, 22);
    lh.writeUInt16LE(name.length, 26);
    const ch = Buffer.alloc(46);
    ch.writeUInt32LE(0x02014b50, 0);
    ch.writeUInt16LE(f.deflate ? 8 : 0, 10);
    ch.writeUInt32LE(crc32(raw), 16);
    ch.writeUInt32LE(body.length, 20);
    ch.writeUInt32LE(raw.length, 24);
    ch.writeUInt16LE(name.length, 28);
    ch.writeUInt32LE(offset, 42);
    locals.push(lh, name, body);
    centrals.push(ch, name);
    offset += lh.length + name.length + body.length;
  }
  const cd = Buffer.concat(centrals);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(files.length, 8);
  eocd.writeUInt16LE(files.length, 10);
  eocd.writeUInt32LE(cd.length, 12);
  eocd.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, cd, eocd]);
}

describe('xlsx-lite', () => {
  const sheet = `<worksheet><sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="inlineStr"><is><t>海外</t><t>投資家</t></is></c></row>
    <row r="2"><c r="A2"><v>-673688289</v></c><c r="B2" t="s"><v>1</v></c><c r="C2"/></row>
  </sheetData></worksheet>`;
  const sst = `<sst><si><t>東証プライム&#10;TSE Prime</t></si><si><r><t>Life &amp;</t></r><r><t> Non-Life</t></r></si></sst>`;

  it('共有文字列・インライン文字列・数値を読む（deflate・stored の両方）', () => {
    for (const deflate of [true, false]) {
      const zip = buildZip([
        { name: 'xl/sharedStrings.xml', data: sst, deflate },
        { name: 'xl/worksheets/sheet1.xml', data: sheet, deflate },
      ]);
      const got = readFirstSheetCells(zip);
      expect(got.get('A1')).toBe('東証プライム\nTSE Prime');
      expect(got.get('B1')).toBe('海外投資家');
      expect(got.get('A2')).toBe(-673688289);
      expect(got.get('B2')).toBe('Life & Non-Life');
      expect(got.has('C2')).toBe(false);
    }
  });

  it('zip でなければ例外', () => {
    expect(() => readZipEntries(Buffer.from('not a zip at all, definitely not'))).toThrow(/終端レコード/);
  });
});
