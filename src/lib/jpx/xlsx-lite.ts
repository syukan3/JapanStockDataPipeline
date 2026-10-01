/**
 * xlsx（Office Open XML）の最小リーダー
 *
 * @description
 * JPX の投資部門別 週次Excel（1シート・数十行）を読むためだけの実装。依存パッケージを増やさない
 * （公開リポジトリで、用途が1ファイルの数値読み取りに限られるため）。
 * - zip は中央ディレクトリから読む（stored / deflate のみ。zip64・暗号化は非対応で例外）
 * - 最初のワークシートのセル値を A1 形式のキーで返す（共有文字列・インライン文字列・数値）
 */

import { inflateRawSync } from 'zlib';

export type CellValue = string | number;

const EOCD_SIG = 0x06054b50;
const CDH_SIG = 0x02014b50;
const LFH_SIG = 0x04034b50;

/** zip のエントリを名前→中身で読む（純関数） */
export function readZipEntries(buf: Buffer): Map<string, Buffer> {
  // End of Central Directory を末尾から探す（コメント長は最大 65535）
  let eocd = -1;
  for (let i = buf.length - 22; i >= Math.max(0, buf.length - 22 - 65535); i--) {
    if (buf.readUInt32LE(i) === EOCD_SIG) {
      eocd = i;
      break;
    }
  }
  if (eocd < 0) throw new Error('xlsx: zip の終端レコードが見つかりません');

  const count = buf.readUInt16LE(eocd + 10);
  let p = buf.readUInt32LE(eocd + 16);
  const out = new Map<string, Buffer>();
  for (let n = 0; n < count; n++) {
    if (buf.readUInt32LE(p) !== CDH_SIG) throw new Error('xlsx: 中央ディレクトリが壊れています');
    const flags = buf.readUInt16LE(p + 8);
    const method = buf.readUInt16LE(p + 10);
    const compSize = buf.readUInt32LE(p + 20);
    const nameLen = buf.readUInt16LE(p + 28);
    const extraLen = buf.readUInt16LE(p + 30);
    const commentLen = buf.readUInt16LE(p + 32);
    const localOffset = buf.readUInt32LE(p + 42);
    const name = buf.toString('utf8', p + 46, p + 46 + nameLen);
    p += 46 + nameLen + extraLen + commentLen;

    if (flags & 0x1) throw new Error(`xlsx: 暗号化エントリは読めません: ${name}`);
    if (compSize === 0xffffffff || localOffset === 0xffffffff) throw new Error('xlsx: zip64 は非対応です');
    if (buf.readUInt32LE(localOffset) !== LFH_SIG) throw new Error(`xlsx: ローカルヘッダが壊れています: ${name}`);
    const dataStart = localOffset + 30 + buf.readUInt16LE(localOffset + 26) + buf.readUInt16LE(localOffset + 28);
    const raw = buf.subarray(dataStart, dataStart + compSize);
    if (method === 0) out.set(name, Buffer.from(raw));
    else if (method === 8) out.set(name, inflateRawSync(raw));
    else throw new Error(`xlsx: 未対応の圧縮方式 ${method}: ${name}`);
  }
  return out;
}

function decodeXml(s: string): string {
  return s
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_, d: string) => String.fromCodePoint(Number(d)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h: string) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&amp;/g, '&');
}

/** <si> や <is> の中の <t> を連結する（リッチテキストの分割にも対応） */
function joinTexts(fragment: string): string {
  return Array.from(fragment.matchAll(/<t(?:\s[^>]*)?>([\s\S]*?)<\/t>/g))
    .map((m) => decodeXml(m[1]))
    .join('');
}

/** 最初のワークシートのセル値（A1 形式キー）を返す */
export function readFirstSheetCells(buf: Buffer): Map<string, CellValue> {
  const entries = readZipEntries(buf);
  const sheetName = Array.from(entries.keys())
    .filter((n) => /^xl\/worksheets\/sheet\d+\.xml$/.test(n))
    .sort((a, b) => Number(a.match(/(\d+)\.xml$/)![1]) - Number(b.match(/(\d+)\.xml$/)![1]))[0];
  if (!sheetName) throw new Error('xlsx: ワークシートが見つかりません');

  const shared: string[] = [];
  const sst = entries.get('xl/sharedStrings.xml');
  if (sst) {
    for (const m of sst.toString('utf8').matchAll(/<si>([\s\S]*?)<\/si>/g)) shared.push(joinTexts(m[1]));
  }

  const cells = new Map<string, CellValue>();
  const xml = entries.get(sheetName)!.toString('utf8');
  for (const m of xml.matchAll(/<c\s+r="([A-Z]+\d+)"([^>]*?)(?:\/>|>([\s\S]*?)<\/c>)/g)) {
    const [, ref, attrs, inner = ''] = m;
    const type = /\bt="([^"]+)"/.exec(attrs)?.[1];
    if (type === 'inlineStr') {
      const is = /<is>([\s\S]*?)<\/is>/.exec(inner);
      if (is) cells.set(ref, joinTexts(is[1]));
      continue;
    }
    const v = /<v>([\s\S]*?)<\/v>/.exec(inner)?.[1];
    if (v == null) continue;
    if (type === 's') {
      const s = shared[Number(v)];
      if (s == null) throw new Error(`xlsx: 共有文字列 ${v} がありません（${ref}）`);
      cells.set(ref, s);
    } else if (type === 'str' || type === 'b' || type === 'e') {
      cells.set(ref, decodeXml(v));
    } else {
      const n = Number(v);
      if (!Number.isFinite(n)) throw new Error(`xlsx: 数値を読めません（${ref}: ${v}）`);
      cells.set(ref, n);
    }
  }
  return cells;
}
