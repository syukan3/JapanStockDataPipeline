/**
 * nikkei225jp.com データ取得クライアント（日経平均PER・空売り比率・信用評価損益率）
 *
 * @description
 * 以下の指標は無料の公式機械可読ソースが存在しないため、第三者サイト
 * nikkei225jp.com が配信する Highcharts 用 JSON を防御的にパースして利用する。
 * - 日経平均PER（日次・公式CSVは日次3年分のみ＋Cloudflare障壁）
 * - 空売り比率（JPX公式はPDFのみ / J-Quants は Standard 以上）
 * - 信用評価損益率（週次・二市場。公式の無料バルクなし）
 *
 * ソース仕様（2026-07-06 実レスポンスで検証済み）:
 * - daily2.json: `var DAILY = [[epoch_ms, 日経終値, ...], ...]`、35列・2009-06〜当日。
 *   col[1]=日経終値, col[7]=騰落レシオ(参照用), col[8]/col[9]=新高値/新安値(参照用),
 *   col[11]=空売り比率[%], col[12]=PER, col[13]=PBR, col[19]=売買代金[百万円](参照用)
 * - dailyweek2.json: 24列・同一日付軸。週次列は週末営業日のみ非空。col[7]=信用評価損益率[%]。
 *   取得には Referer ヘッダが必須（無いと404）。
 *
 * 防御実装:
 * - 列数アサーション（列構成変更＝マッピング破壊を即検知）
 * - 日経終値の Yahoo 突合は呼び出し側（refresh/seed）で実施
 * - 値域チェックは保存対象列ごとに個別適用（逸脱列のみNULL化し他列は保存）
 *
 * 注意: 位置ベース・無ドキュメントの第三者ソースであり、予告なく壊れうる。
 * 壊れた場合は当該指標のみ欠損させ、他の指標・パイプライン本体は止めない。
 */

import { fetchWithRetry } from '../utils/retry';
import { RateLimiter } from '../jquants/rate-limiter';
import { createLogger } from '../utils/logger';
import { BROWSER_USER_AGENT } from './yahoo-chart-client';

const logger = createLogger({ module: 'nikkei225jp-client' });

const DAILY2_URL = 'https://nikkei225jp.com/_data/_nfsDATA/DAY/daily2.json';
const DAILYWEEK2_URL = 'https://nikkei225jp.com/_data/_nfsDATA/DAY/dailyweek2.json';
/** dailyweek2.json はホットリンク保護があり Referer 必須 */
const REFERER = 'https://nikkei225jp.com/data/sinyou.php';

/** 期待列数（変わったら列マッピング破壊とみなし全体を拒否する） */
export const DAILY2_EXPECTED_COLS = 35;
export const DAILYWEEK2_EXPECTED_COLS = 24;

/** サイトへの負荷防止（日次バッチで2リクエストのみだが保守的に） */
let rateLimiter: RateLimiter | null = null;
function getRateLimiter(): RateLimiter {
  if (!rateLimiter) {
    rateLimiter = new RateLimiter({ requestsPerMinute: 10, minIntervalMs: 3000 });
  }
  return rateLimiter;
}

/** 保存対象列の値域（逸脱はその列のみ null 化。ストレス局面も考慮して緩めに設定） */
const RANGE_CHECKS = {
  nikkeiClose: { min: 5000, max: 200000 },
  per: { min: 5, max: 120 }, // リーマン直後は40超の実績あり
  shortSellingRatio: { min: 10, max: 80 },
  marginPlRatio: { min: -50, max: 15 },
} as const;

export interface Nikkei225jpDailyRow {
  /** JST の日付 (YYYY-MM-DD) */
  date: string;
  /** 日経平均終値（Yahoo突合用） */
  nikkeiClose: number | null;
  /** 日経平均PER */
  per: number | null;
  /** 空売り比率[%] */
  shortSellingRatio: number | null;
  /** 参照用（自前計算の検証にのみ使用・保存しない） */
  refAdvDecRatio: number | null;
  refNewHighs: number | null;
  refNewLows: number | null;
  /** 参照用: 売買代金[百万円] */
  refTurnoverMn: number | null;
}

export interface Nikkei225jpWeeklyRow {
  /** JST の日付 (YYYY-MM-DD)（週末営業日） */
  date: string;
  /** 信用評価損益率[%] */
  marginPlRatio: number;
}

/** epoch ms → JST日付文字列 (YYYY-MM-DD) */
export function epochMsToJstDate(epochMs: number): string {
  const formatter = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  return formatter.format(new Date(epochMs));
}

/**
 * `var XXX = [...]` 形式のレスポンスから JSON 配列部分を取り出してパース（純関数）
 *
 * 空要素（`,,` や `""`）は null に正規化する。
 */
export function parseVarArrayPayload(text: string): unknown[][] {
  const start = text.indexOf('[');
  if (start < 0) throw new Error('nikkei225jp: no JSON array found in payload');
  let body = text.slice(start).trim();
  if (body.endsWith(';')) body = body.slice(0, -1);
  // 空要素を null へ（JSONとして不正な `[,` `,,` `,]` を補正）
  body = body.replace(/\[\s*,/g, '[null,').replace(/,(?=\s*[,\]])/g, ',null');
  const parsed = JSON.parse(body) as unknown;
  if (!Array.isArray(parsed)) throw new Error('nikkei225jp: payload is not an array');
  return parsed as unknown[][];
}

function toNumber(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  return null;
}

function inRange(v: number | null, range: { min: number; max: number }): number | null {
  if (v == null) return null;
  return v >= range.min && v <= range.max ? v : null;
}

/**
 * daily2.json をパース（純関数・テスト対象）
 *
 * @throws 列数が期待と異なる場合（列マッピング破壊）
 */
export function parseNikkei225jpDaily(text: string): Nikkei225jpDailyRow[] {
  const rows = parseVarArrayPayload(text);
  const out: Nikkei225jpDailyRow[] = [];
  let rangeRejected = 0;
  for (const row of rows) {
    if (!Array.isArray(row)) continue;
    if (row.length !== DAILY2_EXPECTED_COLS) {
      throw new Error(
        `nikkei225jp daily2: column count changed (expected ${DAILY2_EXPECTED_COLS}, got ${row.length}). 列マッピング要再確認`
      );
    }
    const epochMs = toNumber(row[0]);
    if (epochMs == null) continue;
    const nikkeiClose = inRange(toNumber(row[1]), RANGE_CHECKS.nikkeiClose);
    const per = inRange(toNumber(row[12]), RANGE_CHECKS.per);
    const shortSellingRatio = inRange(toNumber(row[11]), RANGE_CHECKS.shortSellingRatio);
    if (
      (toNumber(row[12]) != null && per == null) ||
      (toNumber(row[11]) != null && shortSellingRatio == null)
    ) {
      rangeRejected++;
    }
    out.push({
      date: epochMsToJstDate(epochMs),
      nikkeiClose,
      per,
      shortSellingRatio,
      refAdvDecRatio: toNumber(row[7]),
      refNewHighs: toNumber(row[8]),
      refNewLows: toNumber(row[9]),
      refTurnoverMn: toNumber(row[19]),
    });
  }
  if (rangeRejected > 0) {
    logger.warn('daily2: 値域チェックで一部の値をnull化', { rangeRejected });
  }
  return out;
}

/**
 * dailyweek2.json をパース（純関数・テスト対象）
 *
 * 週次列（col[7]=信用評価損益率）が非空の行のみ返す。
 *
 * @throws 列数が期待と異なる場合（列マッピング破壊）
 */
export function parseNikkei225jpWeekly(text: string): Nikkei225jpWeeklyRow[] {
  const rows = parseVarArrayPayload(text);
  const out: Nikkei225jpWeeklyRow[] = [];
  let rangeRejected = 0;
  for (const row of rows) {
    if (!Array.isArray(row)) continue;
    if (row.length !== DAILYWEEK2_EXPECTED_COLS) {
      throw new Error(
        `nikkei225jp dailyweek2: column count changed (expected ${DAILYWEEK2_EXPECTED_COLS}, got ${row.length}). 列マッピング要再確認`
      );
    }
    const epochMs = toNumber(row[0]);
    const raw = toNumber(row[7]);
    if (epochMs == null || raw == null) continue;
    const value = inRange(raw, RANGE_CHECKS.marginPlRatio);
    if (value == null) {
      rangeRejected++;
      continue;
    }
    out.push({ date: epochMsToJstDate(epochMs), marginPlRatio: value });
  }
  if (rangeRejected > 0) {
    logger.warn('dailyweek2: 値域チェックで一部の値を除外', { rangeRejected });
  }
  return out;
}

async function fetchText(url: string, referer?: string): Promise<string> {
  await getRateLimiter().acquire();
  const res = await fetchWithRetry(
    url,
    {
      headers: {
        'User-Agent': BROWSER_USER_AGENT,
        Accept: '*/*',
        ...(referer ? { Referer: referer } : {}),
      },
    },
    { maxRetries: 3, baseDelayMs: 2000 }
  );
  return res.text();
}

/** daily2.json（PER・空売り比率・参照値）を取得してパース */
export async function fetchNikkei225jpDaily(): Promise<Nikkei225jpDailyRow[]> {
  logger.info('Fetching nikkei225jp daily2.json');
  const text = await fetchText(DAILY2_URL, REFERER);
  const rows = parseNikkei225jpDaily(text);
  logger.info('daily2.json parsed', { rows: rows.length });
  return rows;
}

/** dailyweek2.json（信用評価損益率・週次）を取得してパース */
export async function fetchNikkei225jpWeekly(): Promise<Nikkei225jpWeeklyRow[]> {
  logger.info('Fetching nikkei225jp dailyweek2.json');
  const text = await fetchText(DAILYWEEK2_URL, REFERER);
  const rows = parseNikkei225jpWeekly(text);
  logger.info('dailyweek2.json parsed', { rows: rows.length });
  return rows;
}
