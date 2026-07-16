/**
 * 日経公式（indexes.nikkei.co.jp）日経平均 日次4本値CSVクライアント
 *
 * @description
 * https://indexes.nikkei.co.jp/nkave/historical/nikkei_stock_average_daily_jp.csv
 * - Shift_JIS。ヘッダは「データ日付,終値,始値,高値,安値」で**終値が先頭**（OHLC順ではない）
 * - 収録は直近約3.5年（2026-07実測: 2023-01-04〜当日。当日終値は大引け後に反映）
 * - 末尾に注意書き行（日付でない引用行）が付くため、日付として解釈できる行のみ採用する
 * - 2026-07実測で Yahoo chart API が GH Actions ランナー/ローカルIPの双方に HTTP 429 を
 *   返し続けるようになったため、日経OHLCの一次ソースを本CSVへ切替（Yahooはフォールバック）。
 *   2023-01-04 より前の OHLC は本ソースでは取得不能（indicators-sync 側で恒久null扱い）。
 */

import { fetchWithRetry } from '../utils/retry';
import { RateLimiter } from '../jquants/rate-limiter';
import { createLogger } from '../utils/logger';
import { BROWSER_USER_AGENT, type DailyBar } from './yahoo-chart-client';

const logger = createLogger({ module: 'nikkei-official-client' });

const CSV_URL =
  'https://indexes.nikkei.co.jp/nkave/historical/nikkei_stock_average_daily_jp.csv';

/** 公式CSVの収録開始日（これより前の日経OHLCは本ソースでは埋まらない） */
export const NIKKEI_OFFICIAL_CSV_FROM = '2023-01-04';

/** 日経平均の値として妥当な範囲（yahoo-chart-client と同水準） */
const PRICE_RANGE = { min: 5000, max: 200000 } as const;

let rateLimiter: RateLimiter | null = null;
function getRateLimiter(): RateLimiter {
  if (!rateLimiter) {
    rateLimiter = new RateLimiter({ requestsPerMinute: 6, minIntervalMs: 5000 });
  }
  return rateLimiter;
}

function toPrice(s: string): number | null {
  const v = Number(s);
  if (!Number.isFinite(v) || v < PRICE_RANGE.min || v > PRICE_RANGE.max) return null;
  return Number(v.toFixed(2));
}

/**
 * 公式CSVをパースして日次OHLCの配列（日付昇順・同一日は後勝ち）へ変換（純関数・テスト対象）
 *
 * - 行形式: `"2023/01/04","25716.86","25834.93","25840.68","25661.89"`（終値,始値,高値,安値）
 * - 日付行と解釈できない行（Shift_JIS化けしたヘッダ・末尾の注意書き）は読み飛ばす
 * - close が値域外の行は行ごと捨てる。open/high/low は値域外のみ null 化（列単位の防御）
 * - high < low の矛盾行は open/high/low を null 化して close だけ採用する
 */
export function parseNikkeiOfficialCsv(text: string): DailyBar[] {
  const rowRe =
    /^"(\d{4})\/(\d{2})\/(\d{2})","([\d.]+)","([\d.]*)","([\d.]*)","([\d.]*)"\s*$/;
  const byDate = new Map<string, DailyBar>();
  for (const line of text.split(/\r?\n/)) {
    const m = line.match(rowRe);
    if (!m) continue;
    const date = `${m[1]}-${m[2]}-${m[3]}`;
    const close = toPrice(m[4]);
    if (close == null) continue;
    let open = m[5] ? toPrice(m[5]) : null;
    let high = m[6] ? toPrice(m[6]) : null;
    let low = m[7] ? toPrice(m[7]) : null;
    if (high != null && low != null && high < low) {
      open = null;
      high = null;
      low = null;
    }
    byDate.set(date, { date, close, open, high, low });
  }
  return Array.from(byDate.values()).sort((a, b) => a.date.localeCompare(b.date));
}

/** 日経平均の日次OHLCを公式CSVから全期間分取得（収録は約3.5年・1リクエスト） */
export async function fetchNikkeiOfficialDaily(): Promise<DailyBar[]> {
  await getRateLimiter().acquire();
  logger.info('Fetching nikkei official daily CSV');
  const res = await fetchWithRetry(
    CSV_URL,
    {
      headers: {
        'User-Agent': BROWSER_USER_AGENT,
        Accept: 'text/csv,*/*',
      },
    },
    { maxRetries: 3, baseDelayMs: 2000 }
  );
  const buf = await res.arrayBuffer();
  const text = new TextDecoder('shift_jis').decode(buf);
  const bars = parseNikkeiOfficialCsv(text);
  if (bars.length === 0) {
    throw new Error('nikkei official CSV: 0 rows parsed（フォーマット変化の疑い）');
  }
  logger.info('official CSV parsed', {
    rows: bars.length,
    first: bars[0].date,
    last: bars[bars.length - 1].date,
  });
  return bars;
}
