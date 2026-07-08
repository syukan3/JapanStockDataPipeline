/**
 * 財務省「国債金利情報」クライアント
 *
 * @description 日次CSV（当月分）を取得し、年限別の利回りをパースする。
 * 認証不要。Shift_JIS + 和暦日付という独特のフォーマットのため専用パーサを持つ。
 */

import { fetchWithRetry } from '../utils/retry';

/** MOFエンドポイントのタイムアウト（ミリ秒）。ハングしてCron D全体(GH Actions 15分)を
 *  止めないよう、FRED/e-Statクライアントと同様に短めに設定する。 */
const REQUEST_TIMEOUT_MS = 15_000;

const CURRENT_MONTH_CSV_URL = 'https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv';
export const ALL_HISTORY_CSV_URL = 'https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv';

/** CSV列の並び順（基準日の次から） */
export const TENOR_COLUMNS = [
  '1年', '2年', '3年', '4年', '5年', '6年', '7年', '8年', '9年', '10年',
  '15年', '20年', '25年', '30年', '40年',
] as const;

/** macro_series_metadata.source_series_id → CSV列名 */
const SOURCE_SERIES_TO_TENOR: Record<string, (typeof TENOR_COLUMNS)[number]> = {
  jgbcm_20y: '20年',
  jgbcm_30y: '30年',
};

export function tenorForSourceSeriesId(sourceSeriesId: string): string | null {
  return SOURCE_SERIES_TO_TENOR[sourceSeriesId] ?? null;
}

export interface MofJgbRow {
  /** YYYY-MM-DD */
  date: string;
  /** 年限（'1年'等） → 利回り（%）。未提供年限は null */
  tenors: Record<string, number | null>;
}

/**
 * 和暦日付（例 "R8.7.7"）を YYYY-MM-DD に変換。
 * 明治以降の元号のうち、CSVで使われうる S(昭和)/H(平成)/R(令和) をサポート。
 * パース不能な場合は null。
 */
export function parseEraDate(raw: string): string | null {
  const m = raw.trim().match(/^([SHR])(\d{1,2})\.(\d{1,2})\.(\d{1,2})$/);
  if (!m) return null;
  const [, era, yStr, moStr, dStr] = m;
  const eraBaseYear: Record<string, number> = { S: 1925, H: 1988, R: 2018 };
  const base = eraBaseYear[era];
  if (base === undefined) return null;
  const year = base + Number(yStr);
  const month = Number(moStr);
  const day = Number(dStr);
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

/**
 * MOF CSV（Shift_JIS、ヘッダ2行、列は 基準日,1年,2年,...,40年）をパースする。
 * 未提供年限（'-'）は null として保持する（0への誤変換を避ける）。
 */
export function parseMofJgbCsv(buffer: ArrayBuffer | Uint8Array): MofJgbRow[] {
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  const text = new TextDecoder('shift_jis').decode(bytes);
  const lines = text.split(/\r\n|\n/).filter((l) => l.trim().length > 0);

  const rows: MofJgbRow[] = [];
  // 先頭2行（タイトル行・列名行）をスキップ
  for (let i = 2; i < lines.length; i++) {
    const cols = lines[i].split(',');
    if (cols.length < 2) continue;

    const date = parseEraDate(cols[0]);
    if (!date) continue;

    const tenors: Record<string, number | null> = {};
    TENOR_COLUMNS.forEach((tenor, idx) => {
      const raw = cols[idx + 1]?.trim();
      tenors[tenor] = raw && raw !== '-' ? Number(raw) : null;
    });

    rows.push({ date, tenors });
  }

  return rows;
}

/**
 * ある観測日の released_at として使うタイムスタンプを返す。
 * MOFは同日中に当日分の利回りを公表するため、取得時刻(now)ではなく観測日ベースで
 * 固定する（バックフィル時にnow()を入れると、過去日付でのas-of評価で
 * released_at <= evalDate を満たせず永久に見えなくなるバグを避けるため）。
 */
export function releasedAtForJgbDate(date: string): string {
  return `${date}T15:00:00+09:00`;
}

export interface MofClient {
  /** 当月分のJGB利回りカーブを取得（日次cron用の差分取得元） */
  getJgbCurve(): Promise<MofJgbRow[]>;
}

export function createMofClient(): MofClient {
  async function getJgbCurve(): Promise<MofJgbRow[]> {
    const res = await fetchWithRetry(
      CURRENT_MONTH_CSV_URL,
      { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) },
      { maxRetries: 2, baseDelayMs: 200 }
    );
    const buf = await res.arrayBuffer();
    return parseMofJgbCsv(buf);
  }

  return { getJgbCurve };
}
