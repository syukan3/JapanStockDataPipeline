/**
 * 営業日カレンダーの自前生成（J-Quants OFF 中の代替）
 *
 * @description
 * 内閣府「国民の祝日」CSV＋土日＋年末年始（12/31〜1/3）から東証の営業日を作る。
 * 2016-07-18〜2027-10-06 の J-Quants カレンダー 4,098 日と営業日判定が完全一致することを
 * 2026-10-01 に確認済み。臨時休場は反映できない（J-Quants でも事前には入らない）。
 * 設計正本: ../../../../docs/PLANS-jquants-off-switch-2026-10.md §4.1
 */

import type { TradingCalendarRecord } from '../jquants/types';
import { BROWSER_USER_AGENT } from '../market/yahoo-chart-client';

export const CABINET_OFFICE_HOLIDAYS_URL = 'https://www8.cao.go.jp/chosei/shukujitsu/syukujitsu.csv';

/** 祝日一覧（YYYY-MM-DD の集合）と、CSV が収録している最後の年 */
export interface HolidayTable {
  dates: Set<string>;
  lastYear: number;
}

function pad2(n: number): string {
  return String(n).padStart(2, '0');
}

/**
 * 内閣府CSV（デコード済みテキスト）を解析する（純関数・テスト対象）
 *
 * 1行目は見出し。各行は `YYYY/M/D,名称`。形式が崩れていたら例外（黙って祝日を落とさない）。
 */
export function parseCabinetOfficeHolidaysCsv(text: string): HolidayTable {
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter((l) => l.length > 0);
  if (lines.length < 2 || !lines[0].includes('祝日')) {
    throw new Error('内閣府祝日CSVの見出しが想定と違います');
  }
  const dates = new Set<string>();
  let lastYear = 0;
  for (const line of lines.slice(1)) {
    const m = /^(\d{4})\/(\d{1,2})\/(\d{1,2}),/.exec(line);
    if (!m) {
      throw new Error(`内閣府祝日CSVの行を解析できません: ${line}`);
    }
    const y = Number(m[1]);
    dates.add(`${y}-${pad2(Number(m[2]))}-${pad2(Number(m[3]))}`);
    lastYear = Math.max(lastYear, y);
  }
  return { dates, lastYear };
}

/** 東証の営業日か（土日・祝日・12/31〜1/3 は休み） */
export function isGeneratedBusinessDay(date: string, holidays: HolidayTable): boolean {
  const d = new Date(`${date}T00:00:00Z`);
  const dow = d.getUTCDay();
  if (dow === 0 || dow === 6) return false;
  if (holidays.dates.has(date)) return false;
  const md = date.slice(5);
  if (md === '12-31' || md === '01-01' || md === '01-02' || md === '01-03') return false;
  return true;
}

/**
 * 期間のカレンダー行を作る（純関数・テスト対象）
 *
 * CSV が収録している最後の年の12/31までしか作らない（翌年の祝日は例年2月ごろに公表されるため、
 * 未公表の年を土日だけで埋めると祝日を営業日と誤る）。
 */
export function buildGeneratedCalendar(
  from: string,
  to: string,
  holidays: HolidayTable
): TradingCalendarRecord[] {
  const limit = `${holidays.lastYear}-12-31`;
  const end = to < limit ? to : limit;
  const records: TradingCalendarRecord[] = [];
  for (let d = new Date(`${from}T00:00:00Z`); ; d.setUTCDate(d.getUTCDate() + 1)) {
    const date = d.toISOString().slice(0, 10);
    if (date > end) break;
    const business = isGeneratedBusinessDay(date, holidays);
    records.push({
      calendar_date: date,
      hol_div: business ? '1' : '0',
      is_business_day: business,
      source: 'generated',
    });
  }
  return records;
}

/** 内閣府CSVを取得して解析する（Shift_JIS） */
export async function fetchCabinetOfficeHolidays(): Promise<HolidayTable> {
  const res = await fetch(CABINET_OFFICE_HOLIDAYS_URL, {
    headers: { 'User-Agent': BROWSER_USER_AGENT },
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok) {
    throw new Error(`内閣府祝日CSVの取得に失敗しました: HTTP ${res.status}`);
  }
  const text = new TextDecoder('shift_jis').decode(await res.arrayBuffer());
  return parseCabinetOfficeHolidaysCsv(text);
}
