/**
 * JST 日付ユーティリティ
 *
 * @description 日本時間（JST: UTC+9）での日付操作
 * Intl.DateTimeFormat を使用してタイムゾーン処理を行う
 */

/**
 * YYYY-MM-DD 形式のフォーマッター（JST）
 * sv-SE ロケールは ISO 8601 形式（YYYY-MM-DD）を出力する
 */
const jstDateFormatter = new Intl.DateTimeFormat('sv-SE', {
  timeZone: 'Asia/Tokyo',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

/**
 * 日時の各部分を取得するフォーマッター（JST）
 */
const jstPartsFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: 'Asia/Tokyo',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

/**
 * 現在のJST日付を取得（YYYY-MM-DD形式）
 */
export function getJSTDate(date: Date = new Date()): string {
  return jstDateFormatter.format(date);
}

/**
 * 現在のJST日時を取得（ISO 8601形式）
 */
export function getJSTDateTime(date: Date = new Date()): string {
  const parts = jstPartsFormatter.formatToParts(date);
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? '00';

  const year = get('year');
  const month = get('month');
  const day = get('day');
  const hour = get('hour');
  const minute = get('minute');
  const second = get('second');

  return `${year}-${month}-${day}T${hour}:${minute}:${second}+09:00`;
}

/**
 * YYYY-MM-DD 形式の文字列を Date オブジェクトに変換（JST 00:00:00）
 */
export function parseJSTDate(dateStr: string): Date {
  const [year, month, day] = dateStr.split('-').map(Number);
  // JST 00:00:00 を UTC で表現（UTC 15:00:00 前日）
  return new Date(Date.UTC(year, month - 1, day, -9, 0, 0));
}

/**
 * 日付を n 日加算/減算（YYYY-MM-DD形式）
 */
export function addDays(dateStr: string, days: number): string {
  const date = parseJSTDate(dateStr);
  date.setTime(date.getTime() + days * 24 * 60 * 60 * 1000);
  return getJSTDate(date);
}

/**
 * 日付範囲を生成（YYYY-MM-DD形式の配列）
 *
 * @param startDate 開始日（含む）
 * @param endDate 終了日（含む）
 */
export function generateDateRange(startDate: string, endDate: string): string[] {
  const dates: string[] = [];
  let current = startDate;

  while (current <= endDate) {
    dates.push(current);
    current = addDays(current, 1);
  }

  return dates;
}

/**
 * 2つの日付の差分を日数で取得
 */
export function diffDays(dateStr1: string, dateStr2: string): number {
  const date1 = parseJSTDate(dateStr1);
  const date2 = parseJSTDate(dateStr2);
  const diffMs = date1.getTime() - date2.getTime();
  return Math.floor(diffMs / (24 * 60 * 60 * 1000));
}

/**
 * 日付が有効な形式（YYYY-MM-DD）かチェック
 */
export function isValidDateFormat(dateStr: string): boolean {
  const regex = /^\d{4}-\d{2}-\d{2}$/;
  if (!regex.test(dateStr)) {
    return false;
  }

  const [year, month, day] = dateStr.split('-').map(Number);
  const date = new Date(year, month - 1, day);

  return (
    date.getFullYear() === year &&
    date.getMonth() === month - 1 &&
    date.getDate() === day
  );
}

/**
 * 日付を YYYYMMDD 形式に変換（APIパラメータ用）
 */
export function toCompactDate(dateStr: string): string {
  return dateStr.replace(/-/g, '');
}

/**
 * YYYYMMDD 形式を YYYY-MM-DD 形式に変換
 */
export function fromCompactDate(compactDate: string): string {
  return `${compactDate.slice(0, 4)}-${compactDate.slice(4, 6)}-${compactDate.slice(6, 8)}`;
}
