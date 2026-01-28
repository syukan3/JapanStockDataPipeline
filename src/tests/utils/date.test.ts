import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  getJSTDate,
  getJSTDateTime,
  parseJSTDate,
  addDays,
  generateDateRange,
  diffDays,
  isValidDateFormat,
  toCompactDate,
  fromCompactDate,
} from '@/lib/utils/date';

describe('date.ts', () => {
  describe('getJSTDate', () => {
    it('現在日時（UTC 10:00）をJSTで当日として返す', () => {
      // UTC 2024-01-15 10:00:00 → JST 2024-01-15 19:00:00
      const date = new Date('2024-01-15T10:00:00Z');
      expect(getJSTDate(date)).toBe('2024-01-15');
    });

    it('日付跨ぎ（UTC 15:00）をJSTで翌日として返す', () => {
      // UTC 2024-01-15 15:00:00 → JST 2024-01-16 00:00:00
      const date = new Date('2024-01-15T15:00:00Z');
      expect(getJSTDate(date)).toBe('2024-01-16');
    });

    it('引数なしで現在日時を使用する', () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2024-06-20T10:00:00Z'));

      expect(getJSTDate()).toBe('2024-06-20');

      vi.useRealTimers();
    });
  });

  describe('getJSTDateTime', () => {
    it('ISO 8601形式で+09:00タイムゾーン付きを返す', () => {
      // UTC 2024-01-15 10:30:45 → JST 2024-01-15 19:30:45
      const date = new Date('2024-01-15T10:30:45Z');
      expect(getJSTDateTime(date)).toBe('2024-01-15T19:30:45+09:00');
    });

    it('日付跨ぎでも正しくフォーマットする', () => {
      // UTC 2024-01-15 15:30:45 → JST 2024-01-16 00:30:45
      const date = new Date('2024-01-15T15:30:45Z');
      expect(getJSTDateTime(date)).toBe('2024-01-16T00:30:45+09:00');
    });

    it('引数なしで現在日時を使用する', () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2024-06-20T10:30:45Z'));

      expect(getJSTDateTime()).toBe('2024-06-20T19:30:45+09:00');

      vi.useRealTimers();
    });
  });

  describe('parseJSTDate', () => {
    it('YYYY-MM-DD形式を正常に変換する', () => {
      const date = parseJSTDate('2024-01-15');
      // JST 00:00:00 = UTC 前日 15:00:00
      expect(date.toISOString()).toBe('2024-01-14T15:00:00.000Z');
    });

    it('月初を正しく変換する', () => {
      const date = parseJSTDate('2024-03-01');
      expect(date.toISOString()).toBe('2024-02-29T15:00:00.000Z');
    });

    it('月末を正しく変換する', () => {
      const date = parseJSTDate('2024-01-31');
      expect(date.toISOString()).toBe('2024-01-30T15:00:00.000Z');
    });
  });

  describe('addDays', () => {
    it('正の加算ができる', () => {
      expect(addDays('2024-01-15', 5)).toBe('2024-01-20');
    });

    it('負の減算ができる', () => {
      expect(addDays('2024-01-15', -5)).toBe('2024-01-10');
    });

    it('月跨ぎの加算ができる', () => {
      expect(addDays('2024-01-30', 5)).toBe('2024-02-04');
    });

    it('年跨ぎの加算ができる', () => {
      expect(addDays('2024-12-30', 5)).toBe('2025-01-04');
    });

    it('閏年: 2024-02-28 +1 → 02-29', () => {
      expect(addDays('2024-02-28', 1)).toBe('2024-02-29');
    });

    it('閏年: 2024-02-29 +1 → 03-01', () => {
      expect(addDays('2024-02-29', 1)).toBe('2024-03-01');
    });
  });

  describe('generateDateRange', () => {
    it('正常範囲の日付配列を生成する', () => {
      const result = generateDateRange('2024-01-15', '2024-01-18');
      expect(result).toEqual([
        '2024-01-15',
        '2024-01-16',
        '2024-01-17',
        '2024-01-18',
      ]);
    });

    it('同日の場合は1要素の配列を返す', () => {
      const result = generateDateRange('2024-01-15', '2024-01-15');
      expect(result).toEqual(['2024-01-15']);
    });

    it('開始日 > 終了日の場合は空配列を返す', () => {
      const result = generateDateRange('2024-01-18', '2024-01-15');
      expect(result).toEqual([]);
    });
  });

  describe('diffDays', () => {
    it('正の差分（date1 > date2）', () => {
      expect(diffDays('2024-01-20', '2024-01-15')).toBe(5);
    });

    it('負の差分（date1 < date2）', () => {
      expect(diffDays('2024-01-15', '2024-01-20')).toBe(-5);
    });

    it('同日の場合は0', () => {
      expect(diffDays('2024-01-15', '2024-01-15')).toBe(0);
    });
  });

  describe('isValidDateFormat', () => {
    it('有効形式: 2024-01-15', () => {
      expect(isValidDateFormat('2024-01-15')).toBe(true);
    });

    it('閏年有効: 2024-02-29', () => {
      expect(isValidDateFormat('2024-02-29')).toBe(true);
    });

    it('無効形式: スラッシュ区切り', () => {
      expect(isValidDateFormat('2024/01/15')).toBe(false);
    });

    it('無効形式: 2桁年', () => {
      expect(isValidDateFormat('24-01-15')).toBe(false);
    });

    it('存在しない日: 2024-02-30', () => {
      expect(isValidDateFormat('2024-02-30')).toBe(false);
    });

    it('存在しない日: 非閏年の2/29', () => {
      expect(isValidDateFormat('2023-02-29')).toBe(false);
    });

    it('空文字はfalse', () => {
      expect(isValidDateFormat('')).toBe(false);
    });
  });

  describe('toCompactDate', () => {
    it('YYYY-MM-DD → YYYYMMDD', () => {
      expect(toCompactDate('2024-01-15')).toBe('20240115');
    });
  });

  describe('fromCompactDate', () => {
    it('YYYYMMDD → YYYY-MM-DD', () => {
      expect(fromCompactDate('20240115')).toBe('2024-01-15');
    });

    it('可逆性: toCompact → fromCompact = 元', () => {
      const original = '2024-06-20';
      const compact = toCompactDate(original);
      expect(fromCompactDate(compact)).toBe(original);
    });
  });
});
