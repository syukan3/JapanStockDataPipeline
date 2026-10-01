/**
 * calendar/generated-calendar.ts のユニットテスト
 */

import { describe, it, expect } from 'vitest';
import {
  parseCabinetOfficeHolidaysCsv,
  isGeneratedBusinessDay,
  buildGeneratedCalendar,
} from '@/lib/calendar/generated-calendar';

const CSV = [
  '国民の祝日・休日月日,国民の祝日・休日名称',
  '2026/1/1,元日',
  '2026/9/21,敬老の日',
  '2026/9/22,休日',
  '2026/9/23,秋分の日',
  '2027/1/1,元日',
  '2027/11/23,勤労感謝の日',
].join('\r\n');

describe('parseCabinetOfficeHolidaysCsv', () => {
  it('日付を YYYY-MM-DD に揃え、最後の年を返す', () => {
    const t = parseCabinetOfficeHolidaysCsv(CSV);
    expect(t.dates.has('2026-09-21')).toBe(true);
    expect(t.dates.has('2027-11-23')).toBe(true);
    expect(t.lastYear).toBe(2027);
  });

  it('見出しが違えば例外', () => {
    expect(() => parseCabinetOfficeHolidaysCsv('date,name\n2026/1/1,x')).toThrow(/見出し/);
  });

  it('壊れた行があれば例外（黙って祝日を落とさない）', () => {
    expect(() => parseCabinetOfficeHolidaysCsv(`${CSV}\n2026-12-23,壊れた行`)).toThrow(/解析できません/);
  });
});

describe('isGeneratedBusinessDay', () => {
  const t = parseCabinetOfficeHolidaysCsv(CSV);

  it('平日は営業日', () => {
    expect(isGeneratedBusinessDay('2026-09-24', t)).toBe(true);
  });

  it('土日・祝日・休日は非営業日', () => {
    expect(isGeneratedBusinessDay('2026-09-19', t)).toBe(false); // 土
    expect(isGeneratedBusinessDay('2026-09-20', t)).toBe(false); // 日
    expect(isGeneratedBusinessDay('2026-09-21', t)).toBe(false); // 敬老の日
    expect(isGeneratedBusinessDay('2026-09-22', t)).toBe(false); // 国民の休日
    expect(isGeneratedBusinessDay('2026-09-23', t)).toBe(false); // 秋分の日
  });

  it('年末年始（12/31〜1/3）は平日でも非営業日', () => {
    expect(isGeneratedBusinessDay('2026-12-31', t)).toBe(false); // 木
    expect(isGeneratedBusinessDay('2027-01-04', t)).toBe(true); // 月（大発会）
    expect(isGeneratedBusinessDay('2026-12-30', t)).toBe(true); // 水（大納会）
  });
});

describe('buildGeneratedCalendar', () => {
  const t = parseCabinetOfficeHolidaysCsv(CSV);

  it('hol_div と source=generated を付ける', () => {
    const rows = buildGeneratedCalendar('2026-09-18', '2026-09-24', t);
    expect(rows.map((r) => [r.calendar_date, r.hol_div])).toEqual([
      ['2026-09-18', '1'],
      ['2026-09-19', '0'],
      ['2026-09-20', '0'],
      ['2026-09-21', '0'],
      ['2026-09-22', '0'],
      ['2026-09-23', '0'],
      ['2026-09-24', '1'],
    ]);
    expect(rows.every((r) => r.source === 'generated' && r.is_business_day === (r.hol_div === '1'))).toBe(true);
  });

  it('CSV が収録していない年は作らない（祝日未公表の年を営業日と誤らない）', () => {
    const rows = buildGeneratedCalendar('2027-12-30', '2028-01-10', t);
    expect(rows.map((r) => r.calendar_date)).toEqual(['2027-12-30', '2027-12-31']);
  });
});
