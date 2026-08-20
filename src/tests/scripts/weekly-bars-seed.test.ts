/**
 * scripts/seed/seed-weekly-bars.ts の引数パース・取得期間算出のユニットテスト
 */

import { describe, it, expect } from 'vitest';

import {
  parseWeeklyBarsSeedArgs,
  getBackfillRange,
  BACKFILL_YEARS,
} from '../../../scripts/seed/seed-weekly-bars';

describe('parseWeeklyBarsSeedArgs', () => {
  it('引数なしは追跡銘柄全件・非dry-run', () => {
    expect(parseWeeklyBarsSeedArgs([])).toEqual({ codes: undefined, dryRun: false });
  });

  it('--dry-run を認識する', () => {
    expect(parseWeeklyBarsSeedArgs(['--dry-run'])).toEqual({ codes: undefined, dryRun: true });
  });

  it('--code はカンマ区切り・trim・大文字化・重複排除', () => {
    expect(parseWeeklyBarsSeedArgs(['--code=72030, 285a0,72030'])).toEqual({
      codes: ['72030', '285A0'],
      dryRun: false,
    });
  });

  it('--code と --dry-run は併用できる', () => {
    expect(parseWeeklyBarsSeedArgs(['--code=72030', '--dry-run'])).toEqual({
      codes: ['72030'],
      dryRun: true,
    });
  });

  it('--code が空なら throw', () => {
    expect(() => parseWeeklyBarsSeedArgs(['--code='])).toThrow('銘柄コードが指定されていません');
  });

  it('不明な引数は throw', () => {
    expect(() => parseWeeklyBarsSeedArgs(['--all'])).toThrow('不明な引数です: --all');
  });
});

describe('getBackfillRange', () => {
  it('既定は10年前から当日まで', () => {
    expect(BACKFILL_YEARS).toBe(10);
    expect(getBackfillRange('2026-08-20')).toEqual({ from: '2016-08-20', to: '2026-08-20' });
  });

  it('タイムゾーンに依存しない（UTC計算）', () => {
    const original = process.env.TZ;
    try {
      process.env.TZ = 'Pacific/Kiritimati';
      expect(getBackfillRange('2026-01-01')).toEqual({ from: '2016-01-01', to: '2026-01-01' });
    } finally {
      process.env.TZ = original;
    }
  });

  it('形式不正は throw', () => {
    expect(() => getBackfillRange('2026/08/20')).toThrow('YYYY-MM-DD');
  });
});
