/** 信用残seedの週末営業日抽出（universeモードの日付ループ対象）を検証する。 */

import { describe, expect, it } from 'vitest';
import { toWeekEndDates } from '../../../scripts/seed/weekly-margin-interest';

describe('toWeekEndDates', () => {
  it('カレンダー週（月〜日）ごとに最終営業日を返す', () => {
    // 2026-07-06(月)〜2026-07-17(金)の2週間（平日のみ）
    const businessDays = [
      '2026-07-06', '2026-07-07', '2026-07-08', '2026-07-09', '2026-07-10',
      '2026-07-13', '2026-07-14', '2026-07-15', '2026-07-16', '2026-07-17',
    ];

    expect(toWeekEndDates(businessDays)).toEqual(['2026-07-10', '2026-07-17']);
  });

  it('金曜が祝日の週は木曜を週末営業日とする', () => {
    const businessDays = [
      '2026-07-06', '2026-07-07', '2026-07-08', '2026-07-09',
      '2026-07-13', '2026-07-14', '2026-07-15', '2026-07-16', '2026-07-17',
    ];

    expect(toWeekEndDates(businessDays)).toEqual(['2026-07-09', '2026-07-17']);
  });

  it('入力順序に依存せず昇順で返す', () => {
    const businessDays = ['2026-07-17', '2026-07-06', '2026-07-10', '2026-07-13'];

    expect(toWeekEndDates(businessDays)).toEqual(['2026-07-10', '2026-07-17']);
  });

  it('空配列は空配列を返す', () => {
    expect(toWeekEndDates([])).toEqual([]);
  });
});
