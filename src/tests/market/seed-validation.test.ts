/**
 * market/seed-validation.ts のユニットテスト
 *
 * seed の検証が統計出力だけでなく閾値ゲートとして機能する（大乖離・照合不足で
 * passed=false になり seed が失敗する）ことを固定する。
 */

import { describe, it, expect } from 'vitest';
import {
  validateAgainstReference,
  type BreadthComparable,
  type ReferenceRow,
} from '@/lib/market/seed-validation';

function breadthRow(date: string, ratio: number | null): BreadthComparable {
  return {
    as_of_date: date,
    adv_dec_ratio_25d: ratio,
    new_highs: 50,
    new_lows: 5,
    prime_turnover_value: 8_000_000_000_000,
  };
}

function refRow(date: string, ratio: number | null): ReferenceRow {
  return { date, refAdvDecRatio: ratio, refNewHighs: 55, refNewLows: 5, refTurnoverMn: 6_000_000 };
}

function dates(n: number): string[] {
  return Array.from({ length: n }, (_, i) => `2026-01-${String((i % 28) + 1).padStart(2, '0')}#${Math.floor(i / 28)}`);
}

describe('validateAgainstReference', () => {
  it('乖離が閾値内なら passed=true', () => {
    const ds = dates(50);
    const breadth = ds.map((d) => breadthRow(d, 100.1));
    const refs = ds.map((d) => refRow(d, 100.0));
    const result = validateAgainstReference(breadth, refs);
    expect(result.passed).toBe(true);
    expect(result.ratioAbsDiff?.n).toBe(50);
  });

  it('系統的な大乖離（営業日欠落を模擬）で passed=false', () => {
    const ds = dates(50);
    const breadth = ds.map((d) => breadthRow(d, 108.0)); // 全日 8pt ズレ
    const refs = ds.map((d) => refRow(d, 100.0));
    const result = validateAgainstReference(breadth, refs);
    expect(result.passed).toBe(false);
    expect(result.failureReasons.join()).toContain('平均乖離');
  });

  it('p95 のみの逸脱でも passed=false', () => {
    const ds = dates(100);
    const breadth = ds.map((d, i) => breadthRow(d, i < 90 ? 100.0 : 110.0));
    const refs = ds.map((d) => refRow(d, 100.0));
    const result = validateAgainstReference(breadth, refs);
    expect(result.passed).toBe(false);
    expect(result.failureReasons.join()).toContain('p95');
  });

  it('参照側と1日も照合できない場合は passed=false', () => {
    const breadth = dates(30).map((d) => breadthRow(d, 100.0));
    const result = validateAgainstReference(breadth, []);
    expect(result.passed).toBe(false);
    expect(result.failureReasons.join()).toContain('照合');
  });

  it('照合日数が計算行に対して不足すると passed=false', () => {
    const ds = dates(100);
    const breadth = ds.map((d) => breadthRow(d, 100.0));
    // 参照は半分の日しか無い（軸ズレ・参照欠落の疑い）
    const refs = ds.slice(0, 50).map((d) => refRow(d, 100.0));
    const result = validateAgainstReference(breadth, refs);
    expect(result.passed).toBe(false);
    expect(result.failureReasons.join()).toContain('照合日数不足');
  });

  it('breadth が空（フェーズ失敗）ならゲートは通過（breadth側の失敗報告に委ねる）', () => {
    const result = validateAgainstReference([], dates(10).map((d) => refRow(d, 100)));
    expect(result.passed).toBe(true);
    expect(result.ratioAbsDiff).toBeNull();
  });

  it('ratio が全て null（窓不足のみ）ならゲート対象外', () => {
    const breadth = dates(10).map((d) => breadthRow(d, null));
    const result = validateAgainstReference(breadth, dates(10).map((d) => refRow(d, 100)));
    expect(result.passed).toBe(true);
  });
});
