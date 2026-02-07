import { describe, it, expect } from 'vitest';
import {
  estimateBenefitValue,
  estimateCategory,
  parseYutaiListPage,
} from '../../lib/yutai/kabuyutai-client';

describe('estimateBenefitValue', () => {
  it('「1,000円相当」→ 1000', () => {
    expect(estimateBenefitValue('QUOカード 1,000円相当')).toBe(1000);
  });

  it('「2000円分」→ 2000', () => {
    expect(estimateBenefitValue('商品券2000円分')).toBe(2000);
  });

  it('「500円」→ 500', () => {
    expect(estimateBenefitValue('クオカード 500円')).toBe(500);
  });

  it('「3,000円」→ 3000', () => {
    expect(estimateBenefitValue('食事券 3,000円')).toBe(3000);
  });

  it('金額なし → null', () => {
    expect(estimateBenefitValue('自社製品詰め合わせ')).toBeNull();
  });

  it('異常な金額（100万以上）→ null', () => {
    expect(estimateBenefitValue('1,500,000円')).toBeNull();
  });

  it('0円 → null', () => {
    expect(estimateBenefitValue('0円')).toBeNull();
  });
});

describe('estimateCategory', () => {
  it('食品を検出', () => {
    expect(estimateCategory('お米5kg')).toBe('食品');
  });

  it('金券を検出', () => {
    expect(estimateCategory('QUOカード 1,000円')).toBe('金券');
  });

  it('優待券を検出', () => {
    expect(estimateCategory('入場券 3,000円')).toBe('優待券');
  });

  it('カタログを検出', () => {
    expect(estimateCategory('カタログギフト 3,000円相当')).toBe('カタログ');
  });

  it('自社製品を検出', () => {
    expect(estimateCategory('自社製品詰め合わせ')).toBe('自社製品');
  });

  it('その他にフォールバック', () => {
    expect(estimateCategory('特別なもの')).toBe('その他');
  });
});

describe('parseYutaiListPage', () => {
  it('テーブル行から優待情報をパース', () => {
    const html = `
<table>
  <tr>
    <td>2914</td>
    <td>JT</td>
    <td>100株</td>
    <td>自社グループ商品（食品詰め合わせ 2,500円相当）</td>
  </tr>
</table>`;

    const results = parseYutaiListPage(html, 6);
    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({
      local_code: '29140',
      company_name: 'JT',
      min_shares: 100,
      benefit_content: '自社グループ商品（食品詰め合わせ 2,500円相当）',
      benefit_value: 2500,
      record_month: 6,
      record_day: 'end',
      category: '食品',
    });
  });

  it('複数行をパース', () => {
    const html = `
<table>
  <tr><td>2914</td><td>JT</td><td>100株</td><td>食品 2,500円相当</td></tr>
  <tr><td>8591</td><td>オリックス</td><td>100株</td><td>カタログギフト</td></tr>
</table>`;

    const results = parseYutaiListPage(html, 3);
    expect(results).toHaveLength(2);
    expect(results[0].local_code).toBe('29140');
    expect(results[1].local_code).toBe('85910');
  });

  it('5桁コードはそのまま使用', () => {
    const html = '<table><tr><td>29140</td><td>JT</td><td>100株</td><td>食品</td></tr></table>';
    const results = parseYutaiListPage(html, 6);
    expect(results[0].local_code).toBe('29140');
  });

  it('コードなし行はスキップ', () => {
    const html = '<table><tr><td>ヘッダー</td><td>企業名</td></tr></table>';
    const results = parseYutaiListPage(html, 6);
    expect(results).toHaveLength(0);
  });

  it('空HTML → 空配列', () => {
    expect(parseYutaiListPage('', 1)).toHaveLength(0);
  });

  it('3セル未満の行はスキップ', () => {
    const html = '<table><tr><td>2914</td><td>JT</td></tr></table>';
    const results = parseYutaiListPage(html, 1);
    expect(results).toHaveLength(0);
  });
});
