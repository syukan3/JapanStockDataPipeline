import { describe, it, expect } from 'vitest';
import {
  estimateBenefitValue,
  estimateCategory,
  parseYutaiListPage,
  detectMaxPage,
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
  const makeEntry = (code: string, name: string, benefit: string, extra = '') =>
    `<div class="table_tr">
      <div><span>${name}</span></div>
      <div class="table_tr_inner"><div class="table_tr_info">
        <p><a href="/kobetu/${code}.html">${name}</a>（${code}）</p>
        <p>【優待内容】${benefit}</p>
        ${extra}
      </div></div>
    </div>`;

  it('div構造から優待情報をパース', () => {
    const html = makeEntry('2914', 'JT', '自社グループ商品（食品詰め合わせ 2,500円相当）');
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

  it('複数エントリをパース', () => {
    const html = makeEntry('2914', 'JT', '食品 2,500円相当')
      + makeEntry('8591', 'オリックス', 'カタログギフト');
    const results = parseYutaiListPage(html, 3);
    expect(results).toHaveLength(2);
    expect(results[0].local_code).toBe('29140');
    expect(results[1].local_code).toBe('85910');
  });

  it('5桁コードはそのまま使用', () => {
    const html = makeEntry('29140', 'JT', '食品');
    const results = parseYutaiListPage(html, 6);
    expect(results[0].local_code).toBe('29140');
  });

  it('コードなしブロックはスキップ', () => {
    const html = '<div class="table_tr"><div>ヘッダー</div></div>';
    const results = parseYutaiListPage(html, 6);
    expect(results).toHaveLength(0);
  });

  it('空HTML → 空配列', () => {
    expect(parseYutaiListPage('', 1)).toHaveLength(0);
  });

  it('優待内容なしはスキップ', () => {
    const html = `<div class="table_tr">
      <div class="table_tr_inner"><div class="table_tr_info">
        <p><a href="/kobetu/2914.html">JT</a>（2914）</p>
      </div></div>
    </div>`;
    const results = parseYutaiListPage(html, 1);
    expect(results).toHaveLength(0);
  });

  it('株数がテキストにあれば抽出', () => {
    const html = makeEntry('2914', 'JT', '食品 500円', '<p>500株以上</p>');
    const results = parseYutaiListPage(html, 6);
    expect(results[0].min_shares).toBe(500);
  });
});

describe('detectMaxPage', () => {
  it('ページネーションリンクから最大ページ数を検出', () => {
    const html = `<div class="pagination">
      <span class="pageNow">1</span>
      <a href="february2.html">2</a>
      <a href="february3.html">3</a>
      <a href="february8.html">8</a>
    </div>`;
    expect(detectMaxPage(html)).toBe(8);
  });

  it('ページネーションなし → 1', () => {
    expect(detectMaxPage('<div>no pagination</div>')).toBe(1);
  });
});
