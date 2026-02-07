import { describe, it, expect } from 'vitest';
import { parseCsvLines, parseMarginInventoryCsv } from '../../lib/yutai/kabu-csv-client';

describe('parseCsvLines', () => {
  it('基本的なCSVをパース', () => {
    const csv = 'a,b,c\n1,2,3';
    const result = parseCsvLines(csv);
    expect(result).toEqual([['a', 'b', 'c'], ['1', '2', '3']]);
  });

  it('ダブルクォート付きフィールドをパース', () => {
    const csv = '"hello, world",b,c';
    const result = parseCsvLines(csv);
    expect(result[0][0]).toBe('hello, world');
  });

  it('エスケープされたダブルクォートをパース', () => {
    const csv = '"say ""hello""",b';
    const result = parseCsvLines(csv);
    expect(result[0][0]).toBe('say "hello"');
  });

  it('空行をスキップ', () => {
    const csv = 'a,b\n\nc,d\n';
    const result = parseCsvLines(csv);
    expect(result).toHaveLength(2);
  });

  it('CRLF改行をハンドル', () => {
    const csv = 'a,b\r\nc,d\r\n';
    const result = parseCsvLines(csv);
    expect(result).toHaveLength(2);
  });
});

describe('parseMarginInventoryCsv', () => {
  const inventoryDate = '2024-06-01';

  it('標準的なCSVをパース', () => {
    const csv = `コード,銘柄名,在庫数量,貸借期限
2914,JT,1000,無期限
8591,オリックス,500,14日`;

    const result = parseMarginInventoryCsv(csv, inventoryDate);
    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      local_code: '29140',
      broker: 'esmart',
      inventory_date: '2024-06-01',
      inventory_qty: 1000,
      is_available: true,
      loan_type: 'general',
      loan_term: '無期限',
      premium_fee: null,
      source: 'kabu_csv',
    });
  });

  it('在庫数量がないCSV', () => {
    const csv = `銘柄コード,銘柄名
2914,JT`;

    const result = parseMarginInventoryCsv(csv, inventoryDate);
    expect(result).toHaveLength(1);
    expect(result[0].inventory_qty).toBeNull();
    expect(result[0].is_available).toBe(true); // qty=null → available
  });

  it('プレミアム料付きCSV', () => {
    const csv = `コード,銘柄名,在庫数量,プレミアム料
2914,JT,100,5.5`;

    const result = parseMarginInventoryCsv(csv, inventoryDate);
    expect(result[0].premium_fee).toBe(5.5);
  });

  it('コード列がないCSV → エラー', () => {
    const csv = `銘柄名,在庫数量
JT,100`;

    expect(() => parseMarginInventoryCsv(csv, inventoryDate)).toThrow(
      /CSV header does not contain code column/,
    );
  });

  it('ヘッダーのみ → 空配列', () => {
    const csv = 'コード,銘柄名';
    const result = parseMarginInventoryCsv(csv, inventoryDate);
    expect(result).toHaveLength(0);
  });

  it('空CSV → 空配列', () => {
    const result = parseMarginInventoryCsv('', inventoryDate);
    expect(result).toHaveLength(0);
  });

  it('不正なコード行をスキップ', () => {
    const csv = `コード,銘柄名
abc,テスト
2914,JT`;

    const result = parseMarginInventoryCsv(csv, inventoryDate);
    expect(result).toHaveLength(1);
    expect(result[0].local_code).toBe('29140');
  });

  it('在庫0 → is_available = false', () => {
    const csv = `コード,銘柄名,在庫数量
2914,JT,0`;

    const result = parseMarginInventoryCsv(csv, inventoryDate);
    expect(result[0].is_available).toBe(false);
  });

  it('5桁コードはそのまま使用', () => {
    const csv = `コード,銘柄名
29140,JT`;

    const result = parseMarginInventoryCsv(csv, inventoryDate);
    expect(result[0].local_code).toBe('29140');
  });
});
