/**
 * notification/capacity-report.ts のユニットテスト
 */

import { describe, it, expect } from 'vitest';
import {
  buildCapacityReport,
  FREE_PLAN_DB_LIMIT_MB,
  CAPACITY_WARNING_THRESHOLD_PCT,
  CAPACITY_CRITICAL_THRESHOLD_PCT,
  type CapacityReportInput,
} from '@/lib/notification/capacity-report';

function baseInput(overrides: Partial<CapacityReportInput> = {}): CapacityReportInput {
  return {
    dbSizeMb: 250,
    topTables: [
      { schemaName: 'jquants_core', tableName: 'equity_bar_daily', totalSizeBytes: 326 * 1024 * 1024 },
      { schemaName: 'analytics', tableName: 'stock_metrics', totalSizeBytes: 29 * 1024 * 1024 },
    ],
    equityBarDeadTupleCount: 210_799,
    archival: { executed: false },
    generatedAt: new Date('2026-07-19T18:00:00.000Z'),
    ...overrides,
  };
}

describe('notification/capacity-report.ts', () => {
  it('定数: Free プラン上限は500MB、警告80%/危険90%', () => {
    expect(FREE_PLAN_DB_LIMIT_MB).toBe(500);
    expect(CAPACITY_WARNING_THRESHOLD_PCT).toBe(80);
    expect(CAPACITY_CRITICAL_THRESHOLD_PCT).toBe(90);
  });

  describe('件名の閾値プレフィックス（境界値）', () => {
    it('79.9%（80%未満）はプレフィックスなし', () => {
      const { subject } = buildCapacityReport(baseInput({ dbSizeMb: 399.5 }));

      expect(subject).not.toContain('⚠️');
      expect(subject).not.toContain('🚨');
      expect(subject.startsWith('[DB容量]')).toBe(true);
    });

    it('80%ちょうどは⚠️プレフィックス', () => {
      const { subject, usagePct } = buildCapacityReport(baseInput({ dbSizeMb: 400 }));

      expect(usagePct).toBe(80);
      expect(subject).toContain('⚠️');
      expect(subject).not.toContain('🚨');
    });

    it('89.9%（90%未満）は⚠️のまま（🚨にならない）', () => {
      const { subject } = buildCapacityReport(baseInput({ dbSizeMb: 449.5 }));

      expect(subject).toContain('⚠️');
      expect(subject).not.toContain('🚨');
    });

    it('90%ちょうどは🚨プレフィックス（⚠️は含まない）', () => {
      const { subject, usagePct } = buildCapacityReport(baseInput({ dbSizeMb: 450 }));

      expect(usagePct).toBe(90);
      expect(subject).toContain('🚨');
      expect(subject).not.toContain('⚠️');
    });

    it('50%は通常表示（プレフィックスなし）', () => {
      const { subject } = buildCapacityReport(baseInput({ dbSizeMb: 250 }));

      expect(subject).toBe('[DB容量] 250MB / 500MB (50%)');
    });
  });

  it('freePlanLimitMb を上書きできる', () => {
    const { subject, usagePct } = buildCapacityReport(
      baseInput({ dbSizeMb: 80, freePlanLimitMb: 100 })
    );

    expect(usagePct).toBe(80);
    expect(subject).toContain('80MB / 100MB (80%)');
  });

  describe('アーカイブ実施有無', () => {
    it('未実施の場合は「未実施」と表示する', () => {
      const { html } = buildCapacityReport(baseInput({ archival: { executed: false } }));

      expect(html).toContain('今回は未実施');
      expect(html).not.toContain('退避範囲');
    });

    it('実施した場合は退避範囲・削除行数・削減容量・退避先を表示する', () => {
      const { html } = buildCapacityReport(
        baseInput({
          archival: {
            executed: true,
            archiveRange: '2020-01-06 to 2020-06-30',
            rowsArchived: 123_456,
            savedMb: 42,
            storagePath: 'db-archives/equity_bar_daily/2020-01-06_to_2020-06-30.csv.gz',
          },
        })
      );

      expect(html).toContain('2020-01-06 to 2020-06-30');
      expect(html).toContain('123,456');
      expect(html).toContain('42 MB');
      expect(html).toContain('db-archives/equity_bar_daily/2020-01-06_to_2020-06-30.csv.gz');
    });
  });

  describe('上位テーブル', () => {
    it('スキーマ.テーブル名とMB換算サイズを表示する', () => {
      const { html } = buildCapacityReport(baseInput());

      expect(html).toContain('jquants_core.equity_bar_daily');
      expect(html).toContain('326.0 MB');
      expect(html).toContain('analytics.stock_metrics');
      expect(html).toContain('29.0 MB');
    });

    it('空配列の場合は「データなし」と表示する', () => {
      const { html } = buildCapacityReport(baseInput({ topTables: [] }));

      expect(html).toContain('データなし');
    });

    it('テーブル名をHTMLエスケープする', () => {
      const { html } = buildCapacityReport(
        baseInput({
          topTables: [
            { schemaName: '<script>', tableName: 'alert(1)</script>', totalSizeBytes: 1024 * 1024 },
          ],
        })
      );

      expect(html).not.toContain('<script>alert');
      expect(html).toContain('&lt;script&gt;');
    });
  });

  it('デッドタプル数を表示する', () => {
    const { html } = buildCapacityReport(baseInput({ equityBarDeadTupleCount: 210_799 }));

    expect(html).toContain('210,799');
  });

  it('退避先パスをHTMLエスケープする', () => {
    const { html } = buildCapacityReport(
      baseInput({
        archival: {
          executed: true,
          archiveRange: '<script>x</script>',
          rowsArchived: 1,
          savedMb: 1,
          storagePath: '<script>alert(1)</script>',
        },
      })
    );

    expect(html).not.toContain('<script>alert');
    expect(html).toContain('&lt;script&gt;');
  });

  it('生成時刻をISO文字列で本文に含める', () => {
    const { html } = buildCapacityReport(
      baseInput({ generatedAt: new Date('2026-07-19T18:00:00.000Z') })
    );

    expect(html).toContain('2026-07-19T18:00:00.000Z');
  });
});
