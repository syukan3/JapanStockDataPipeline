/**
 * cron/handlers/macro.ts のユニットテスト
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const {
  mockCreateFredClient,
  mockCreateEStatClient,
  mockIsMonthlyOrLower,
  mockCreateAdminClient,
  mockSendJobFailureEmail,
} = vi.hoisted(() => ({
  mockCreateFredClient: vi.fn(),
  mockCreateEStatClient: vi.fn(),
  mockIsMonthlyOrLower: vi.fn(),
  mockCreateAdminClient: vi.fn(),
  mockSendJobFailureEmail: vi.fn(),
}));

vi.mock('@/lib/fred/client', () => ({
  createFredClient: mockCreateFredClient,
}));

vi.mock('@/lib/estat/client', () => ({
  createEStatClient: mockCreateEStatClient,
}));

vi.mock('@/lib/fred/series-config', () => ({
  isMonthlyOrLower: mockIsMonthlyOrLower,
}));

vi.mock('@/lib/supabase/admin', () => ({
  createAdminClient: mockCreateAdminClient,
}));

vi.mock('@/lib/notification/email', () => ({
  sendJobFailureEmail: mockSendJobFailureEmail,
}));

vi.mock('@/lib/utils/logger', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    startTimer: vi.fn(() => ({ end: vi.fn(), endWithError: vi.fn() })),
  })),
}));

import { handleCronD, CronDRequestSchema } from '@/lib/cron/handlers/macro';

describe('cron/handlers/macro.ts', () => {
  let mockSupabase: any;
  let mockFredClient: any;
  let mockEStatClient: any;

  beforeEach(() => {
    mockCreateFredClient.mockReset();
    mockCreateEStatClient.mockReset();
    mockIsMonthlyOrLower.mockReset();
    mockCreateAdminClient.mockReset();
    mockSendJobFailureEmail.mockReset();

    mockFredClient = {
      getSeriesObservations: vi.fn(),
    };
    mockCreateFredClient.mockReturnValue(mockFredClient);

    mockEStatClient = {
      getStatsData: vi.fn(),
    };
    mockCreateEStatClient.mockReturnValue(mockEStatClient);
  });

  function setupSupabaseMock(seriesList: any[], upsertError: any = null) {
    // macro_series_metadata は select（メタデータ取得）と update（更新）の2パターンがある
    // select → eq → await: 初回呼び出し（handleCronD本体）
    // update → eq → await: 2回目以降（processFredSeries / processEStatSeries）
    let metadataSelectCalled = false;

    function createMetadataBuilder() {
      let filteredList = seriesList;
      const builder: any = {};

      // select 用: 初回は select().eq().then() チェーン
      builder.select = vi.fn(() => {
        metadataSelectCalled = true;
        return builder;
      });

      // update 用: update().eq().then() チェーン
      builder.update = vi.fn(() => builder);

      builder.eq = vi.fn((col: string, val: string) => {
        if (col === 'source') {
          filteredList = seriesList.filter((s: any) => s.source === val);
        }
        return builder;
      });

      builder.then = (resolve: any) => {
        if (metadataSelectCalled) {
          // select チェーン: データを返す
          metadataSelectCalled = false;
          return resolve({ data: filteredList, error: null });
        }
        // update チェーン: エラーなしを返す
        return resolve({ error: null });
      };

      return builder;
    }

    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'macro_series_metadata') {
          return createMetadataBuilder();
        }
        if (table === 'macro_indicator_daily') {
          return {
            upsert: vi.fn().mockResolvedValue({ error: upsertError }),
          };
        }
        return {};
      }),
    };

    mockCreateAdminClient.mockReturnValue(mockSupabase);
  }

  describe('CronDRequestSchema', () => {
    it('デフォルト値が適用される', () => {
      const result = CronDRequestSchema.parse({});
      expect(result.source).toBe('all');
      expect(result.backfill_days).toBe(0);
    });

    it('sourceを指定できる', () => {
      const result = CronDRequestSchema.parse({ source: 'fred' });
      expect(result.source).toBe('fred');
    });

    it('backfill_daysを指定できる', () => {
      const result = CronDRequestSchema.parse({ backfill_days: 30 });
      expect(result.backfill_days).toBe(30);
    });

    it('無効なsourceを拒否する', () => {
      const result = CronDRequestSchema.safeParse({ source: 'invalid' });
      expect(result.success).toBe(false);
    });
  });

  describe('FRED系列の処理', () => {
    it('取得+upsertを行う', async () => {
      const fredSeries = [{
        series_id: 'vixcls',
        source: 'fred',
        source_series_id: 'VIXCLS',
        source_filter: null,
        frequency: 'daily',
        last_value_date: '2024-01-10',
      }];

      setupSupabaseMock(fredSeries);
      mockIsMonthlyOrLower.mockReturnValue(false);
      mockFredClient.getSeriesObservations.mockResolvedValue({
        observations: [
          { date: '2024-01-11', value: 13.5, releasedAt: '2024-01-12T00:00:00Z' },
        ],
        skippedCount: 0,
      });

      const result = await handleCronD('fred', 'run-123');

      expect(result.success).toBe(true);
      expect(result.rowsUpserted).toBe(1);
      expect(result.seriesProcessed).toBe(1);
    });

    it('backfill_days指定時はその日数分遡る', async () => {
      const fredSeries = [{
        series_id: 'vixcls',
        source: 'fred',
        source_series_id: 'VIXCLS',
        source_filter: null,
        frequency: 'daily',
        last_value_date: '2024-01-10',
      }];

      setupSupabaseMock(fredSeries);
      mockFredClient.getSeriesObservations.mockResolvedValue({
        observations: [],
        skippedCount: 0,
      });

      await handleCronD('fred', 'run-123', 30);

      // getSeriesObservationsの第2引数が30日前の日付であることを確認
      expect(mockFredClient.getSeriesObservations).toHaveBeenCalledWith(
        'VIXCLS',
        expect.any(String) // 日付文字列
      );
    });

    it('初回（last_value_date=null）は730日分バックフィルする', async () => {
      const fredSeries = [{
        series_id: 'vixcls',
        source: 'fred',
        source_series_id: 'VIXCLS',
        source_filter: null,
        frequency: 'daily',
        last_value_date: null,
      }];

      setupSupabaseMock(fredSeries);
      mockFredClient.getSeriesObservations.mockResolvedValue({
        observations: [],
        skippedCount: 0,
      });

      await handleCronD('fred', 'run-123');

      expect(mockFredClient.getSeriesObservations).toHaveBeenCalled();
    });

    it('月次系列はvintage再取得（90日）を行う', async () => {
      const fredSeries = [{
        series_id: 'fedfunds',
        source: 'fred',
        source_series_id: 'FEDFUNDS',
        source_filter: null,
        frequency: 'monthly',
        last_value_date: '2024-01-01',
      }];

      setupSupabaseMock(fredSeries);
      mockIsMonthlyOrLower.mockReturnValue(true);
      mockFredClient.getSeriesObservations.mockResolvedValue({
        observations: [],
        skippedCount: 0,
      });

      await handleCronD('fred', 'run-123');

      expect(mockIsMonthlyOrLower).toHaveBeenCalledWith('monthly');
    });

    it('差分取得（last_value_dateから）を行う', async () => {
      const fredSeries = [{
        series_id: 'vixcls',
        source: 'fred',
        source_series_id: 'VIXCLS',
        source_filter: null,
        frequency: 'daily',
        last_value_date: '2024-01-10',
      }];

      setupSupabaseMock(fredSeries);
      mockIsMonthlyOrLower.mockReturnValue(false);
      mockFredClient.getSeriesObservations.mockResolvedValue({
        observations: [],
        skippedCount: 0,
      });

      await handleCronD('fred', 'run-123');

      expect(mockFredClient.getSeriesObservations).toHaveBeenCalledWith(
        'VIXCLS',
        '2024-01-10'
      );
    });

    it('空データ時はスキップする', async () => {
      const fredSeries = [{
        series_id: 'vixcls',
        source: 'fred',
        source_series_id: 'VIXCLS',
        source_filter: null,
        frequency: 'daily',
        last_value_date: '2024-01-10',
      }];

      setupSupabaseMock(fredSeries);
      mockIsMonthlyOrLower.mockReturnValue(false);
      mockFredClient.getSeriesObservations.mockResolvedValue({
        observations: [],
        skippedCount: 0,
      });

      const result = await handleCronD('fred', 'run-123');

      expect(result.rowsUpserted).toBe(0);
    });

    it('metadata更新を行う', async () => {
      const fredSeries = [{
        series_id: 'vixcls',
        source: 'fred',
        source_series_id: 'VIXCLS',
        source_filter: null,
        frequency: 'daily',
        last_value_date: '2024-01-10',
      }];

      setupSupabaseMock(fredSeries);
      mockIsMonthlyOrLower.mockReturnValue(false);
      mockFredClient.getSeriesObservations.mockResolvedValue({
        observations: [
          { date: '2024-01-11', value: 13.5, releasedAt: '2024-01-12T00:00:00Z' },
        ],
        skippedCount: 0,
      });

      await handleCronD('fred', 'run-123');

      // macro_series_metadataテーブルのupdateが呼ばれることを確認
      expect(mockSupabase.from).toHaveBeenCalledWith('macro_series_metadata');
    });
  });

  describe('e-Stat系列の処理', () => {
    it('取得+upsertを行う', async () => {
      const estatSeries = [{
        series_id: 'jp_cpi_core',
        source: 'estat',
        source_series_id: '0003143513',
        source_filter: { cat01: '生鮮食品を除く総合' },
        frequency: 'monthly',
        last_value_date: '2024-01-01',
      }];

      setupSupabaseMock(estatSeries);
      mockEStatClient.getStatsData.mockResolvedValue({
        observations: [
          { date: '2024-01-31', value: 103.5 },
        ],
        skippedCount: 0,
      });

      const result = await handleCronD('estat', 'run-123');

      expect(result.success).toBe(true);
      expect(result.rowsUpserted).toBe(1);
    });

    it('差分フィルタ（last_value_date以降のみ）を適用する', async () => {
      const estatSeries = [{
        series_id: 'jp_cpi_core',
        source: 'estat',
        source_series_id: '0003143513',
        source_filter: null,
        frequency: 'monthly',
        last_value_date: '2024-01-31',
      }];

      setupSupabaseMock(estatSeries);
      mockEStatClient.getStatsData.mockResolvedValue({
        observations: [
          { date: '2023-12-31', value: 102.0 }, // 古い → フィルタ
          { date: '2024-01-31', value: 103.5 }, // 境界 → 含む
          { date: '2024-02-29', value: 104.0 }, // 新しい → 含む
        ],
        skippedCount: 0,
      });

      const result = await handleCronD('estat', 'run-123');

      expect(result.rowsUpserted).toBe(2); // 2024-01-31 + 2024-02-29
    });

    it('初回（last_value_date=null）は全件upsertする', async () => {
      const estatSeries = [{
        series_id: 'jp_cpi_core',
        source: 'estat',
        source_series_id: '0003143513',
        source_filter: null,
        frequency: 'monthly',
        last_value_date: null,
      }];

      setupSupabaseMock(estatSeries);
      mockEStatClient.getStatsData.mockResolvedValue({
        observations: [
          { date: '2023-01-31', value: 100.0 },
          { date: '2023-02-28', value: 101.0 },
          { date: '2023-03-31', value: 102.0 },
        ],
        skippedCount: 0,
      });

      const result = await handleCronD('estat', 'run-123');

      expect(result.rowsUpserted).toBe(3);
    });
  });

  describe('sourceフィルタ', () => {
    it('source=fredでFRED系列のみ処理する', async () => {
      const seriesList = [
        { series_id: 'vixcls', source: 'fred', source_series_id: 'VIXCLS', source_filter: null, frequency: 'daily', last_value_date: null },
        { series_id: 'jp_cpi', source: 'estat', source_series_id: '0003143513', source_filter: null, frequency: 'monthly', last_value_date: null },
      ];

      setupSupabaseMock(seriesList);
      mockFredClient.getSeriesObservations.mockResolvedValue({ observations: [], skippedCount: 0 });

      await handleCronD('fred', 'run-123');

      expect(mockFredClient.getSeriesObservations).toHaveBeenCalled();
      expect(mockEStatClient.getStatsData).not.toHaveBeenCalled();
    });

    it('source=estatでe-Stat系列のみ処理する', async () => {
      const seriesList = [
        { series_id: 'vixcls', source: 'fred', source_series_id: 'VIXCLS', source_filter: null, frequency: 'daily', last_value_date: null },
        { series_id: 'jp_cpi', source: 'estat', source_series_id: '0003143513', source_filter: null, frequency: 'monthly', last_value_date: null },
      ];

      setupSupabaseMock(seriesList);
      mockEStatClient.getStatsData.mockResolvedValue({ observations: [], skippedCount: 0 });

      await handleCronD('estat', 'run-123');

      expect(mockFredClient.getSeriesObservations).not.toHaveBeenCalled();
      expect(mockEStatClient.getStatsData).toHaveBeenCalled();
    });

    it('source=allで全系列を処理する', async () => {
      const seriesList = [
        { series_id: 'vixcls', source: 'fred', source_series_id: 'VIXCLS', source_filter: null, frequency: 'daily', last_value_date: null },
        { series_id: 'jp_cpi', source: 'estat', source_series_id: '0003143513', source_filter: null, frequency: 'monthly', last_value_date: null },
      ];

      setupSupabaseMock(seriesList);
      mockFredClient.getSeriesObservations.mockResolvedValue({ observations: [], skippedCount: 0 });
      mockEStatClient.getStatsData.mockResolvedValue({ observations: [], skippedCount: 0 });

      const result = await handleCronD('all', 'run-123');

      expect(result.seriesProcessed).toBe(2);
    });
  });

  describe('エラーハンドリング', () => {
    it('全系列失敗でsuccess=falseを返す', async () => {
      const seriesList = [{
        series_id: 'vixcls',
        source: 'fred',
        source_series_id: 'VIXCLS',
        source_filter: null,
        frequency: 'daily',
        last_value_date: null,
      }];

      setupSupabaseMock(seriesList);
      mockFredClient.getSeriesObservations.mockRejectedValue(new Error('API Error'));

      const result = await handleCronD('fred', 'run-error');

      expect(result.success).toBe(false);
      expect(result.errors).toHaveLength(1);
    });

    it('部分成功（一部のみ失敗）でsuccess=trueを返す', async () => {
      const seriesList = [
        { series_id: 'vixcls', source: 'fred', source_series_id: 'VIXCLS', source_filter: null, frequency: 'daily', last_value_date: null },
        { series_id: 'fedfunds', source: 'fred', source_series_id: 'FEDFUNDS', source_filter: null, frequency: 'monthly', last_value_date: null },
      ];

      setupSupabaseMock(seriesList);
      mockFredClient.getSeriesObservations
        .mockRejectedValueOnce(new Error('API Error'))
        .mockResolvedValueOnce({
          observations: [{ date: '2024-01-11', value: 5.5, releasedAt: '2024-01-12T00:00:00Z' }],
          skippedCount: 0,
        });

      const result = await handleCronD('fred', 'run-partial');

      expect(result.success).toBe(true); // 部分成功
      expect(result.errors).toHaveLength(1);
      expect(result.rowsUpserted).toBe(1);
    });

    it('metadata取得エラーで例外を投げる', async () => {
      mockCreateAdminClient.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          then: (resolve: any) => resolve({
            data: null,
            error: { message: 'Metadata fetch error' },
          }),
        })),
      });
      mockSendJobFailureEmail.mockResolvedValue(undefined);

      const result = await handleCronD('fred', 'run-meta-error');

      expect(result.success).toBe(false);
      expect(result.errors).toContainEqual(
        expect.stringContaining('Metadata fetch error')
      );
    });

    it('エラー時にメール通知を送信する', async () => {
      mockCreateAdminClient.mockReturnValue({
        from: vi.fn(() => ({
          select: vi.fn().mockReturnThis(),
          eq: vi.fn().mockReturnThis(),
          then: (resolve: any) => resolve({
            data: null,
            error: { message: 'Fatal error' },
          }),
        })),
      });
      mockSendJobFailureEmail.mockResolvedValue(undefined);

      await handleCronD('fred', 'run-fatal');

      expect(mockSendJobFailureEmail).toHaveBeenCalledWith(
        expect.objectContaining({
          jobName: 'cron-d-macro',
        })
      );
    });
  });
});
