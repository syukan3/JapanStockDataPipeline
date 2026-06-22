/**
 * cron/forward-fill.ts のユニットテスト
 *
 * 対象日決定の境界ケース（overlap by 1 / 上限 / カレンダー stale・null /
 * 空テーブル開始 / 既に最新）を検証する。
 */

import { describe, it, expect, vi } from 'vitest';
import {
  resolveForwardFillDates,
  getMaxDataDate,
  runForwardFill,
  type ForwardFillDatasetConfig,
} from '@/lib/cron/forward-fill';

/**
 * Supabase クライアントの最小モック。
 *
 * @param calendarMaxRow trading_calendar の最新日付クエリの結果（.single 相当, limit(1)）
 * @param calendarRangeRows getBusinessDaysOrThrow が読む営業日レンジ（.order 相当）
 * @param dataMaxRow 実データテーブル最新日付クエリの結果（.limit(1) 相当）
 */
function makeSupabase(opts: {
  calendarMaxRow?: { data: unknown; error: unknown };
  calendarRangeRows?: { data: unknown; error: unknown };
  dataMaxRow?: { data: unknown; error: unknown };
}) {
  return {
    from: vi.fn((table: string) => {
      if (table === 'trading_calendar') {
        // getCalendarMaxDate: select().order().limit(1).single()
        // getBusinessDaysOrThrow: select().gte().lte().order()
        return {
          select: vi.fn().mockReturnThis(),
          order: vi.fn(function (this: unknown, _col: string, _o?: unknown) {
            // getBusinessDaysOrThrow は order() の戻りを await する
            return Object.assign(
              Promise.resolve(opts.calendarRangeRows ?? { data: [], error: null }),
              {
                limit: vi.fn().mockReturnValue({
                  single: vi.fn().mockResolvedValue(
                    opts.calendarMaxRow ?? { data: null, error: null }
                  ),
                }),
              }
            );
          }),
          gte: vi.fn().mockReturnThis(),
          lte: vi.fn().mockReturnThis(),
        };
      }
      // 実データテーブル: select().not().order().limit(1)
      return {
        select: vi.fn().mockReturnThis(),
        not: vi.fn().mockReturnThis(),
        order: vi.fn().mockReturnThis(),
        limit: vi.fn().mockResolvedValue(opts.dataMaxRow ?? { data: [], error: null }),
      };
    }),
  };
}

describe('cron/forward-fill.ts', () => {
  const CAL_MAX = (d: string) => ({ data: { calendar_date: d }, error: null });

  // 2024-01-15(月)〜19(金) 営業日, 20(土)21(日) 非営業日, 22(月)23(火) 営業日
  const RANGE = (rows: Array<[string, string]>) => ({
    data: rows.map(([calendar_date, hol_div]) => ({ calendar_date, hol_div })),
    error: null,
  });

  describe('resolveForwardFillDates', () => {
    it('overlap by 1: 最新データ日「そのもの」から再取得する', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [{ trade_date: '2024-01-18' }], error: null },
        calendarRangeRows: RANGE([
          ['2024-01-18', '1'],
          ['2024-01-19', '1'],
          ['2024-01-20', '0'],
          ['2024-01-21', '0'],
          ['2024-01-22', '1'],
          ['2024-01-23', '1'],
        ]),
      });

      const result = await resolveForwardFillDates(supabase as never, 'equity_bar_daily', 'trade_date', {
        today: '2024-01-23',
      });

      // 18(最新日)を含めて再取得し、非営業日は除外
      expect(result).toEqual(['2024-01-18', '2024-01-19', '2024-01-22', '2024-01-23']);
    });

    it('既に最新まで取り込み済みなら空配列（overlap分の最新日のみ再取得）', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [{ trade_date: '2024-01-23' }], error: null },
        calendarRangeRows: RANGE([['2024-01-23', '1']]),
      });

      const result = await resolveForwardFillDates(supabase as never, 'equity_bar_daily', 'trade_date', {
        today: '2024-01-23',
      });

      expect(result).toEqual(['2024-01-23']);
    });

    it('maxDate が today より後（再実行直後）なら空配列', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [{ trade_date: '2024-01-24' }], error: null },
      });

      const result = await resolveForwardFillDates(supabase as never, 'equity_bar_daily', 'trade_date', {
        today: '2024-01-23',
      });

      expect(result).toEqual([]);
    });

    it('空テーブルなら floor（today - maxBackfillDays）から開始', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [], error: null },
        calendarRangeRows: RANGE([
          ['2024-01-22', '1'],
          ['2024-01-23', '1'],
        ]),
      });

      const result = await resolveForwardFillDates(supabase as never, 'topix_bar_daily', 'trade_date', {
        today: '2024-01-23',
        maxBackfillDays: 2, // floor = 2024-01-21
      });

      expect(result).toEqual(['2024-01-22', '2024-01-23']);
    });

    it('maxDaysPerRun で件数を上限制限（古い順）', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [{ trade_date: '2024-01-17' }], error: null },
        calendarRangeRows: RANGE([
          ['2024-01-17', '1'],
          ['2024-01-18', '1'],
          ['2024-01-19', '1'],
          ['2024-01-22', '1'],
          ['2024-01-23', '1'],
        ]),
      });

      const result = await resolveForwardFillDates(supabase as never, 'equity_bar_daily', 'trade_date', {
        today: '2024-01-23',
        maxDaysPerRun: 2,
      });

      expect(result).toEqual(['2024-01-17', '2024-01-18']);
    });

    it('上限が窓より大きければ today まで含む全営業日を返す（allowEmptyの停滞防止）', async () => {
      // financial 想定: max が10営業日前でも、上限を窓いっぱいに広げれば today まで到達する。
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [{ disclosed_date: '2024-01-09' }], error: null },
        calendarRangeRows: RANGE([
          ['2024-01-09', '1'],
          ['2024-01-10', '1'],
          ['2024-01-11', '1'],
          ['2024-01-12', '1'],
          ['2024-01-15', '1'],
          ['2024-01-16', '1'],
          ['2024-01-17', '1'],
          ['2024-01-18', '1'],
          ['2024-01-19', '1'],
          ['2024-01-22', '1'],
          ['2024-01-23', '1'],
        ]),
      });

      const result = await resolveForwardFillDates(supabase as never, 'financial_disclosure', 'disclosed_date', {
        today: '2024-01-23',
        maxDaysPerRun: 60,
      });

      // 先頭(max=01-09)から today(01-23)まで途切れず、最後は today を含む
      expect(result[0]).toBe('2024-01-09');
      expect(result[result.length - 1]).toBe('2024-01-23');
      expect(result).toHaveLength(11);
    });

    it('カレンダーが取得できない（null）なら例外を投げる（静かなスキップ防止）', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: { data: null, error: { message: 'DB error' } },
      });

      await expect(
        resolveForwardFillDates(supabase as never, 'equity_bar_daily', 'trade_date', {
          today: '2024-01-23',
        })
      ).rejects.toThrow(/trading_calendar max date/);
    });

    it('カレンダーが today より古い（stale）なら例外を投げる', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-20'),
      });

      await expect(
        resolveForwardFillDates(supabase as never, 'equity_bar_daily', 'trade_date', {
          today: '2024-01-23',
        })
      ).rejects.toThrow(/stale/);
    });
  });

  describe('getMaxDataDate', () => {
    it('最新日付を返す', async () => {
      const supabase = makeSupabase({
        dataMaxRow: { data: [{ trade_date: '2024-01-19' }], error: null },
      });
      const result = await getMaxDataDate(supabase as never, 'equity_bar_daily', 'trade_date');
      expect(result).toBe('2024-01-19');
    });

    it('データが無ければ null', async () => {
      const supabase = makeSupabase({ dataMaxRow: { data: [], error: null } });
      const result = await getMaxDataDate(supabase as never, 'equity_bar_daily', 'trade_date');
      expect(result).toBeNull();
    });

    it('DBエラーなら例外', async () => {
      const supabase = makeSupabase({
        dataMaxRow: { data: null, error: { message: 'boom' } },
      });
      await expect(
        getMaxDataDate(supabase as never, 'equity_bar_daily', 'trade_date')
      ).rejects.toThrow(/Failed to read max/);
    });
  });

  describe('runForwardFill', () => {
    const cfg = (
      allowEmpty: boolean,
      sync: ForwardFillDatasetConfig['sync']
    ): ForwardFillDatasetConfig => ({
      dataset: allowEmpty ? 'financial' : 'equity_bars',
      table: allowEmpty ? 'financial_disclosure' : 'equity_bar_daily',
      dateColumn: allowEmpty ? 'disclosed_date' : 'trade_date',
      sync,
      allowEmpty,
    });

    const dateKey = (allowEmpty: boolean) => (allowEmpty ? 'disclosed_date' : 'trade_date');

    it('allowEmpty=false: 中間営業日が0件なら throw（内部欠損・stale metrics防止）', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [{ [dateKey(false)]: '2024-01-17' }], error: null },
        calendarRangeRows: RANGE([
          ['2024-01-17', '1'],
          ['2024-01-18', '1'],
          ['2024-01-19', '1'],
          ['2024-01-22', '1'],
          ['2024-01-23', '1'],
        ]),
      });
      // 01-18(中間)が0件 → throw
      const sync = vi.fn(async (date: string) =>
        date === '2024-01-18' ? { fetched: 0, inserted: 0 } : { fetched: 5, inserted: 5 }
      );

      await expect(
        runForwardFill(supabase as never, cfg(false, sync), { today: '2024-01-23' })
      ).rejects.toThrow(/returned 0 rows \(data expected/);
    });

    it('allowEmpty=false: 末尾(当日未配信)0件は許容（throwしない）', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [{ [dateKey(false)]: '2024-01-22' }], error: null },
        calendarRangeRows: RANGE([
          ['2024-01-22', '1'],
          ['2024-01-23', '1'],
        ]),
      });
      const sync = vi.fn(async (date: string) =>
        date === '2024-01-23' ? { fetched: 0, inserted: 0 } : { fetched: 5, inserted: 5 }
      );

      const result = await runForwardFill(supabase as never, cfg(false, sync), { today: '2024-01-23' });
      expect(result.targetDates).toEqual(['2024-01-22', '2024-01-23']);
      expect(result.fetched).toBe(5);
    });

    it('allowEmpty=false: today以外（today休場時の前営業日など）の0件は末尾でも throw', async () => {
      // today=01-24（休場 hol_div=0）、対象営業日の末尾は 01-23（≠today）。
      // 末尾でも today ではないので 0 件なら throw する。
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-24'),
        dataMaxRow: { data: [{ [dateKey(false)]: '2024-01-22' }], error: null },
        calendarRangeRows: RANGE([
          ['2024-01-22', '1'],
          ['2024-01-23', '1'],
          ['2024-01-24', '0'], // today は休場
        ]),
      });
      const sync = vi.fn(async (date: string) =>
        date === '2024-01-23' ? { fetched: 0, inserted: 0 } : { fetched: 5, inserted: 5 }
      );

      await expect(
        runForwardFill(supabase as never, cfg(false, sync), { today: '2024-01-24' })
      ).rejects.toThrow(/not today=2024-01-24/);
    });

    it('allowEmpty=true: cap を maxBackfillDays に広げ today まで到達（financial停滞防止）', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [{ [dateKey(true)]: '2024-01-17' }], error: null },
        calendarRangeRows: RANGE([
          ['2024-01-17', '1'],
          ['2024-01-18', '1'],
          ['2024-01-19', '1'],
          ['2024-01-22', '1'],
          ['2024-01-23', '1'],
        ]),
      });
      // 0件日があっても throw せず today まで処理する
      const sync = vi.fn(async (date: string) =>
        date === '2024-01-23' ? { fetched: 2, inserted: 2 } : { fetched: 0, inserted: 0 }
      );

      // maxDaysPerRun=1 を渡しても allowEmpty は maxBackfillDays(=60) を採用して全5日処理
      const result = await runForwardFill(supabase as never, cfg(true, sync), {
        today: '2024-01-23',
        maxBackfillDays: 60,
        maxDaysPerRun: 1,
      });
      expect(result.targetDates).toEqual([
        '2024-01-17',
        '2024-01-18',
        '2024-01-19',
        '2024-01-22',
        '2024-01-23',
      ]);
      expect(sync).toHaveBeenCalledTimes(5);
    });

    it('対象日なし（最新済み）なら sync を呼ばず空結果', async () => {
      const supabase = makeSupabase({
        calendarMaxRow: CAL_MAX('2024-01-23'),
        dataMaxRow: { data: [{ [dateKey(false)]: '2024-01-24' }], error: null }, // max > today
      });
      const sync = vi.fn(async () => ({ fetched: 1, inserted: 1 }));

      const result = await runForwardFill(supabase as never, cfg(false, sync), { today: '2024-01-23' });
      expect(result).toEqual({ targetDates: [], fetched: 0, inserted: 0 });
      expect(sync).not.toHaveBeenCalled();
    });
  });
});
