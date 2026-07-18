/**
 * analytics.market_indicators の同期共有ロジック
 *
 * @description
 * 日次更新（scripts/cron/refresh-market-indicators.ts）と
 * 5年バックフィル（scripts/seed/market-indicators.ts）で共用する。
 *
 * 設計原則:
 * - ワイド型1テーブルに複数ソースが書くため、各ソースグループは
 *   「担当カラムのみ」を同一shapeのpayloadでupsertする（暗黙のNULL上書き防止）。
 * - forward-fill のカーソルは行の最新日ではなく担当カラムのNULL検出
 *   （部分失敗した日を後続実行が自己修復できる）。
 * - 外部ソース（Yahoo/nikkei225jp）の失敗は該当グループのみ欠損させる。
 */

import { createLogger } from '../utils/logger';
import { batchUpsert } from '../utils/batch';
import { computeAdvDecRatio25 } from '../analytics/market-breadth';
import { fetchNikkeiDailyBars, type DailyBar } from './yahoo-chart-client';
import { fetchNikkeiOfficialDaily, NIKKEI_OFFICIAL_CSV_FROM } from './nikkei-official-client';
import { fetchNikkei225jpDaily, fetchNikkei225jpWeekly } from './nikkei225jp-client';

const logger = createLogger({ module: 'market-indicators-sync' });

/** 系列開始日（バックフィル起点） */
export const SERIES_START = '2021-07-01';
/** daily2 の終値突合の許容乖離（列マッピング破壊の検知閾値） */
export const CLOSE_TOLERANCE = 0.005;

export interface IndicatorRow {
  as_of_date: string;
  nikkei_close: number | null;
  nikkei_open: number | null;
  nikkei_high: number | null;
  nikkei_low: number | null;
  nikkei_per: number | null;
  nikkei_vi: number | null;
  short_selling_ratio_restricted: number | null;
  short_selling_ratio_unrestricted: number | null;
  margin_pl_ratio: number | null;
  advancers: number | null;
  decliners: number | null;
  unchanged: number | null;
  adv_dec_ratio_25d: number | null;
  new_highs: number | null;
  new_lows: number | null;
  prime_turnover_value: number | null;
  pct_above_sma25: number | null;
  pct_above_sma200: number | null;
  nikkei_eps: number | null;
  topix_close: number | null;
  nt_ratio: number | null;
}

// Supabaseクライアントはスキーマ束縛の動的型のため any で受ける（repo慣習）
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Client = any;

export function toNum(v: unknown): number | null {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/** PostgREST の numeric→文字列を数値へ正規化 */
export function normalizeRow(r: IndicatorRow): IndicatorRow {
  return {
    as_of_date: r.as_of_date,
    nikkei_close: toNum(r.nikkei_close),
    nikkei_open: toNum(r.nikkei_open),
    nikkei_high: toNum(r.nikkei_high),
    nikkei_low: toNum(r.nikkei_low),
    nikkei_per: toNum(r.nikkei_per),
    nikkei_vi: toNum(r.nikkei_vi),
    short_selling_ratio_restricted: toNum(r.short_selling_ratio_restricted),
    short_selling_ratio_unrestricted: toNum(r.short_selling_ratio_unrestricted),
    margin_pl_ratio: toNum(r.margin_pl_ratio),
    advancers: toNum(r.advancers),
    decliners: toNum(r.decliners),
    unchanged: toNum(r.unchanged),
    adv_dec_ratio_25d: toNum(r.adv_dec_ratio_25d),
    new_highs: toNum(r.new_highs),
    new_lows: toNum(r.new_lows),
    prime_turnover_value: toNum(r.prime_turnover_value),
    pct_above_sma25: toNum(r.pct_above_sma25),
    pct_above_sma200: toNum(r.pct_above_sma200),
    nikkei_eps: toNum(r.nikkei_eps),
    topix_close: toNum(r.topix_close),
    nt_ratio: toNum(r.nt_ratio),
  };
}

export function emptyRow(date: string): IndicatorRow {
  return {
    as_of_date: date,
    nikkei_close: null,
    nikkei_open: null,
    nikkei_high: null,
    nikkei_low: null,
    nikkei_per: null,
    nikkei_vi: null,
    short_selling_ratio_restricted: null,
    short_selling_ratio_unrestricted: null,
    margin_pl_ratio: null,
    advancers: null,
    decliners: null,
    unchanged: null,
    adv_dec_ratio_25d: null,
    new_highs: null,
    new_lows: null,
    prime_turnover_value: null,
    pct_above_sma25: null,
    pct_above_sma200: null,
    nikkei_eps: null,
    topix_close: null,
    nt_ratio: null,
  };
}

export function getOrCreate(rowMap: Map<string, IndicatorRow>, date: string): IndicatorRow {
  let row = rowMap.get(date);
  if (!row) {
    row = emptyRow(date);
    rowMap.set(date, row);
  }
  return row;
}

/**
 * 保存済み行を取得（windowStart 以降＋騰落レシオ25日窓の正準営業日軸用に直前60行）
 */
export async function loadExistingRows(
  analytics: Client,
  windowStart: string
): Promise<Map<string, IndicatorRow>> {
  const map = new Map<string, IndicatorRow>();
  const { data: before, error: beforeErr } = await analytics
    .from('market_indicators')
    .select('*')
    .lt('as_of_date', windowStart)
    .order('as_of_date', { ascending: false })
    .limit(60);
  if (beforeErr) throw new Error(`Failed to load prior rows: ${beforeErr.message}`);
  for (const r of (before as IndicatorRow[] | null) ?? []) map.set(r.as_of_date, normalizeRow(r));

  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await analytics
      .from('market_indicators')
      .select('*')
      .gte('as_of_date', windowStart)
      .order('as_of_date', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) throw new Error(`Failed to load rows: ${error.message}`);
    const rows = (data as IndicatorRow[] | null) ?? [];
    for (const r of rows) map.set(r.as_of_date, normalizeRow(r));
    if (rows.length < PAGE) break;
  }
  return map;
}

export async function upsertRows(
  analytics: Client,
  rows: Array<Record<string, unknown> & { as_of_date: string }>,
  dryRun: boolean
): Promise<number> {
  if (dryRun || rows.length === 0) return rows.length;
  const result = await batchUpsert(analytics, 'market_indicators', rows, 'as_of_date', {
    batchSize: 500,
  });
  return result.inserted;
}

// ============================================================
// ohlc: 日経平均OHLC。一次ソース=日経公式CSV（2026-07にYahooが429恒常化したため切替）、
// フォールバック=Yahoo chart API。close と open/high/low は列単位で独立に埋める。
// ============================================================

/**
 * 日経OHLCのpending判定（純関数・テスト対象）。
 * 公式CSVの収録開始（NIKKEI_OFFICIAL_CSV_FROM=2023-01-04）より前の日付では
 * open/high/low の null は「取得手段のない恒久null」でありpending扱いしない
 * （breadth % の PCT_SMA*_EXPECTED_FROM と同じ理由。含めると埋まらない行を
 * 再スキャンし続ける）。close は全期間でpending対象。
 */
export function isOhlcPending(date: string, row: IndicatorRow | undefined): boolean {
  if (!row || row.nikkei_close == null) return true;
  if (date < NIKKEI_OFFICIAL_CSV_FROM) return false;
  return row.nikkei_open == null || row.nikkei_high == null || row.nikkei_low == null;
}

export async function fillYahoo(
  analytics: Client,
  businessDays: string[],
  rowMap: Map<string, IndicatorRow>,
  dryRun: boolean
): Promise<Record<string, unknown>> {
  const pending = businessDays.filter((d) => isOhlcPending(d, rowMap.get(d)));
  if (pending.length === 0) return { pending: 0, upserted: 0 };
  let source = 'official';
  let bars: DailyBar[];
  try {
    const all = await fetchNikkeiOfficialDaily();
    const first = pending[0];
    const last = pending[pending.length - 1];
    bars = all.filter((b) => b.date >= first && b.date <= last);
  } catch (err) {
    logger.warn('公式CSV取得失敗・Yahooへフォールバック', {
      error: err instanceof Error ? err.message : String(err),
    });
    source = 'yahoo';
    bars = await fetchNikkeiDailyBars(pending[0], pending[pending.length - 1]);
  }
  const plan = planYahooUpdates(pending, rowMap, bars);
  if (plan.missing.length > 0) {
    logger.warn('OHLC: 一部日付が取得できず', {
      source,
      count: plan.missing.length,
      sample: plan.missing.slice(0, 5),
    });
  }
  const stamp = { updated_at: new Date().toISOString() };
  // 列ごとに同一shapeでupsert（既存非NULL列へnullを送らない = 部分upsertの列保護）。
  // rowMap への反映は各グループの upsert 成功後（書き込み失敗時に未永続化の値を
  // 後続グループへ伝播させない）。
  let upserted = 0;
  upserted += await upsertRows(analytics, plan.closeRows.map((r) => ({ ...r, ...stamp })), dryRun);
  for (const u of plan.closeRows) getOrCreate(rowMap, u.as_of_date).nikkei_close = u.nikkei_close;
  upserted += await upsertRows(analytics, plan.openRows.map((r) => ({ ...r, ...stamp })), dryRun);
  for (const u of plan.openRows) getOrCreate(rowMap, u.as_of_date).nikkei_open = u.nikkei_open;
  upserted += await upsertRows(analytics, plan.highRows.map((r) => ({ ...r, ...stamp })), dryRun);
  for (const u of plan.highRows) getOrCreate(rowMap, u.as_of_date).nikkei_high = u.nikkei_high;
  upserted += await upsertRows(analytics, plan.lowRows.map((r) => ({ ...r, ...stamp })), dryRun);
  for (const u of plan.lowRows) getOrCreate(rowMap, u.as_of_date).nikkei_low = u.nikkei_low;
  return { pending: pending.length, upserted, missing: plan.missing.length, source };
}

/**
 * Yahoo ソースから更新payloadを列単位で組み立てる（純関数・テスト対象）
 *
 * - 各列は「保存値がNULLかつソースに値がある」場合のみ更新対象（null上書き防止）。
 *   OHLC には close と独立の null穴があり得るため、close/open/high/low を別グループにする
 *   （終値のみの日を再取得し続けても、既存 close を触らず OHLC だけ埋められる）。
 * - rowMap は読み取りのみ（反映は呼び出し側が upsert 成功後に行う）
 */
export function planYahooUpdates(
  pending: string[],
  rowMap: Map<string, IndicatorRow>,
  bars: DailyBar[]
): {
  closeRows: Array<{ as_of_date: string; nikkei_close: number }>;
  openRows: Array<{ as_of_date: string; nikkei_open: number }>;
  highRows: Array<{ as_of_date: string; nikkei_high: number }>;
  lowRows: Array<{ as_of_date: string; nikkei_low: number }>;
  missing: string[];
} {
  const barMap = new Map(bars.map((b) => [b.date, b]));
  const closeRows: Array<{ as_of_date: string; nikkei_close: number }> = [];
  const openRows: Array<{ as_of_date: string; nikkei_open: number }> = [];
  const highRows: Array<{ as_of_date: string; nikkei_high: number }> = [];
  const lowRows: Array<{ as_of_date: string; nikkei_low: number }> = [];
  const missing: string[] = [];
  for (const date of pending) {
    const bar = barMap.get(date);
    if (!bar) {
      missing.push(date);
      continue;
    }
    const row = rowMap.get(date) ?? emptyRow(date);
    if (row.nikkei_close == null) closeRows.push({ as_of_date: date, nikkei_close: bar.close });
    if (row.nikkei_open == null && bar.open != null) {
      openRows.push({ as_of_date: date, nikkei_open: bar.open });
    }
    if (row.nikkei_high == null && bar.high != null) {
      highRows.push({ as_of_date: date, nikkei_high: bar.high });
    }
    if (row.nikkei_low == null && bar.low != null) {
      lowRows.push({ as_of_date: date, nikkei_low: bar.low });
    }
  }
  return { closeRows, openRows, highRows, lowRows, missing };
}

// ============================================================
// daily2（PER・日経VI。Yahoo欠損日の終値フォールバックも担当）
// ============================================================
export async function fillDaily2(
  analytics: Client,
  businessDays: string[],
  rowMap: Map<string, IndicatorRow>,
  dryRun: boolean,
  prefetched?: Awaited<ReturnType<typeof fetchNikkei225jpDaily>>,
  options?: { skipShortSelling?: boolean }
): Promise<Record<string, unknown>> {
  // SHORT_RATIO_SOURCE=jquants 時は空売り比率2成分を公式データ（fillShortSellingOfficial）へ
  // 委譲するため daily2 側では書かない。判定・書き込みの両方から2成分を除外する。
  const skipShortSelling = options?.skipShortSelling ?? false;
  const pending = businessDays.filter((d) => {
    const r = rowMap.get(d);
    return (
      !r ||
      r.nikkei_per == null ||
      r.nikkei_vi == null ||
      (!skipShortSelling &&
        (r.short_selling_ratio_restricted == null ||
          r.short_selling_ratio_unrestricted == null)) ||
      r.nikkei_close == null
    );
  });
  if (pending.length === 0) return { pending: 0, upserted: 0 };
  const rows = prefetched ?? (await fetchNikkei225jpDaily());
  const plan = planDaily2Updates(pending, rowMap, rows, { skipShortSelling });
  const stamp = { updated_at: new Date().toISOString() };
  // 列ごとに同一shapeでupsert（既存非NULL列へnullを送らない = 部分upsertの列保護）。
  // rowMap への反映は各グループの upsert 成功後に行う。
  let upserted = 0;
  upserted += await upsertRows(analytics, plan.perRows.map((r) => ({ ...r, ...stamp })), dryRun);
  for (const u of plan.perRows) getOrCreate(rowMap, u.as_of_date).nikkei_per = u.nikkei_per;
  upserted += await upsertRows(analytics, plan.viRows.map((r) => ({ ...r, ...stamp })), dryRun);
  for (const u of plan.viRows) {
    getOrCreate(rowMap, u.as_of_date).nikkei_vi = u.nikkei_vi;
  }
  upserted += await upsertRows(
    analytics,
    plan.ssRestrictedRows.map((r) => ({ ...r, ...stamp })),
    dryRun
  );
  for (const u of plan.ssRestrictedRows) {
    getOrCreate(rowMap, u.as_of_date).short_selling_ratio_restricted =
      u.short_selling_ratio_restricted;
  }
  upserted += await upsertRows(
    analytics,
    plan.ssUnrestrictedRows.map((r) => ({ ...r, ...stamp })),
    dryRun
  );
  for (const u of plan.ssUnrestrictedRows) {
    getOrCreate(rowMap, u.as_of_date).short_selling_ratio_unrestricted =
      u.short_selling_ratio_unrestricted;
  }
  upserted += await upsertRows(analytics, plan.closeRows.map((r) => ({ ...r, ...stamp })), dryRun);
  for (const u of plan.closeRows) getOrCreate(rowMap, u.as_of_date).nikkei_close = u.nikkei_close;
  return {
    pending: pending.length,
    upserted,
    closeFallbacks: plan.closeRows.length,
    noSource: plan.noSource,
    closeMismatch: plan.closeMismatch,
  };
}

/**
 * daily2 ソースから更新payloadを列単位で組み立てる（純関数・テスト対象）
 *
 * - 各列は「保存値がNULLかつソースに値がある」場合のみ更新対象（null上書き防止）
 * - 終値突合が乖離した日はそのソース由来の全列をスキップ
 * - rowMap は読み取りのみ（反映は呼び出し側が upsert 成功後に行う）
 */
export function planDaily2Updates(
  pending: string[],
  rowMap: Map<string, IndicatorRow>,
  srcRows: Array<{
    date: string;
    nikkeiClose: number | null;
    per: number | null;
    nikkeiVi: number | null;
    shortSellingRestricted: number | null;
    shortSellingUnrestricted: number | null;
  }>,
  options?: { skipShortSelling?: boolean }
): {
  perRows: Array<{ as_of_date: string; nikkei_per: number }>;
  viRows: Array<{ as_of_date: string; nikkei_vi: number }>;
  ssRestrictedRows: Array<{ as_of_date: string; short_selling_ratio_restricted: number }>;
  ssUnrestrictedRows: Array<{ as_of_date: string; short_selling_ratio_unrestricted: number }>;
  closeRows: Array<{ as_of_date: string; nikkei_close: number }>;
  noSource: number;
  closeMismatch: number;
} {
  const srcMap = new Map(srcRows.map((r) => [r.date, r]));
  const perRows: Array<{ as_of_date: string; nikkei_per: number }> = [];
  const viRows: Array<{ as_of_date: string; nikkei_vi: number }> = [];
  const ssRestrictedRows: Array<{ as_of_date: string; short_selling_ratio_restricted: number }> =
    [];
  const ssUnrestrictedRows: Array<{
    as_of_date: string;
    short_selling_ratio_unrestricted: number;
  }> = [];
  const closeRows: Array<{ as_of_date: string; nikkei_close: number }> = [];
  const skipShortSelling = options?.skipShortSelling ?? false;
  let closeMismatch = 0;
  let noSource = 0;
  for (const date of pending) {
    const src = srcMap.get(date);
    if (!src) {
      noSource++;
      continue;
    }
    const row = rowMap.get(date) ?? emptyRow(date);
    // 終値突合: Yahoo由来の保存値と比較し、乖離時は列マッピング破壊とみなしスキップ
    if (row.nikkei_close != null && src.nikkeiClose != null) {
      const diff = Math.abs(src.nikkeiClose - row.nikkei_close) / row.nikkei_close;
      if (diff > CLOSE_TOLERANCE) {
        closeMismatch++;
        logger.error('daily2: 終値がYahoo保存値と乖離（列マッピング破壊の可能性）', {
          date,
          stored: row.nikkei_close,
          source: src.nikkeiClose,
        });
        continue;
      }
    }
    // Yahoo欠損日の終値フォールバック（daily2の終値も公式値の転載）
    if (row.nikkei_close == null && src.nikkeiClose != null) {
      closeRows.push({ as_of_date: date, nikkei_close: src.nikkeiClose });
    }
    let updatedAny = false;
    if (row.nikkei_per == null && src.per != null) {
      perRows.push({ as_of_date: date, nikkei_per: src.per });
      updatedAny = true;
    }
    if (row.nikkei_vi == null && src.nikkeiVi != null) {
      viRows.push({ as_of_date: date, nikkei_vi: src.nikkeiVi });
      updatedAny = true;
    }
    if (
      !skipShortSelling &&
      row.short_selling_ratio_restricted == null &&
      src.shortSellingRestricted != null
    ) {
      ssRestrictedRows.push({
        as_of_date: date,
        short_selling_ratio_restricted: src.shortSellingRestricted,
      });
      updatedAny = true;
    }
    if (
      !skipShortSelling &&
      row.short_selling_ratio_unrestricted == null &&
      src.shortSellingUnrestricted != null
    ) {
      ssUnrestrictedRows.push({
        as_of_date: date,
        short_selling_ratio_unrestricted: src.shortSellingUnrestricted,
      });
      updatedAny = true;
    }
    if (
      !updatedAny &&
      (row.nikkei_per == null ||
        row.nikkei_vi == null ||
        (!skipShortSelling &&
          (row.short_selling_ratio_restricted == null ||
            row.short_selling_ratio_unrestricted == null)))
    ) {
      noSource++;
    }
  }
  return {
    perRows,
    viRows,
    ssRestrictedRows,
    ssUnrestrictedRows,
    closeRows,
    noSource,
    closeMismatch,
  };
}

// ============================================================
// short-selling official（J-Quants 業種別空売り比率の市場全体集計）
//
// SHORT_RATIO_SOURCE=jquants 時に analytics.short_selling_sector（全33業種の売り注文
// 代金内訳）を日次で合算し、market_indicators の空売り比率2成分を公式値で埋める。
// 集計式は現行 daily2 定義（col22/col24）と一致（互換契約: 列名・単位・非null契約を維持）:
//   restricted%   = ΣShrtWithResVa / Σ(SellExShortVa+ShrtWithResVa+ShrtNoResVa) × 100
//   unrestricted% = ΣShrtNoResVa   / 同分母 × 100
// 2成分は必ず同時に書く（片方だけ非nullの日を作らない）。
// ============================================================

/** short_selling_sector の1行（金額。numericは文字列で来るため toNum で正規化） */
export interface ShortSellingSectorRow {
  as_of_date: string;
  selling_ex_short_value: number | null;
  short_with_restrictions_value: number | null;
  short_without_restrictions_value: number | null;
}

/** numeric(6,2) 相当（小数2桁）へ丸める */
function round2(v: number): number {
  return Math.round(v * 100) / 100;
}

/**
 * 業種別空売り金額から日次の市場全体空売り比率2成分を集計する（純関数・テスト対象）
 *
 * - 日付ごとに全業種の3列を合算し、分母（3列合計）で規制あり/なしの比率[%]を出す。
 * - 分母が0以下（全業種欠損・データ不整合）の日はマップに含めない（比率算出不能）。
 */
export function aggregateShortSellingByDate(
  sectorRows: ShortSellingSectorRow[]
): Map<string, { restricted: number; unrestricted: number }> {
  const sums = new Map<string, { selling: number; restricted: number; unrestricted: number }>();
  for (const r of sectorRows) {
    const s = sums.get(r.as_of_date) ?? { selling: 0, restricted: 0, unrestricted: 0 };
    s.selling += r.selling_ex_short_value ?? 0;
    s.restricted += r.short_with_restrictions_value ?? 0;
    s.unrestricted += r.short_without_restrictions_value ?? 0;
    sums.set(r.as_of_date, s);
  }
  const out = new Map<string, { restricted: number; unrestricted: number }>();
  for (const [date, s] of sums) {
    const denom = s.selling + s.restricted + s.unrestricted;
    if (denom <= 0) continue;
    out.set(date, {
      restricted: round2((s.restricted / denom) * 100),
      unrestricted: round2((s.unrestricted / denom) * 100),
    });
  }
  return out;
}

/**
 * 公式空売り比率の更新payloadを組み立てる（純関数・テスト対象）
 *
 * - 2成分は必ず同時に書く（対象日は両列を同一payloadに含める）。
 * - overwrite=false（日次）: 既存の2成分がどちらか NULL の日のみ対象（fill-null・冪等）。
 * - overwrite=true（seed）: 集計値がある日は既存値に関わらず上書きする（daily2由来値の是正）。
 * - rowMap は読み取りのみ（反映は呼び出し側が upsert 成功後に行う）。
 */
export function planShortSellingOfficial(
  dates: string[],
  rowMap: Map<string, IndicatorRow>,
  aggregated: Map<string, { restricted: number; unrestricted: number }>,
  overwrite: boolean
): Array<{
  as_of_date: string;
  short_selling_ratio_restricted: number;
  short_selling_ratio_unrestricted: number;
}> {
  const out: Array<{
    as_of_date: string;
    short_selling_ratio_restricted: number;
    short_selling_ratio_unrestricted: number;
  }> = [];
  for (const date of dates) {
    const agg = aggregated.get(date);
    if (!agg) continue;
    const row = rowMap.get(date);
    const pending =
      overwrite ||
      row == null ||
      row.short_selling_ratio_restricted == null ||
      row.short_selling_ratio_unrestricted == null;
    if (!pending) continue;
    out.push({
      as_of_date: date,
      short_selling_ratio_restricted: agg.restricted,
      short_selling_ratio_unrestricted: agg.unrestricted,
    });
  }
  return out;
}

/** analytics.short_selling_sector を日付レンジで読み込む（金額はnumeric→数値へ正規化） */
export async function loadShortSellingSector(
  analytics: Client,
  from: string,
  to: string
): Promise<ShortSellingSectorRow[]> {
  const out: ShortSellingSectorRow[] = [];
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await analytics
      .from('short_selling_sector')
      .select(
        'as_of_date, selling_ex_short_value, short_with_restrictions_value, short_without_restrictions_value'
      )
      .gte('as_of_date', from)
      .lte('as_of_date', to)
      .order('as_of_date', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) throw new Error(`Failed to load short_selling_sector: ${error.message}`);
    const rows =
      (data as Array<{
        as_of_date: string;
        selling_ex_short_value: unknown;
        short_with_restrictions_value: unknown;
        short_without_restrictions_value: unknown;
      }> | null) ?? [];
    for (const r of rows) {
      out.push({
        as_of_date: r.as_of_date,
        selling_ex_short_value: toNum(r.selling_ex_short_value),
        short_with_restrictions_value: toNum(r.short_with_restrictions_value),
        short_without_restrictions_value: toNum(r.short_without_restrictions_value),
      });
    }
    if (rows.length < PAGE) break;
  }
  return out;
}

/**
 * 市場全体の空売り比率2成分を J-Quants 業種別データから埋める。
 *
 * @param businessDays 対象営業日（昇順）。この範囲の short_selling_sector を読み集計する。
 * @param options.overwrite seed 用。既存の daily2 由来値も公式値で上書きする（デフォルト false）。
 */
export async function fillShortSellingOfficial(
  analytics: Client,
  businessDays: string[],
  rowMap: Map<string, IndicatorRow>,
  dryRun: boolean,
  options?: { overwrite?: boolean }
): Promise<Record<string, unknown>> {
  if (businessDays.length === 0) return { aggregatedDays: 0, upserted: 0 };
  const from = businessDays[0];
  const to = businessDays[businessDays.length - 1];
  const sectorRows = await loadShortSellingSector(analytics, from, to);
  const aggregated = aggregateShortSellingByDate(sectorRows);
  const plan = planShortSellingOfficial(
    businessDays,
    rowMap,
    aggregated,
    options?.overwrite ?? false
  );
  const stamp = { updated_at: new Date().toISOString() };
  // 2成分は同一payloadなので1グループでupsert（片方だけ書く経路を作らない）。
  const upserted = await upsertRows(
    analytics,
    plan.map((r) => ({ ...r, ...stamp })),
    dryRun
  );
  // rowMap への反映は upsert 成功後
  for (const u of plan) {
    const row = getOrCreate(rowMap, u.as_of_date);
    row.short_selling_ratio_restricted = u.short_selling_ratio_restricted;
    row.short_selling_ratio_unrestricted = u.short_selling_ratio_unrestricted;
  }
  return { aggregatedDays: aggregated.size, upserted };
}

// ============================================================
// weekly（信用評価損益率。ソース側に存在する日付のみ対象）
// ============================================================
export async function fillWeekly(
  analytics: Client,
  from: string,
  to: string,
  rowMap: Map<string, IndicatorRow>,
  dryRun: boolean
): Promise<Record<string, unknown>> {
  const rows = await fetchNikkei225jpWeekly();
  const targets = rows.filter((r) => r.date >= from && r.date <= to);
  const upserts: Array<{ as_of_date: string; margin_pl_ratio: number; updated_at: string }> = [];
  for (const t of targets) {
    const existing = rowMap.get(t.date)?.margin_pl_ratio;
    if (existing != null) continue;
    upserts.push({
      as_of_date: t.date,
      margin_pl_ratio: t.marginPlRatio,
      updated_at: new Date().toISOString(),
    });
  }
  // rowMap への反映は upsert 成功後
  const upserted = await upsertRows(analytics, upserts, dryRun);
  for (const u of upserts) getOrCreate(rowMap, u.as_of_date).margin_pl_ratio = u.margin_pl_ratio;
  return { sourceInWindow: targets.length, upserted };
}

// ============================================================
// derived（EPS・TOPIX終値・NT倍率）
// ============================================================
export async function fillDerived(
  core: Client,
  analytics: Client,
  businessDays: string[],
  rowMap: Map<string, IndicatorRow>,
  dryRun: boolean
): Promise<Record<string, unknown>> {
  const epsRows: Array<{ as_of_date: string; nikkei_eps: number; updated_at: string }> = [];
  for (const date of businessDays) {
    const r = rowMap.get(date);
    if (!r) continue;
    if (r.nikkei_eps == null && r.nikkei_close != null && r.nikkei_per != null && r.nikkei_per > 0) {
      epsRows.push({
        as_of_date: date,
        nikkei_eps: Number((r.nikkei_close / r.nikkei_per).toFixed(2)),
        updated_at: new Date().toISOString(),
      });
    }
  }

  // TOPIX終値 / NT倍率（判定はソース側 topix_bar_daily の存在で行う）
  const ntPending = businessDays.filter((d) => {
    const r = rowMap.get(d);
    return r != null && r.nikkei_close != null && (r.topix_close == null || r.nt_ratio == null);
  });
  const ntRows: Array<{
    as_of_date: string;
    topix_close: number;
    nt_ratio: number;
    updated_at: string;
  }> = [];
  let topixMissing = 0;
  if (ntPending.length > 0) {
    const topixMap = await loadTopixCloses(core, ntPending[0], ntPending[ntPending.length - 1]);
    for (const date of ntPending) {
      const r = rowMap.get(date)!;
      const topix = topixMap.get(date);
      if (topix == null || topix <= 0) {
        topixMissing++;
        continue;
      }
      ntRows.push({
        as_of_date: date,
        topix_close: topix,
        nt_ratio: Number((r.nikkei_close! / topix).toFixed(3)),
        updated_at: new Date().toISOString(),
      });
    }
  }
  // EPS行とNT行はpayload shapeが異なるため別々にupsertし、rowMap反映は成功後に行う
  let upserted = 0;
  upserted += await upsertRows(analytics, epsRows, dryRun);
  for (const u of epsRows) getOrCreate(rowMap, u.as_of_date).nikkei_eps = u.nikkei_eps;
  upserted += await upsertRows(analytics, ntRows, dryRun);
  for (const u of ntRows) {
    const r = getOrCreate(rowMap, u.as_of_date);
    r.topix_close = u.topix_close;
    r.nt_ratio = u.nt_ratio;
  }
  return { epsFilled: epsRows.length, ntFilled: ntRows.length, topixMissing, upserted };
}

export async function loadTopixCloses(
  core: Client,
  from: string,
  to: string
): Promise<Map<string, number>> {
  const map = new Map<string, number>();
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await core
      .from('topix_bar_daily')
      .select('trade_date, close')
      .gte('trade_date', from)
      .lte('trade_date', to)
      .order('trade_date', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) throw new Error(`Failed to load topix: ${error.message}`);
    const rows = (data as { trade_date: string; close: unknown }[] | null) ?? [];
    for (const r of rows) {
      const close = toNum(r.close);
      if (close != null) map.set(r.trade_date, close);
    }
    if (rows.length < PAGE) break;
  }
  return map;
}

// ============================================================
// 騰落レシオ(25日)の導出
// ============================================================

/**
 * 騰落レシオの更新payloadを組み立てる（純関数・テスト対象）
 *
 * @param canonicalDays 正準の営業日配列（昇順・連続。trading_calendar 等に基づく）。
 *   窓は必ずこの配列上の連続25営業日で組み、行が欠けた営業日を含む窓は保存しない。
 * @param recomputeAfter 騰落数を今回埋め直した日付集合。これらの日を窓に含む
 *   後続日（最大24営業日先）は既存 ratio があっても再計算する（欠損修復の伝播）。
 */
export function planRatioUpserts(
  canonicalDays: string[],
  rowMap: Map<string, IndicatorRow>,
  recomputeAfter?: ReadonlySet<string>
): Array<{ as_of_date: string; adv_dec_ratio_25d: number }> {
  const out: Array<{ as_of_date: string; adv_dec_ratio_25d: number }> = [];
  for (let i = 24; i < canonicalDays.length; i++) {
    const date = canonicalDays[i];
    const row = rowMap.get(date);
    if (!row || row.advancers == null) continue;
    const windowDays = canonicalDays.slice(i - 24, i + 1);
    const needsRecompute =
      recomputeAfter != null && windowDays.some((d) => recomputeAfter.has(d));
    if (row.adv_dec_ratio_25d != null && !needsRecompute) continue;
    const windowRows = windowDays.map((d) => rowMap.get(d));
    const ratio = computeAdvDecRatio25(
      windowRows.map((r) => r?.advancers),
      windowRows.map((r) => r?.decliners)
    );
    if (ratio == null) continue; // 窓内に欠損営業日あり → 保存しない（後日修復で再計算）
    if (row.adv_dec_ratio_25d === ratio) continue;
    out.push({ as_of_date: date, adv_dec_ratio_25d: ratio });
  }
  return out;
}

/** 騰落レシオを正準営業日軸で埋める（欠損修復日の後続再計算を含む） */
export async function fillRatio(
  analytics: Client,
  canonicalDays: string[],
  rowMap: Map<string, IndicatorRow>,
  dryRun: boolean,
  recomputeAfter?: ReadonlySet<string>
): Promise<number> {
  const stamp = { updated_at: new Date().toISOString() };
  const plan = planRatioUpserts(canonicalDays, rowMap, recomputeAfter);
  const upserted = await upsertRows(analytics, plan.map((r) => ({ ...r, ...stamp })), dryRun);
  // rowMap への反映は upsert 成功後
  for (const u of plan) getOrCreate(rowMap, u.as_of_date).adv_dec_ratio_25d = u.adv_dec_ratio_25d;
  return upserted;
}
