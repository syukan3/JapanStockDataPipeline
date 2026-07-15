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
import { fetchNikkeiDailyCloses } from './yahoo-chart-client';
import { fetchNikkei225jpDaily, fetchNikkei225jpWeekly } from './nikkei225jp-client';

const logger = createLogger({ module: 'market-indicators-sync' });

/** 系列開始日（バックフィル起点） */
export const SERIES_START = '2021-07-01';
/** daily2 の終値突合の許容乖離（列マッピング破壊の検知閾値） */
export const CLOSE_TOLERANCE = 0.005;

export interface IndicatorRow {
  as_of_date: string;
  nikkei_close: number | null;
  nikkei_per: number | null;
  nikkei_vi: number | null;
  margin_pl_ratio: number | null;
  advancers: number | null;
  decliners: number | null;
  unchanged: number | null;
  adv_dec_ratio_25d: number | null;
  new_highs: number | null;
  new_lows: number | null;
  prime_turnover_value: number | null;
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
    nikkei_per: toNum(r.nikkei_per),
    nikkei_vi: toNum(r.nikkei_vi),
    margin_pl_ratio: toNum(r.margin_pl_ratio),
    advancers: toNum(r.advancers),
    decliners: toNum(r.decliners),
    unchanged: toNum(r.unchanged),
    adv_dec_ratio_25d: toNum(r.adv_dec_ratio_25d),
    new_highs: toNum(r.new_highs),
    new_lows: toNum(r.new_lows),
    prime_turnover_value: toNum(r.prime_turnover_value),
    nikkei_eps: toNum(r.nikkei_eps),
    topix_close: toNum(r.topix_close),
    nt_ratio: toNum(r.nt_ratio),
  };
}

export function emptyRow(date: string): IndicatorRow {
  return {
    as_of_date: date,
    nikkei_close: null,
    nikkei_per: null,
    nikkei_vi: null,
    margin_pl_ratio: null,
    advancers: null,
    decliners: null,
    unchanged: null,
    adv_dec_ratio_25d: null,
    new_highs: null,
    new_lows: null,
    prime_turnover_value: null,
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
// yahoo（日経平均終値）
// ============================================================
export async function fillYahoo(
  analytics: Client,
  businessDays: string[],
  rowMap: Map<string, IndicatorRow>,
  dryRun: boolean
): Promise<Record<string, unknown>> {
  const pending = businessDays.filter((d) => rowMap.get(d)?.nikkei_close == null);
  if (pending.length === 0) return { pending: 0, upserted: 0 };
  const closes = await fetchNikkeiDailyCloses(pending[0], pending[pending.length - 1]);
  const closeMap = new Map(closes.map((c) => [c.date, c.close]));
  const upserts: Array<{ as_of_date: string; nikkei_close: number; updated_at: string }> = [];
  for (const date of pending) {
    const close = closeMap.get(date);
    if (close == null) continue;
    upserts.push({ as_of_date: date, nikkei_close: close, updated_at: new Date().toISOString() });
  }
  const missing = pending.filter((d) => !closeMap.has(d));
  if (missing.length > 0) {
    logger.warn('Yahoo: 一部日付が取得できず', { count: missing.length, sample: missing.slice(0, 5) });
  }
  // rowMap への反映は upsert 成功後（書き込み失敗時に未永続化の値を後続グループへ伝播させない）
  const upserted = await upsertRows(analytics, upserts, dryRun);
  for (const u of upserts) getOrCreate(rowMap, u.as_of_date).nikkei_close = u.nikkei_close;
  return { pending: pending.length, upserted, missing: missing.length };
}

// ============================================================
// daily2（PER・日経VI。Yahoo欠損日の終値フォールバックも担当）
// ============================================================
export async function fillDaily2(
  analytics: Client,
  businessDays: string[],
  rowMap: Map<string, IndicatorRow>,
  dryRun: boolean,
  prefetched?: Awaited<ReturnType<typeof fetchNikkei225jpDaily>>
): Promise<Record<string, unknown>> {
  const pending = businessDays.filter((d) => {
    const r = rowMap.get(d);
    return !r || r.nikkei_per == null || r.nikkei_vi == null || r.nikkei_close == null;
  });
  if (pending.length === 0) return { pending: 0, upserted: 0 };
  const rows = prefetched ?? (await fetchNikkei225jpDaily());
  const plan = planDaily2Updates(pending, rowMap, rows);
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
  }>
): {
  perRows: Array<{ as_of_date: string; nikkei_per: number }>;
  viRows: Array<{ as_of_date: string; nikkei_vi: number }>;
  closeRows: Array<{ as_of_date: string; nikkei_close: number }>;
  noSource: number;
  closeMismatch: number;
} {
  const srcMap = new Map(srcRows.map((r) => [r.date, r]));
  const perRows: Array<{ as_of_date: string; nikkei_per: number }> = [];
  const viRows: Array<{ as_of_date: string; nikkei_vi: number }> = [];
  const closeRows: Array<{ as_of_date: string; nikkei_close: number }> = [];
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
    if (!updatedAny && (row.nikkei_per == null || row.nikkei_vi == null)) {
      noSource++;
    }
  }
  return { perRows, viRows, closeRows, noSource, closeMismatch };
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
