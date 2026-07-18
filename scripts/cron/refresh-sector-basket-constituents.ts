#!/usr/bin/env tsx
/**
 * 業種自動導出バスケットの構成銘柄 日次差分更新スクリプト（GH Actions用）
 *
 * @description
 * constituent_source='sector33_auto' の analytics.basket_definitions each について、
 * jquants_core.equity_master（is_current=true）の sector33_name=sector33_filter の現行銘柄一覧を
 * analytics.basket_constituents（valid_to is null）と差分比較し、追加/クローズする（冪等）。
 *
 *   - 新規（新規上場・セクター編入）→ weight_factor=1, official_weight=null,
 *     is_semicon_main=true, valid_from=当日 で追加
 *   - 消滅（上場廃止・セクター変更）→ 既存行の valid_to=当日 でクローズ
 *   - 変化なし → 何もしない
 *
 * weight_factor=1 は TOPIX-33業種がキャップ無し（時価総額シェアそのものがウエート）のため。
 * 実際の日次ウエートは refresh-basket-metrics 側で w_i(t)=mcap_i/Σmcap として算出される。
 *
 * 実行順序: Cron A 後段の refresh-basket-metrics の *前* に置く（構成銘柄を確定してから当日集計）。
 * 200A（constituent_source='curated'）はこのバッチの対象外（年次手動更新のまま）。
 *
 * 防御的挙動: マイグレーション 00106 未適用（constituent_source 列が無い）の環境では
 * 「マイグレーション未適用のためスキップ」を出して正常終了する（本番でカラム存在確認してから動く）。
 *
 * 計画書: docs/PLANS-entry-timing-2026-07.md §5.1（ルートリポ）、DDL: 00106
 *
 * 実行:
 *   npx tsx scripts/cron/refresh-sector-basket-constituents.ts [--dry-run] [--date=YYYY-MM-DD]
 */

import { createAdminClient } from '../../src/lib/supabase/admin';
import { createLogger } from '../../src/lib/utils/logger';
import { getJSTDate } from '../../src/lib/utils/date';
import {
  diffSectorConstituents,
  type ConstituentDiff,
} from '../../src/lib/analytics/basket-valuation';
import { fetchSector33Constituents } from '../../src/lib/analytics/basket-valuation-data';

// Supabaseクライアントはスキーマ束縛の動的型のため any で受ける（repo慣習）
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Client = any;

const logger = createLogger({ module: 'refresh-sector-basket-constituents' });

/** Postgres undefined_column（列が存在しない）。00106 未適用の検知に使う */
const UNDEFINED_COLUMN = '42703';

interface SectorAutoBasket {
  basket_id: string;
  sector33_filter: string | null;
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function validateEnv(): void {
  const required = ['NEXT_PUBLIC_SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

/** PostgREST エラーが「constituent_source 列が無い（00106未適用）」を示すか */
function isConstituentSourceMissing(error: { code?: string; message?: string } | null): boolean {
  if (!error) return false;
  if (error.code === UNDEFINED_COLUMN) return true;
  const msg = error.message ?? '';
  return msg.includes('constituent_source') && msg.includes('does not exist');
}

async function main(): Promise<void> {
  validateEnv();
  const dryRun = process.argv.includes('--dry-run');
  const dateArg = process.argv.find((a) => a.startsWith('--date='));
  const asOfDate = dateArg ? dateArg.split('=')[1] : getJSTDate();
  if (dryRun) logger.info('DRY RUN: 読み取り＋計算のみ。書き込みは行わない');

  const core = createAdminClient('jquants_core');
  const analytics = createAdminClient('analytics');

  const baskets = await listSectorAutoBaskets(analytics);
  if (baskets === 'migration_not_applied') {
    logger.info('マイグレーション 00106 未適用（constituent_source 列が無い）のためスキップ');
    console.log(JSON.stringify({ success: true, skipped: 'migration_not_applied', baskets: 0 }));
    return;
  }
  if (baskets.length === 0) {
    logger.info('No sector33_auto baskets found; nothing to do');
    console.log(JSON.stringify({ success: true, baskets: 0, results: [] }));
    return;
  }
  logger.info('Loaded sector33_auto baskets', { count: baskets.length });

  const results: Record<string, unknown>[] = [];
  const failures: string[] = [];

  for (const basket of baskets) {
    try {
      const result = await refreshBasketConstituents(core, analytics, basket, asOfDate, dryRun);
      results.push({ basketId: basket.basket_id, ...result });
    } catch (e) {
      logger.error('Failed to refresh constituents', {
        basketId: basket.basket_id,
        error: message(e),
      });
      failures.push(`${basket.basket_id}: ${message(e)}`);
      results.push({ basketId: basket.basket_id, error: message(e) });
    }
  }

  console.log(
    JSON.stringify({ success: failures.length === 0, baskets: baskets.length, results }, null, 2)
  );
  if (failures.length > 0) {
    throw new Error(`Some baskets failed: ${failures.join(' / ')}`);
  }
}

/** 1バスケット分の構成銘柄差分を計算し反映する */
async function refreshBasketConstituents(
  core: Client,
  analytics: Client,
  basket: SectorAutoBasket,
  asOfDate: string,
  dryRun: boolean
): Promise<Record<string, unknown>> {
  if (!basket.sector33_filter) {
    logger.warn('sector33_filter が未設定のためスキップ', { basketId: basket.basket_id });
    return { skipped: true, reason: 'no_sector33_filter' };
  }

  const current = await fetchSector33Constituents(core, basket.sector33_filter);
  if (current.length === 0) {
    // equity_master 側が空（マスタ未同期等）の場合、全構成銘柄を誤クローズしないよう安全側でスキップ
    logger.warn('equity_master 側の該当銘柄が0件のためスキップ（全クローズ防止）', {
      basketId: basket.basket_id,
      sector33Filter: basket.sector33_filter,
    });
    return { skipped: true, reason: 'no_current_constituents' };
  }
  const existing = await listCurrentConstituentCodes(analytics, basket.basket_id);
  const diff = diffSectorConstituents(current, existing);

  if (diff.toAdd.length === 0 && diff.toClose.length === 0) {
    logger.info('No constituent changes', {
      basketId: basket.basket_id,
      current: current.length,
    });
    return {
      currentCount: current.length,
      existingCount: existing.length,
      added: 0,
      closed: 0,
      unchanged: true,
    };
  }

  logger.info('Constituent diff detected', {
    basketId: basket.basket_id,
    add: diff.toAdd,
    close: diff.toClose,
  });

  if (dryRun) {
    return {
      dryRun: true,
      asOfDate,
      currentCount: current.length,
      existingCount: existing.length,
      wouldAdd: diff.toAdd,
      wouldClose: diff.toClose,
    };
  }

  await applyDiff(analytics, basket.basket_id, diff, asOfDate);

  return {
    asOfDate,
    currentCount: current.length,
    existingCount: existing.length,
    added: diff.toAdd.length,
    closed: diff.toClose.length,
  };
}

/**
 * constituent_source='sector33_auto' のバスケット定義一覧を取得する。
 * constituent_source 列が無ければ（00106未適用）'migration_not_applied' を返す。
 */
async function listSectorAutoBaskets(
  analytics: Client
): Promise<SectorAutoBasket[] | 'migration_not_applied'> {
  const { data, error } = await analytics
    .from('basket_definitions')
    .select('basket_id, sector33_filter')
    .eq('constituent_source', 'sector33_auto')
    .order('basket_id', { ascending: true });
  if (error) {
    if (isConstituentSourceMissing(error)) return 'migration_not_applied';
    throw new Error(`Failed to load basket_definitions: ${error.message}`);
  }
  const rows =
    (data as Array<{ basket_id: string; sector33_filter: string | null }> | null) ?? [];
  return rows.map((r) => ({ basket_id: r.basket_id, sector33_filter: r.sector33_filter }));
}

/** 現行構成銘柄コード（valid_to is null）をページングで取得する */
async function listCurrentConstituentCodes(analytics: Client, basketId: string): Promise<string[]> {
  const codes: string[] = [];
  const PAGE = 1000;
  for (let offset = 0; ; offset += PAGE) {
    const { data, error } = await analytics
      .from('basket_constituents')
      .select('local_code')
      .eq('basket_id', basketId)
      .is('valid_to', null)
      .order('local_code', { ascending: true })
      .range(offset, offset + PAGE - 1);
    if (error) {
      throw new Error(`Failed to load basket_constituents for ${basketId}: ${error.message}`);
    }
    const page = (data as { local_code: string }[] | null) ?? [];
    for (const r of page) codes.push(r.local_code);
    if (page.length < PAGE) break;
  }
  return codes;
}

/** 差分を basket_constituents へ反映（追加 upsert / クローズ update） */
async function applyDiff(
  analytics: Client,
  basketId: string,
  diff: ConstituentDiff,
  asOfDate: string
): Promise<void> {
  if (diff.toAdd.length > 0) {
    const rows = diff.toAdd.map((code) => ({
      basket_id: basketId,
      local_code: code,
      weight_factor: 1,
      official_weight: null,
      is_semicon_main: true,
      valid_from: asOfDate,
      valid_to: null,
    }));
    const { error } = await analytics
      .from('basket_constituents')
      .upsert(rows, { onConflict: 'basket_id,local_code,valid_from' });
    if (error) throw new Error(`Failed to add constituents for ${basketId}: ${error.message}`);
  }

  if (diff.toClose.length > 0) {
    const { error } = await analytics
      .from('basket_constituents')
      .update({ valid_to: asOfDate })
      .eq('basket_id', basketId)
      .is('valid_to', null)
      .in('local_code', diff.toClose);
    if (error) throw new Error(`Failed to close constituents for ${basketId}: ${error.message}`);
  }
}

// 直接実行時のみ main() を呼ぶ（テストで import した際は実行しない）
const isDirectRun =
  process.argv[1]?.endsWith('refresh-sector-basket-constituents.ts') ||
  process.argv[1]?.endsWith('refresh-sector-basket-constituents');
if (isDirectRun) {
  main()
    .then(() => process.exit(0))
    .catch((error) => {
      logger.error('Script failed', { error: message(error) });
      process.exit(1);
    });
}

export {
  main as refreshSectorBasketConstituents,
  refreshBasketConstituents,
  listSectorAutoBaskets,
  isConstituentSourceMissing,
};
