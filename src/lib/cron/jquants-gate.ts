/**
 * J-Quants 系 Cron ルートの OFF ゲート
 *
 * @description
 * GitHub Actions の if と pg_cron の dispatch 停止だけでは、手動の再実行や CRON_SECRET 付きの
 * curl で J-Quants の処理が走る経路が残る。ルート自身も認証直後にモードを確認する。
 * 設計正本: ../../../../docs/PLANS-jquants-off-switch-2026-10.md §3.4
 *
 * - OFF: 200 { skipped: 'jquants_off' }（job_runs は書かない。OFF 中の未達検知は 00132 が外す）
 * - モードが読めない: 503（推測で J-Quants を呼ばない）
 * - ON: null（呼び出し側はそのまま続行）
 */

import { NextResponse } from 'next/server';
import { getJQuantsMode } from '../data-source/jquants-mode';
import { createLogger } from '../utils/logger';

const logger = createLogger({ module: 'cron/jquants-gate' });

export async function jquantsGate(route: string): Promise<Response | null> {
  try {
    const mode = await getJQuantsMode();
    if (mode.enabled) return null;
    logger.info('J-Quants OFF のためスキップ', { route });
    return NextResponse.json({ skipped: 'jquants_off', route });
  } catch (error) {
    logger.error('J-Quants モードの取得に失敗（J-Quants を呼ばずに中止）', {
      route,
      error: error instanceof Error ? error.message : String(error),
    });
    return NextResponse.json(
      { error: 'J-Quants mode unavailable', detail: error instanceof Error ? error.message : String(error) },
      { status: 503 }
    );
  }
}
