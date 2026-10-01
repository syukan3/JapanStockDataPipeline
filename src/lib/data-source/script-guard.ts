/**
 * 直接実行スクリプト用の J-Quants OFF ガード
 *
 * @description
 * GH Actions の if で止めていても、手元やワークフローの手動実行で J-Quants 系スクリプトが
 * 走る経路が残る。起動時にモードを確認し、OFF なら「停止中」を出して終了する。
 * 設計正本: ../../../../docs/PLANS-jquants-off-switch-2026-10.md §3.4
 *
 * モードが読めないときは例外（スクリプトは失敗終了する。推測で J-Quants を呼ばない）。
 */

import { getJQuantsMode } from './jquants-mode';

export async function exitIfJQuantsOff(
  script: string,
  options?: { exitCode?: number }
): Promise<void> {
  const mode = await getJQuantsMode({ fresh: true });
  if (mode.enabled) return;
  console.log(
    JSON.stringify({
      skipped: 'jquants_off',
      script,
      lastOfficialTradeDate: mode.lastOfficialTradeDate,
      hint: '再開は scripts/ops/jquants-toggle.sh on',
    })
  );
  process.exit(options?.exitCode ?? 0);
}
