/**
 * J-Quants の ON/OFF モードを GitHub Actions のステップ出力へ書く（00132 のスイッチ）
 *
 * @description
 * 各ワークフローの最初のステップで実行し、後続ステップを
 *   if: steps.mode.outputs.enabled == 'true'   （J-Quants 経路）
 *   if: steps.mode.outputs.enabled == 'false'  （代替経路）
 * で切り替える。設計正本: ../../../docs/PLANS-jquants-off-switch-2026-10.md §3.3
 *
 * 出力:
 *   enabled=true|false
 *   last_official_trade_date=YYYY-MM-DD|（空）
 *   short_ratio_source=jquants|daily2   （refresh-market-indicators の SHORT_RATIO_SOURCE 用）
 *
 * モードが読めないときは exit 1。ON 側・OFF 側どちらのステップも走らず、失敗通知が飛ぶ
 * （どちらかを推測で選ばない）。
 *
 * 実行: npx tsx scripts/cron/jquants-mode.ts
 */

import { appendFileSync } from 'fs';
import { getJQuantsMode } from '../../src/lib/data-source/jquants-mode';

async function main(): Promise<void> {
  const mode = await getJQuantsMode({ fresh: true });
  const lines = [
    `enabled=${mode.enabled ? 'true' : 'false'}`,
    `last_official_trade_date=${mode.lastOfficialTradeDate ?? ''}`,
    `short_ratio_source=${mode.enabled ? 'jquants' : 'daily2'}`,
  ];

  console.log(JSON.stringify({ jquants: mode.enabled ? 'ON' : 'OFF', ...mode }));

  const outputPath = process.env.GITHUB_OUTPUT;
  if (outputPath) {
    appendFileSync(outputPath, lines.join('\n') + '\n');
  } else {
    // ローカル実行時は標準出力で確認できるようにする
    console.log(lines.join('\n'));
  }
}

main().catch((error) => {
  console.error('J-Quants モードの取得に失敗しました:', error instanceof Error ? error.message : error);
  process.exit(1);
});
