-- 00084_register_newer_scouter_dispatch.sql
-- ============================================================================
-- 外部トリガー(pg_cron dispatch)未登録の4ワークフローを manifest に追加
-- ----------------------------------------------------------------------------
-- price-alert/earnings-alert/earnings-surprise/holdings-news は 00057/00058
-- (外部トリガー基盤・12ワークフロー登録)より後に追加されたため
-- ops.expected_workflows 未登録=pg_cron dispatch なし=GitHub schedule(ベスト
-- エフォート、最大数時間遅延)のみに依存していた。
-- (実例: 2026-07-09 Price Alert Check が予定19:10 JSTから約2.5時間遅延して発火、
--  さらにランナー未割当のまま timeout-minutes 到達で failure)
--
-- 4ワークフローとも dispatch_label input・run-name・concurrency は既に実装済み
-- (P0-d相当済み)、かつ alreadySucceededToday 冪等ガードも実装済みのため、
-- カナリア無しで直接 enabled=true とする（既存12本と同じ運用: GitHub schedule
-- はフォールバックとして残置。pg_cron失敗時の安全網・冪等ガードで二重実行無害）。
-- job_name は既存マイグレーション(00063/00065/00077/00079)で job_runs の
-- CHECK制約に追加済み。
-- ============================================================================

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('price-alert.yml',       'JapanStockScouter', 'Price Alert Check',       '10 10 * * *',   'daily',   '19:40', 'scouter-price-alert',       true, NULL),
  ('earnings-alert.yml',    'JapanStockScouter', 'Earnings Alert Check',    '0 11 * * *',    'daily',   '20:30', 'scouter-earnings-alert',    true, NULL),
  ('earnings-surprise.yml', 'JapanStockScouter', 'Earnings Surprise Check', '45 11 * * *',   'daily',   '21:15', 'scouter-earnings-surprise', true, NULL),
  ('holdings-news.yml',     'JapanStockScouter', 'Holdings News Check',     '30 11 * * 1-5', 'weekday', '21:00', 'scouter-holdings-news',     true, 'メール有')
ON CONFLICT (workflow_file) DO NOTHING;

SELECT cron.schedule('dispatch-price-alert',       '10 10 * * *',   $$ SELECT ops.dispatch_by_name('price-alert.yml') $$);
SELECT cron.schedule('dispatch-earnings-alert',    '0 11 * * *',    $$ SELECT ops.dispatch_by_name('earnings-alert.yml') $$);
SELECT cron.schedule('dispatch-earnings-surprise', '45 11 * * *',   $$ SELECT ops.dispatch_by_name('earnings-surprise.yml') $$);
SELECT cron.schedule('dispatch-holdings-news',     '30 11 * * 1-5', $$ SELECT ops.dispatch_by_name('holdings-news.yml') $$);
