-- ============================================================================
-- pg_cron スケジュール登録: 各ワークフローを定刻 dispatch
-- ----------------------------------------------------------------------------
-- 安全設計: ここで全ジョブを登録するが、ops.expected_workflows.enabled=false の
--   間は ops.dispatch_by_name が no-op（RAISE NOTICE のみ・dispatch しない）。
--   よって GitHub 側の schedule: がまだ生きていても二重起動は起きない。
--
-- カットオーバー（workflow 単位）:
--   1. 当該 workflow の yaml から schedule: を削除して main に merge（workflow_dispatch は残す）
--   2. UPDATE ops.expected_workflows SET enabled=true WHERE workflow_file='<file>';
--   （cron.schedule を触らずデータ駆動で切替。ロールバックは enabled=false へ戻す）
--
-- 時刻は UTC（Supabase pg_cron は UTC 基準。日本は DST 無し）。
-- cron.schedule(job_name, schedule, command) は job_name で upsert。
-- ============================================================================

-- DataPipeline
SELECT cron.schedule('dispatch-cron-c',        '10 3 * * *',     $$ SELECT ops.dispatch_by_name('cron-c.yml') $$);
SELECT cron.schedule('dispatch-cron-e',        '30 7 * * *',     $$ SELECT ops.dispatch_by_name('cron-e.yml') $$);
SELECT cron.schedule('dispatch-cron-a',        '40 9 * * *',     $$ SELECT ops.dispatch_by_name('cron-a.yml') $$);
SELECT cron.schedule('dispatch-cron-b',        '20 10 * * *',    $$ SELECT ops.dispatch_by_name('cron-b.yml') $$);
SELECT cron.schedule('dispatch-cron-d',        '0 22 * * *',     $$ SELECT ops.dispatch_by_name('cron-d.yml') $$);
SELECT cron.schedule('dispatch-db-archival',   '0 18 * * 6',     $$ SELECT ops.dispatch_by_name('db-archival.yml') $$);

-- Scouter
SELECT cron.schedule('dispatch-yutai-cross',   '0 8 * * *',      $$ SELECT ops.dispatch_by_name('yutai-cross-screening.yml') $$);
SELECT cron.schedule('dispatch-macro-regime',  '45 10 * * 1-5',  $$ SELECT ops.dispatch_by_name('macro-regime.yml') $$);
SELECT cron.schedule('dispatch-high-dividend', '50 10 * * 1-5',  $$ SELECT ops.dispatch_by_name('high-dividend-screening.yml') $$);
SELECT cron.schedule('dispatch-macro-ai',      '50 10 * * 1-5',  $$ SELECT ops.dispatch_by_name('macro-ai-evaluation.yml') $$);
SELECT cron.schedule('dispatch-macro-ai-meta', '0 12 1,16 * *',  $$ SELECT ops.dispatch_by_name('macro-ai-meta-evaluation.yml') $$);
SELECT cron.schedule('dispatch-signal-perf',   '0 1 1 * *',      $$ SELECT ops.dispatch_by_name('signal-performance-eval.yml') $$);

-- 注: reconcile_dispatches / check_freshness / check_cron_health / notify と
--     その cron.schedule は Phase 5（監視・自動再ディスパッチ）で別途追加する。
