-- 00103_add_weekly_margin_job.sql
-- 信用取引週末残高の週次Cron F(cron-f.yml)導入に伴うDB変更:
--   1. job_runs の job_name CHECK制約 … 'weekly-margin' を追加
--   2. 外部トリガー登録               … ops.expected_workflows + pg_cron dispatch
--
-- 注: J-Quants Standard は未契約のため enabled=false で登録する
--     （cron-f.yml 側も schedule コメントアウト。契約後Runbookで両方を有効化）。
--     enabled=false の間は ops.dispatch_by_name が no-op となり定期失敗は発生しない。

-- ============================================================
-- 1. job_runs の job_name CHECK制約に 'weekly-margin' を追加
--    （既存慣習に倣い DROP → 全許可名を含めて ADD し直す。最新の全リストは
--     00092 を基準にし、末尾に新規名を足す）
-- ============================================================

ALTER TABLE jquants_ingest.job_runs
  DROP CONSTRAINT IF EXISTS job_runs_job_name_check;

ALTER TABLE jquants_ingest.job_runs
  ADD CONSTRAINT job_runs_job_name_check
  CHECK (job_name IN (
    'cron_a', 'cron_b', 'cron_c',
    'scouter-high-dividend', 'cron-d-macro',
    'scouter-macro-regime', 'scouter-macro-ai',
    'cron-e-yutai', 'scouter-yutai-cross',
    'db-archival', 'scouter-signal-performance',
    'scouter-growth-signal', 'scouter-macro-ai-meta',
    'scouter-holdings-news', 'scouter-earnings-alert',
    'scouter-factor-paper',
    'scouter-price-alert',
    'scouter-earnings-surprise',
    'scouter-analyst-target-monitor',
    'scouter-yutai-alert',
    'scouter-weekly-summary',
    'weekly-margin'
  ));

-- ============================================================
-- 2. 外部トリガー登録（pg_cron → GitHub workflow_dispatch）
--    kind='weekly' にすることで ops.check_freshness()（daily/weekday のみ対象）から
--    外し、deadline_jst も NULL にする（00089/00092 と同じ扱い）。
--    同名cronが既にあれば他migrationの所有物を上書きしないよう fail closed にする。
-- ============================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM cron.job j
    WHERE j.jobname = 'dispatch-cron-f'
  ) THEN
    RAISE EXCEPTION 'cron job dispatch-cron-f already exists';
  END IF;
END;
$$;

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('cron-f.yml', 'JapanStockDataPipeline', 'Cron F - Weekly Margin Interest', '30 20 * * 2', 'weekly', NULL, 'weekly-margin', false, 'J-Quants Standard契約後に有効化（契約前は dispatch no-op）。毎週水曜05:30 JSTに前週末残高を取得。weeklyのためfreshness対象外')
ON CONFLICT (workflow_file) DO NOTHING;

SELECT cron.schedule(
  'dispatch-cron-f',
  '30 20 * * 2',
  $$ SELECT ops.dispatch_by_name('cron-f.yml') $$
);
