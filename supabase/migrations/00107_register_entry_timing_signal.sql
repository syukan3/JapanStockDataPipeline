-- 00107_register_entry_timing_signal.sql
-- エントリータイミング判定バッチ(JapanStockScouter entry-timing-signal.yml)の登録:
--   1. job_runs の job_name CHECK制約 … 'scouter-entry-timing-signal' を追加
--      （未登録のままだと index.ts の job_runs insert がCHECK違反で失敗し、
--       ジョブログ記録がサイレントに欠落し続ける実害があるため必須）
--   2. 外部トリガー登録 … ops.expected_workflows + pg_cron dispatch
--      （未登録のままだとGitHub schedule単独運用となり、他バッチ同様+1.5〜5h遅延し得る）
--
-- 計画書: docs/PLANS-entry-timing-2026-07.md（ルートリポ）

-- ============================================================
-- 1. job_runs の job_name CHECK制約に 'scouter-entry-timing-signal' を追加
--    （既存慣習に倣い DROP → 全許可名を含めて ADD し直す。最新の全リストは
--     00103 を基準にし、末尾に新規名を足す）
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
    'weekly-margin',
    'scouter-entry-timing-signal'
  ));

-- ============================================================
-- 2. 外部トリガー登録（pg_cron → GitHub workflow_dispatch）
--    JST 20:10（月〜金）= UTC 11:10。entry-timing-signal.yml の schedule と一致させる。
--    同名cronが既にあれば他migrationの所有物を上書きしないよう fail closed にする。
-- ============================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM cron.job j
    WHERE j.jobname = 'dispatch-entry-timing-signal'
  ) THEN
    RAISE EXCEPTION 'cron job dispatch-entry-timing-signal already exists';
  END IF;
END;
$$;

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('entry-timing-signal.yml', 'JapanStockScouter', 'Entry Timing Signal', '10 11 * * 1-5', 'weekday', '20:40', 'scouter-entry-timing-signal', true, NULL)
ON CONFLICT (workflow_file) DO NOTHING;

SELECT cron.schedule(
  'dispatch-entry-timing-signal',
  '10 11 * * 1-5',
  $$ SELECT ops.dispatch_by_name('entry-timing-signal.yml') $$
);
