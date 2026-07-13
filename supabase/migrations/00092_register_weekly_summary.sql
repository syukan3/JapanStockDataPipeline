-- 00092_register_weekly_summary.sql
-- 週次ポートフォリオサマリーメール(weekly-summary)の実装に伴うDB変更:
--   1. job_runs の job_name CHECK制約 … 'scouter-weekly-summary' を追加
--   2. 外部トリガー登録               … ops.expected_workflows + pg_cron dispatch
-- 詳細: JapanStockScouter/src/strategies/weekly-summary/
--
-- 注: 本 migration は 00091（yutai-alert 登録）の後に適用される想定。
--     CHECK制約の再ADDリストは 00091 の全許可名（'scouter-yutai-alert' を含む）に
--     新規の 'scouter-weekly-summary' を足したものにする。

-- ============================================================
-- 1. job_runs の job_name CHECK制約に 'scouter-weekly-summary' を追加
--    （既存慣習に倣い DROP → 全許可名を含めて ADD し直す。最新の全リストは
--     00091 を基準にし、末尾に新規名を足す。これを忘れると job_runs への insert が
--     失敗し、冪等ガード(alreadySucceededToday)と監視が機能しなくなる）
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
    'scouter-weekly-summary'
  ));

-- ============================================================
-- 2. 外部トリガー登録（pg_cron → GitHub workflow_dispatch）
--    weekly-summary.yml は dispatch_label input・run-name・concurrency・冪等ガードを
--    実装済みのため、既存fleetと同じく直接 enabled=true とする（GitHub schedule は
--    フォールバックとして残置。00091 の end-state と同方針）。
--    kind='weekly' にすることで ops.check_freshness()（daily/weekday のみ対象）から
--    外す（db-archival.yml / analyst-target-monitor と同じ扱い）。weekly は freshness
--    判定の対象外なので deadline_jst は NULL にする。
--    同名cronが既にあれば他migrationの所有物を上書きしないよう fail closed にする。
-- ============================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM cron.job j
    WHERE j.jobname = 'dispatch-weekly-summary'
  ) THEN
    RAISE EXCEPTION 'cron job dispatch-weekly-summary already exists';
  END IF;
END;
$$;

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('weekly-summary.yml', 'JapanStockScouter', 'Weekly Portfolio Summary', '0 1 * * 6', 'weekly', NULL, 'scouter-weekly-summary', true, 'メール有・毎週土曜10:00 JST（保有損益/マクロ/シグナル/リスクの週次サマリー）。weeklyのためfreshness対象外')
ON CONFLICT (workflow_file) DO NOTHING;

SELECT cron.schedule(
  'dispatch-weekly-summary',
  '0 1 * * 6',
  $$ SELECT ops.dispatch_by_name('weekly-summary.yml') $$
);
