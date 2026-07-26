-- 00111_register_thesis_lens.sql
-- 論者レンズ採点バッチ(thesis-lens)の実装に伴うDB変更:
--   1. job_runs の job_name CHECK制約 … 'scouter-thesis-lens' を追加
--   2. 外部トリガー登録               … ops.expected_workflows + pg_cron dispatch
-- 詳細: JapanStockScouter/src/strategies/thesis-lens/ ・ docs/PLANS-thesis-lens-2026-07.md
--
-- 注: 本 migration は 00107（entry-timing-signal 登録）の後に適用される想定。
--     CHECK制約の再ADDリストは 00107 の全許可名に新規の 'scouter-thesis-lens' を足したもの。

-- ============================================================
-- 1. job_runs の job_name CHECK制約に 'scouter-thesis-lens' を追加
--    （既存慣習に倣い DROP → 全許可名を含めて ADD し直す。最新の全リストは
--     00107 を基準にし、末尾に新規名を足す。これを忘れると job_runs への insert が
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
    'scouter-weekly-summary',
    'weekly-margin',
    'scouter-entry-timing-signal',
    'scouter-thesis-lens'
  ));

-- ============================================================
-- 2. 外部トリガー登録（pg_cron → GitHub workflow_dispatch）
--    JST 土曜 11:00 = UTC 土曜 02:00。thesis-lens.yml の schedule と一致させる。
--    kind='weekly' にすることで ops.check_freshness()（daily/weekday のみ対象）から
--    外す（weekly-summary / db-archival と同じ扱い）。weekly は freshness 判定の
--    対象外なので deadline_jst は NULL にする。
--    同名cronが既にあれば他migrationの所有物を上書きしないよう fail closed にする。
-- ============================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM cron.job j
    WHERE j.jobname = 'dispatch-thesis-lens'
  ) THEN
    RAISE EXCEPTION 'cron job dispatch-thesis-lens already exists';
  END IF;
END;
$$;

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  ('thesis-lens.yml', 'JapanStockScouter', 'Thesis Lens Scoring', '0 2 * * 6', 'weekly', NULL, 'scouter-thesis-lens', true, 'メールなし・毎週土曜11:00 JST（論者レンズの命題を決定論で採点しPIT保存）。結果はPortfolio /lenses で参照。weeklyのためfreshness対象外')
ON CONFLICT (workflow_file) DO NOTHING;

SELECT cron.schedule(
  'dispatch-thesis-lens',
  '0 2 * * 6',
  $$ SELECT ops.dispatch_by_name('thesis-lens.yml') $$
);
