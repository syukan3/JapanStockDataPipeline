-- job_runs の job_name CHECK制約に 'scouter-macro-ai-meta' を追加
-- （マクロAIメタ評価バッチのジョブログ用。これまで CHECK 制約に未登録で
--  insert が失敗し warn 握り潰しになっており、job_runs に記録が残らず監視不能だった）
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
    'scouter-growth-signal', 'scouter-macro-ai-meta'
  ));
