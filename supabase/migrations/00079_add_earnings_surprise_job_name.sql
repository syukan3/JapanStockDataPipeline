-- 00079_add_earnings_surprise_job_name.sql
-- job_runs の job_name CHECK制約に 'scouter-earnings-surprise' を追加
-- （決算サプライズトラッカーのジョブログ用。00077 と同様 DROP → ADD で追加する）
--
-- 注意: 並行作業(LLM自己改善、00070〜00075)が同じ制約を変更する場合、後から適用する側が
-- 先行分のジョブ名を含む全リストで ADD し直すこと（DROP→ADDのため上書きになる）。

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
    'scouter-earnings-surprise'
  ));
