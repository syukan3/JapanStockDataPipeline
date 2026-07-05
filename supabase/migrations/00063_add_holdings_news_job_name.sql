-- 00063_add_holdings_news_job_name.sql
-- job_runs の job_name CHECK制約に 'scouter-holdings-news' を追加
-- （保有銘柄ニュース日次チェックバッチのジョブログ用。既存慣習(00014/00019/00032/00037/
--  00039/00045/00055/00056)に倣い DROP → ADD で追加する。これを忘れると job_runs への
--  insert が失敗し、冪等ガード(alreadySucceededToday)と監視が機能しなくなる）

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
    'scouter-holdings-news'
  ));
