-- 00086_create_external_search_budget.sql の手動ロールバック。
-- 先に analyst-target-monitor.yml を無効化し、00087 のdownを実行すること。
-- 実行中workerが完了できなくなるため、running行がある間はrollbackを拒否する。

BEGIN;

-- running確認とDDLの間に新しいclaimが割り込まないよう、job_runsをrollback完了まで
-- 固定する。後続ALTER TABLEと同じACCESS EXCLUSIVEを最初から取得し、待機writerとの
-- ロック昇格deadlockも避ける。既に進行中の処理は完了を待ってから確認する。
LOCK TABLE jquants_ingest.job_runs IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM jquants_ingest.job_runs jr
    WHERE jr.job_name = 'scouter-analyst-target-monitor'
      AND jr.status = 'running'
  ) THEN
    RAISE EXCEPTION 'analyst target monitor is still running; drain it before rollback';
  END IF;
END;
$$;

DROP FUNCTION IF EXISTS scouter.complete_external_search_request(bigint, uuid, uuid, text, smallint);
DROP FUNCTION IF EXISTS scouter.reserve_external_search_request(text, date, text, uuid, uuid);
DROP TABLE IF EXISTS scouter.external_search_requests;
DROP TABLE IF EXISTS scouter.external_search_budget_guard;

ALTER TABLE jquants_ingest.job_runs
  DROP CONSTRAINT IF EXISTS job_runs_job_name_check;

-- 既存のanalyst-target job_runsは監査ログとして残しつつ、rollback後の新規insertを
-- 拒否するためNOT VALIDで旧許可リストを復元する。
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
  )) NOT VALID;

COMMIT;
