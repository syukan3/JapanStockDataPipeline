-- 00085_create_dataset_coverage.sql の手動ロールバック。
-- supabase/migrations に置くと順方向migrationとして適用されるため、
-- 明示的に実行する rollbacks/ 配下で管理する。

DROP FUNCTION IF EXISTS jquants_core.get_earnings_coverage(date, text);
DROP FUNCTION IF EXISTS jquants_core.commit_earnings_calendar_attempt(date, uuid, uuid, timestamptz, jsonb);
DROP FUNCTION IF EXISTS jquants_core.fail_earnings_coverage_attempt(date, uuid, uuid, bigint, integer, timestamptz);
DROP TABLE IF EXISTS jquants_core.dataset_coverage;
DROP FUNCTION IF EXISTS jquants_ingest.complete_job_run_attempt(uuid, uuid, text, text, jsonb);
DROP FUNCTION IF EXISTS jquants_ingest.claim_job_run(text, date, jsonb, integer, integer, text);
ALTER TABLE jquants_ingest.job_runs DROP COLUMN IF EXISTS attempt_id;
