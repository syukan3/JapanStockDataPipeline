-- 00085_create_dataset_coverage.sql
-- Portfolio のサーバー側(service_role)から参照する、
-- 日付×データセットの取込coverage契約。
-- jquants_ingest.job_runs / job_heartbeat は内部管理用で非公開のため、
-- 「対象日を確認して明示的に0件だった」ことも jquants_core に記録する。

-- 同じrun_idを再利用するretryでも、claimごとにattempt_idを更新する。
-- reclaim前のworkerは以降の書込みをattempt_idで拒否される。
ALTER TABLE jquants_ingest.job_runs
  ADD COLUMN IF NOT EXISTS attempt_id uuid NOT NULL DEFAULT gen_random_uuid();

COMMENT ON COLUMN jquants_ingest.job_runs.attempt_id IS
  '同一run_id内の実行世代。retry/reclaimごとに更新し、旧workerの後書きを防ぐfencing token。';

CREATE TABLE IF NOT EXISTS jquants_core.dataset_coverage (
  dataset             text        NOT NULL,
  target_date         date        NOT NULL,
  status              text        NOT NULL CHECK (status IN ('success', 'failed')),
  row_count           bigint      NOT NULL DEFAULT 0 CHECK (row_count >= 0),
  error_count         integer     NOT NULL DEFAULT 0 CHECK (error_count >= 0),
  source_observed_at  timestamptz,
  run_id              uuid,
  updated_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (dataset, target_date),
  CONSTRAINT dataset_coverage_status_consistency CHECK (
    (status = 'success' AND error_count = 0 AND source_observed_at IS NOT NULL)
    OR (status = 'failed' AND error_count > 0)
  )
);

COMMENT ON TABLE jquants_core.dataset_coverage IS
  '日付×データセットの取込coverage。success + row_count=0は、対象日のソースを正常に確認した結果0件であることを示す。';
COMMENT ON COLUMN jquants_core.dataset_coverage.row_count IS
  '対象日に正常保存された行数。';
COMMENT ON COLUMN jquants_core.dataset_coverage.error_count IS
  '保存エラーまたはcoverage契約違反の件数。';
COMMENT ON COLUMN jquants_core.dataset_coverage.source_observed_at IS
  '外部ソースから応答を受け取った時刻。取得自体が失敗した場合はNULL。';
COMMENT ON COLUMN jquants_core.dataset_coverage.run_id IS
  '対応するjquants_ingest.job_runs.run_id。公開契約を管理スキーマの生存期間と切り離すためFKは張らない。';

CREATE INDEX IF NOT EXISTS idx_dataset_coverage_target_date
  ON jquants_core.dataset_coverage (target_date DESC, dataset);

DROP TRIGGER IF EXISTS trg_dataset_coverage_updated_at
  ON jquants_core.dataset_coverage;
CREATE TRIGGER trg_dataset_coverage_updated_at
  BEFORE UPDATE ON jquants_core.dataset_coverage
  FOR EACH ROW EXECUTE FUNCTION jquants_core.set_updated_at();

ALTER TABLE jquants_core.dataset_coverage ENABLE ROW LEVEL SECURITY;
ALTER TABLE jquants_core.dataset_coverage FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON jquants_core.dataset_coverage;
CREATE POLICY "service_role_all"
  ON jquants_core.dataset_coverage
  FOR ALL TO service_role
  USING (true)
  WITH CHECK (true);

REVOKE ALL ON jquants_core.dataset_coverage FROM PUBLIC, anon, authenticated;
GRANT ALL ON jquants_core.dataset_coverage TO service_role;

-- failed / stale running / stale success / fresh duplicate の判定とclaimを
-- 1トランザクション・1行ロック内で完結させる。
CREATE OR REPLACE FUNCTION jquants_ingest.claim_job_run(
  p_job_name text,
  p_target_date date,
  p_meta jsonb,
  p_running_stale_after_seconds integer,
  p_success_stale_after_seconds integer,
  p_coverage_dataset text
)
RETURNS TABLE (
  run_id uuid,
  attempt_id uuid,
  claimed boolean,
  reason text
)
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_run_id uuid;
  v_current_attempt_id uuid;
  v_new_attempt_id uuid := gen_random_uuid();
  v_status text;
  v_started_at timestamptz;
  v_finished_at timestamptz;
  v_reason text;
BEGIN
  IF p_target_date IS NULL THEN
    RAISE EXCEPTION 'target_date is required for atomic job claim';
  END IF;
  IF p_running_stale_after_seconds IS NOT NULL
     AND p_running_stale_after_seconds <= 0 THEN
    RAISE EXCEPTION 'running stale threshold must be positive';
  END IF;
  IF p_success_stale_after_seconds IS NOT NULL
     AND p_success_stale_after_seconds <= 0 THEN
    RAISE EXCEPTION 'success stale threshold must be positive';
  END IF;
  IF p_coverage_dataset IS NOT NULL
     AND p_coverage_dataset <> 'earnings_calendar' THEN
    RAISE EXCEPTION 'unsupported coverage dataset: %', p_coverage_dataset;
  END IF;
  IF p_coverage_dataset = 'earnings_calendar'
     AND p_job_name <> 'cron_b' THEN
    RAISE EXCEPTION 'earnings_calendar coverage can only be claimed by cron_b';
  END IF;

  INSERT INTO jquants_ingest.job_runs AS jr (
    job_name,
    target_date,
    status,
    started_at,
    finished_at,
    error_message,
    meta,
    attempt_id
  )
  VALUES (
    p_job_name,
    p_target_date,
    'running',
    v_now,
    NULL,
    NULL,
    coalesce(p_meta, '{}'::jsonb),
    v_new_attempt_id
  )
  ON CONFLICT (job_name, target_date) WHERE target_date IS NOT NULL
  DO NOTHING
  RETURNING jr.run_id, jr.attempt_id
    INTO v_run_id, v_current_attempt_id;

  IF v_run_id IS NOT NULL THEN
    IF p_coverage_dataset IS NOT NULL THEN
      INSERT INTO jquants_core.dataset_coverage AS dc (
        dataset,
        target_date,
        status,
        row_count,
        error_count,
        source_observed_at,
        run_id
      )
      VALUES (p_coverage_dataset, p_target_date, 'failed', 0, 1, NULL, v_run_id)
      ON CONFLICT (dataset, target_date) DO UPDATE
      SET
        status = EXCLUDED.status,
        row_count = EXCLUDED.row_count,
        error_count = EXCLUDED.error_count,
        source_observed_at = EXCLUDED.source_observed_at,
        run_id = EXCLUDED.run_id;
    END IF;

    INSERT INTO jquants_ingest.job_heartbeat AS jh (
      job_name,
      last_seen_at,
      last_status,
      last_run_id,
      last_target_date,
      last_error,
      meta
    )
    VALUES (p_job_name, v_now, 'running', v_run_id, p_target_date, NULL, '{}'::jsonb)
    ON CONFLICT (job_name) DO UPDATE
    SET
      last_seen_at = EXCLUDED.last_seen_at,
      last_status = EXCLUDED.last_status,
      last_run_id = EXCLUDED.last_run_id,
      last_target_date = EXCLUDED.last_target_date,
      last_error = EXCLUDED.last_error,
      meta = EXCLUDED.meta;

    RETURN QUERY
      SELECT v_run_id, v_current_attempt_id, true, 'inserted'::text;
    RETURN;
  END IF;

  SELECT
    jr.run_id,
    jr.attempt_id,
    jr.status,
    jr.started_at,
    jr.finished_at
  INTO
    v_run_id,
    v_current_attempt_id,
    v_status,
    v_started_at,
    v_finished_at
  FROM jquants_ingest.job_runs jr
  WHERE jr.job_name = p_job_name
    AND jr.target_date = p_target_date
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'job run disappeared during claim'
      USING ERRCODE = '40001';
  END IF;

  IF v_status = 'failed' THEN
    v_reason := 'failed';
  ELSIF v_status = 'running'
    AND p_running_stale_after_seconds IS NOT NULL
    AND v_started_at < v_now - make_interval(secs => p_running_stale_after_seconds)
  THEN
    v_reason := 'stale_running';
  ELSIF v_status = 'success'
    AND p_success_stale_after_seconds IS NOT NULL
    AND v_finished_at IS NOT NULL
    AND v_finished_at < v_now - make_interval(secs => p_success_stale_after_seconds)
  THEN
    v_reason := 'stale_success';
  ELSE
    RETURN QUERY
      SELECT v_run_id, NULL::uuid, false, 'already_executed'::text;
    RETURN;
  END IF;

  UPDATE jquants_ingest.job_runs jr
  SET
    status = 'running',
    started_at = v_now,
    finished_at = NULL,
    error_message = NULL,
    meta = coalesce(p_meta, '{}'::jsonb),
    attempt_id = v_new_attempt_id
  WHERE jr.run_id = v_run_id;

  DELETE FROM jquants_ingest.job_run_items jri
  WHERE jri.run_id = v_run_id;

  IF p_coverage_dataset IS NOT NULL THEN
    INSERT INTO jquants_core.dataset_coverage AS dc (
      dataset,
      target_date,
      status,
      row_count,
      error_count,
      source_observed_at,
      run_id
    )
    VALUES (p_coverage_dataset, p_target_date, 'failed', 0, 1, NULL, v_run_id)
    ON CONFLICT (dataset, target_date) DO UPDATE
    SET
      status = EXCLUDED.status,
      row_count = EXCLUDED.row_count,
      error_count = EXCLUDED.error_count,
      source_observed_at = EXCLUDED.source_observed_at,
      run_id = EXCLUDED.run_id;
  END IF;

  INSERT INTO jquants_ingest.job_heartbeat AS jh (
    job_name,
    last_seen_at,
    last_status,
    last_run_id,
    last_target_date,
    last_error,
    meta
  )
  VALUES (p_job_name, v_now, 'running', v_run_id, p_target_date, NULL, '{}'::jsonb)
  ON CONFLICT (job_name) DO UPDATE
  SET
    last_seen_at = EXCLUDED.last_seen_at,
    last_status = EXCLUDED.last_status,
    last_run_id = EXCLUDED.last_run_id,
    last_target_date = EXCLUDED.last_target_date,
    last_error = EXCLUDED.last_error,
    meta = EXCLUDED.meta;

  RETURN QUERY
    SELECT v_run_id, v_new_attempt_id, true, v_reason;
END;
$$;

-- reclaim後の旧workerはjob statusを完了できない。
CREATE OR REPLACE FUNCTION jquants_ingest.complete_job_run_attempt(
  p_run_id uuid,
  p_attempt_id uuid,
  p_status text,
  p_error_message text,
  p_heartbeat_meta jsonb
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  v_updated boolean;
  v_job_name text;
  v_target_date date;
BEGIN
  IF p_status NOT IN ('success', 'failed') THEN
    RAISE EXCEPTION 'invalid terminal job status: %', p_status;
  END IF;

  UPDATE jquants_ingest.job_runs jr
  SET
    status = p_status,
    finished_at = clock_timestamp(),
    error_message = p_error_message
  WHERE jr.run_id = p_run_id
    AND jr.attempt_id = p_attempt_id
    AND jr.status = 'running'
  RETURNING true, jr.job_name, jr.target_date
    INTO v_updated, v_job_name, v_target_date;

  IF NOT coalesce(v_updated, false) THEN
    RETURN false;
  END IF;

  INSERT INTO jquants_ingest.job_heartbeat AS jh (
    job_name,
    last_seen_at,
    last_status,
    last_run_id,
    last_target_date,
    last_error,
    meta
  )
  VALUES (
    v_job_name,
    clock_timestamp(),
    p_status,
    p_run_id,
    v_target_date,
    p_error_message,
    coalesce(p_heartbeat_meta, '{}'::jsonb)
  )
  ON CONFLICT (job_name) DO UPDATE
  SET
    last_seen_at = EXCLUDED.last_seen_at,
    last_status = EXCLUDED.last_status,
    last_run_id = EXCLUDED.last_run_id,
    last_target_date = EXCLUDED.last_target_date,
    last_error = EXCLUDED.last_error,
    meta = EXCLUDED.meta;

  RETURN true;
END;
$$;

REVOKE ALL ON FUNCTION jquants_ingest.claim_job_run(text, date, jsonb, integer, integer, text)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION jquants_ingest.claim_job_run(text, date, jsonb, integer, integer, text)
  TO service_role;
REVOKE ALL ON FUNCTION jquants_ingest.complete_job_run_attempt(uuid, uuid, text, text, jsonb)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION jquants_ingest.complete_job_run_attempt(uuid, uuid, text, text, jsonb)
  TO service_role;

-- API取得・保存失敗時も、現在のattemptだけがfailed manifestを更新できる。
CREATE OR REPLACE FUNCTION jquants_core.fail_earnings_coverage_attempt(
  p_target_date date,
  p_run_id uuid,
  p_attempt_id uuid,
  p_row_count bigint,
  p_error_count integer,
  p_source_observed_at timestamptz
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
BEGIN
  PERFORM 1
  FROM jquants_ingest.job_runs jr
  WHERE jr.run_id = p_run_id
    AND jr.job_name = 'cron_b'
    AND jr.target_date = p_target_date
    AND jr.status = 'running'
    AND jr.attempt_id = p_attempt_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN false;
  END IF;

  INSERT INTO jquants_core.dataset_coverage AS dc (
    dataset,
    target_date,
    status,
    row_count,
    error_count,
    source_observed_at,
    run_id
  )
  VALUES (
    'earnings_calendar',
    p_target_date,
    'failed',
    greatest(coalesce(p_row_count, 0), 0),
    greatest(coalesce(p_error_count, 1), 1),
    p_source_observed_at,
    p_run_id
  )
  ON CONFLICT (dataset, target_date) DO UPDATE
  SET
    status = EXCLUDED.status,
    row_count = EXCLUDED.row_count,
    error_count = EXCLUDED.error_count,
    source_observed_at = EXCLUDED.source_observed_at,
    run_id = EXCLUDED.run_id;

  RETURN true;
END;
$$;

-- job rowをロックしてattempt所有権を検証し、対象日集合の全置換・実件数確認・
-- success manifest公開を1トランザクションで確定する。
CREATE OR REPLACE FUNCTION jquants_core.commit_earnings_calendar_attempt(
  p_target_date date,
  p_run_id uuid,
  p_attempt_id uuid,
  p_source_observed_at timestamptz,
  p_records jsonb
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  v_expected_count bigint;
  v_actual_count bigint;
BEGIN
  PERFORM 1
  FROM jquants_ingest.job_runs jr
  WHERE jr.run_id = p_run_id
    AND jr.job_name = 'cron_b'
    AND jr.target_date = p_target_date
    AND jr.status = 'running'
    AND jr.attempt_id = p_attempt_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'stale Cron B attempt cannot commit earnings calendar';
  END IF;
  IF p_source_observed_at IS NULL THEN
    RAISE EXCEPTION 'source_observed_at is required for successful coverage';
  END IF;
  IF p_records IS NULL OR jsonb_typeof(p_records) <> 'array' THEN
    RAISE EXCEPTION 'earnings calendar records must be a JSON array';
  END IF;

  v_expected_count := jsonb_array_length(p_records);

  IF EXISTS (
    SELECT 1
    FROM jsonb_to_recordset(p_records) AS r(
      announcement_date date,
      local_code text,
      company_name text,
      fiscal_year text,
      fiscal_quarter text,
      sector_name text
    )
    WHERE r.announcement_date IS DISTINCT FROM p_target_date
       OR coalesce(r.local_code, '') = ''
  ) THEN
    RAISE EXCEPTION 'earnings calendar records violate target date or local code contract';
  END IF;

  DELETE FROM jquants_core.earnings_calendar ec
  WHERE ec.announcement_date = p_target_date;

  INSERT INTO jquants_core.earnings_calendar (
    announcement_date,
    local_code,
    company_name,
    fiscal_year,
    fiscal_quarter,
    sector_name
  )
  SELECT
    r.announcement_date,
    r.local_code,
    r.company_name,
    r.fiscal_year,
    r.fiscal_quarter,
    r.sector_name
  FROM jsonb_to_recordset(p_records) AS r(
    announcement_date date,
    local_code text,
    company_name text,
    fiscal_year text,
    fiscal_quarter text,
    sector_name text
  );

  SELECT count(*)::bigint
  INTO v_actual_count
  FROM jquants_core.earnings_calendar ec
  WHERE ec.announcement_date = p_target_date;

  IF v_actual_count <> v_expected_count THEN
    RAISE EXCEPTION
      'earnings calendar row count mismatch for %: expected %, got %',
      p_target_date,
      v_expected_count,
      v_actual_count;
  END IF;

  INSERT INTO jquants_core.dataset_coverage AS dc (
    dataset,
    target_date,
    status,
    row_count,
    error_count,
    source_observed_at,
    run_id
  )
  VALUES (
    'earnings_calendar',
    p_target_date,
    'success',
    v_actual_count,
    0,
    p_source_observed_at,
    p_run_id
  )
  ON CONFLICT (dataset, target_date) DO UPDATE
  SET
    status = EXCLUDED.status,
    row_count = EXCLUDED.row_count,
    error_count = EXCLUDED.error_count,
    source_observed_at = EXCLUDED.source_observed_at,
    run_id = EXCLUDED.run_id;

  RETURN v_actual_count;
END;
$$;

REVOKE ALL ON FUNCTION jquants_core.fail_earnings_coverage_attempt(date, uuid, uuid, bigint, integer, timestamptz)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION jquants_core.fail_earnings_coverage_attempt(date, uuid, uuid, bigint, integer, timestamptz)
  TO service_role;
REVOKE ALL ON FUNCTION jquants_core.commit_earnings_calendar_attempt(date, uuid, uuid, timestamptz, jsonb)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION jquants_core.commit_earnings_calendar_attempt(date, uuid, uuid, timestamptz, jsonb)
  TO service_role;

-- manifestと実データを1つのSQL statement/MVCC snapshotで読み、
-- 件数が同じまま銘柄集合が入れ替わる更新でも交錯させない。
CREATE OR REPLACE FUNCTION jquants_core.get_earnings_coverage(
  p_target_date date,
  p_local_code text
)
RETURNS TABLE (
  dataset             text,
  target_date         date,
  status              text,
  row_count           bigint,
  error_count         integer,
  source_observed_at  timestamptz,
  run_id              uuid,
  actual_count        bigint,
  has_local_event     boolean
)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = ''
AS $$
  WITH event_stats AS (
    SELECT
      count(*)::bigint AS actual_count,
      coalesce(bool_or(ec.local_code = p_local_code), false) AS has_local_event
    FROM jquants_core.earnings_calendar ec
    WHERE ec.announcement_date = p_target_date
  )
  SELECT
    'earnings_calendar'::text AS dataset,
    p_target_date AS target_date,
    dc.status,
    dc.row_count,
    dc.error_count,
    dc.source_observed_at,
    dc.run_id,
    es.actual_count,
    es.has_local_event
  FROM event_stats es
  LEFT JOIN jquants_core.dataset_coverage dc
    ON dc.dataset = 'earnings_calendar'
   AND dc.target_date = p_target_date
$$;

COMMENT ON FUNCTION jquants_core.get_earnings_coverage(date, text) IS
  '決算発表coverage manifest、対象日実件数、対象銘柄イベント有無を単一snapshotで返す。manifest未作成時もstatus等をNULLとして1行返す。';

REVOKE ALL ON FUNCTION jquants_core.get_earnings_coverage(date, text)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION jquants_core.get_earnings_coverage(date, text)
  TO service_role;
