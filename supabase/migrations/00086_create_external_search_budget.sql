-- 00086_create_external_search_budget.sql
-- Analyst Target Monitor が外部検索APIを呼ぶ前に使う、DB強制の予算予約ledger。
--
-- 不変条件:
--   * job_runs の current attempt だけが予約・完了できる。
--   * provider × scan_date × local_code は1度だけ予約し、retry/reclaimでも再課金しない。
--   * quota時計はcaller入力ではなくDB serverのreserved_atを使う。
--   * providerごとのguard行をFOR UPDATEしてから、日次35件・実時間30日900件を判定する。
--   * 検索query/response/title/snippet/URLは保存せず、安全なoutcomeだけを残す。
--   * service_roleにもtable直接DMLを許さず、SECURITY DEFINER RPCだけを公開する。

-- ---------------------------------------------------------------------------
-- 1. Analyst Target Monitorをjob_runsの許可リストへ追加
-- ---------------------------------------------------------------------------
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
    'scouter-analyst-target-monitor'
  ));

-- ---------------------------------------------------------------------------
-- 2. provider単位の固定hard ceilingと予約ledger
-- ---------------------------------------------------------------------------
CREATE TABLE scouter.external_search_budget_guard (
  provider                   text        PRIMARY KEY,
  daily_request_limit        integer     NOT NULL DEFAULT 35,
  rolling_30d_request_limit  integer     NOT NULL DEFAULT 900,
  created_at                 timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT external_search_budget_guard_provider_chk
    CHECK (provider ~ '^[a-z][a-z0-9_]*$'),
  -- callerや環境変数でDB hard ceilingを増減させない。より小さい運用上限は
  -- Scouter側で追加適用する。
  CONSTRAINT external_search_budget_guard_daily_limit_chk
    CHECK (daily_request_limit = 35),
  CONSTRAINT external_search_budget_guard_rolling_limit_chk
    CHECK (rolling_30d_request_limit = 900)
);

COMMENT ON TABLE scouter.external_search_budget_guard IS
  '外部検索providerごとのhard ceiling。予約RPCがprovider行をFOR UPDATEしてquota判定を直列化する。';

INSERT INTO scouter.external_search_budget_guard (
  provider,
  daily_request_limit,
  rolling_30d_request_limit
)
VALUES ('brave_search', 35, 900)
ON CONFLICT (provider) DO NOTHING;

CREATE TABLE scouter.external_search_requests (
  reservation_id    bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  provider          text        NOT NULL,
  scan_date         date        NOT NULL,
  local_code        text        NOT NULL,
  reserved_at       timestamptz NOT NULL DEFAULT now(),
  run_id            uuid        NOT NULL,
  attempt_id        uuid        NOT NULL,
  outcome           text,
  http_status_class smallint,
  completed_at      timestamptz,
  CONSTRAINT external_search_requests_provider_fk
    FOREIGN KEY (provider)
    REFERENCES scouter.external_search_budget_guard(provider)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT,
  CONSTRAINT external_search_requests_local_code_chk
    CHECK (local_code ~ '^[0-9]{4,5}$'),
  CONSTRAINT external_search_requests_outcome_chk
    CHECK (outcome IS NULL OR outcome IN (
      'succeeded', 'http_error', 'timeout', 'invalid_response'
    )),
  CONSTRAINT external_search_requests_http_class_chk
    CHECK (http_status_class IS NULL OR http_status_class IN (2, 4, 5)),
  CONSTRAINT external_search_requests_completion_chk
    CHECK (
      (outcome IS NULL AND http_status_class IS NULL AND completed_at IS NULL)
      OR
      (
        outcome IS NOT NULL
        AND completed_at IS NOT NULL
        AND (
          (outcome = 'succeeded' AND http_status_class = 2)
          OR (outcome = 'http_error' AND http_status_class IN (4, 5))
          OR (outcome = 'timeout' AND http_status_class IS NULL)
          OR (outcome = 'invalid_response' AND (http_status_class IS NULL OR http_status_class = 2))
        )
      )
    ),
  CONSTRAINT external_search_requests_provider_scan_code_key
    UNIQUE (provider, scan_date, local_code)
);

COMMENT ON TABLE scouter.external_search_requests IS
  '外部検索1 HTTP requestごとの予約ledger。検索内容は保存せず、安全なoutcomeとHTTP classだけを記録する。';
COMMENT ON COLUMN scouter.external_search_requests.reserved_at IS
  'DB serverが生成するquota時計。callerのscan_dateはquota日付に使わない。';
COMMENT ON COLUMN scouter.external_search_requests.http_status_class IS
  'レスポンス本文を保存せず、2/4/5のHTTP classだけを記録する。';

-- rolling 30日・JST当日の両countはprovider equality + reserved_at rangeで読む。
CREATE INDEX idx_external_search_requests_provider_reserved_at
  ON scouter.external_search_requests (provider, reserved_at DESC);

ALTER TABLE scouter.external_search_budget_guard ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.external_search_budget_guard FORCE ROW LEVEL SECURITY;
ALTER TABLE scouter.external_search_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.external_search_requests FORCE ROW LEVEL SECURITY;

-- 00016のdefault privilege（authenticated SELECT / service_role ALL）を明示的に剥奪。
-- owner権限で動く専用RPC以外からはSELECTもDMLもできない。
REVOKE ALL ON TABLE scouter.external_search_budget_guard
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE scouter.external_search_requests
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON SEQUENCE scouter.external_search_requests_reservation_id_seq
  FROM PUBLIC, anon, authenticated, service_role;

-- ---------------------------------------------------------------------------
-- 3. 原子的な検索予約
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION scouter.reserve_external_search_request(
  p_provider text,
  p_scan_date date,
  p_local_code text,
  p_run_id uuid,
  p_attempt_id uuid
)
RETURNS TABLE (
  reservation_id bigint,
  decision text,
  should_call boolean,
  prior_outcome text,
  daily_used integer,
  rolling_30d_used integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_now timestamptz;
  v_today_jst date;
  v_day_start timestamptz;
  v_day_end timestamptz;
  v_daily_limit integer;
  v_rolling_limit integer;
  v_daily_used integer;
  v_rolling_used integer;
  v_reservation_id bigint;
  v_prior_outcome text;
BEGIN
  IF p_provider IS NULL OR p_provider = '' THEN
    RAISE EXCEPTION 'provider is required';
  END IF;
  IF p_scan_date IS NULL THEN
    RAISE EXCEPTION 'scan_date is required';
  END IF;
  IF p_local_code IS NULL OR p_local_code !~ '^[0-9]{4,5}$' THEN
    RAISE EXCEPTION 'local_code must be 4 or 5 digits';
  END IF;
  IF p_run_id IS NULL OR p_attempt_id IS NULL THEN
    RAISE EXCEPTION 'run_id and attempt_id are required';
  END IF;

  -- job rowを最初にロックする。全RPCで job_runs -> provider guard の順に
  -- ロックしてdeadlockを避ける。target_dateもscan_dateと完全一致させる。
  PERFORM 1
  FROM jquants_ingest.job_runs jr
  WHERE jr.run_id = p_run_id
    AND jr.job_name = 'scouter-analyst-target-monitor'
    AND jr.target_date = p_scan_date
    AND jr.status = 'running'
    AND jr.attempt_id = p_attempt_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'stale or mismatched analyst target monitor attempt'
      USING ERRCODE = '55000';
  END IF;

  -- providerごとの全予約を短いtransaction内で直列化する。
  SELECT
    g.daily_request_limit,
    g.rolling_30d_request_limit
  INTO
    v_daily_limit,
    v_rolling_limit
  FROM scouter.external_search_budget_guard g
  WHERE g.provider = p_provider
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'unsupported external search provider: %', p_provider;
  END IF;

  -- guard待機中にJSTの日付や30日境界を跨ぐ可能性があるため、quotaを直列化する
  -- provider行を取得した後のserver時刻をreserved_atと全countで共有する。
  v_now := clock_timestamp();
  v_today_jst := (timezone('Asia/Tokyo', v_now))::date;
  v_day_start := v_today_jst::timestamp AT TIME ZONE 'Asia/Tokyo';
  v_day_end := (v_today_jst + 1)::timestamp AT TIME ZONE 'Asia/Tokyo';

  SELECT count(*)::integer
  INTO v_daily_used
  FROM scouter.external_search_requests r
  WHERE r.provider = p_provider
    AND r.reserved_at >= v_day_start
    AND r.reserved_at < v_day_end;

  SELECT count(*)::integer
  INTO v_rolling_used
  FROM scouter.external_search_requests r
  WHERE r.provider = p_provider
    -- calendar dayではなく実時間30日（720時間）を固定する。
    AND r.reserved_at > v_now - interval '720 hours'
    AND r.reserved_at <= v_now;

  -- 同一provider/date/codeはquota到達後でも既存予約を返す。retry/reclaimは
  -- HTTPを再実行せず、既存のsafe outcomeだけを参照できる。
  SELECT r.reservation_id, r.outcome
  INTO v_reservation_id, v_prior_outcome
  FROM scouter.external_search_requests r
  WHERE r.provider = p_provider
    AND r.scan_date = p_scan_date
    AND r.local_code = p_local_code;

  IF FOUND THEN
    RETURN QUERY SELECT
      v_reservation_id,
      'already_reserved'::text,
      false,
      v_prior_outcome,
      v_daily_used,
      v_rolling_used;
    RETURN;
  END IF;

  IF v_daily_used >= v_daily_limit THEN
    RETURN QUERY SELECT
      NULL::bigint,
      'daily_limit_reached'::text,
      false,
      NULL::text,
      v_daily_used,
      v_rolling_used;
    RETURN;
  END IF;

  IF v_rolling_used >= v_rolling_limit THEN
    RETURN QUERY SELECT
      NULL::bigint,
      'rolling_limit_reached'::text,
      false,
      NULL::text,
      v_daily_used,
      v_rolling_used;
    RETURN;
  END IF;

  INSERT INTO scouter.external_search_requests (
    provider,
    scan_date,
    local_code,
    reserved_at,
    run_id,
    attempt_id
  )
  VALUES (
    p_provider,
    p_scan_date,
    p_local_code,
    v_now,
    p_run_id,
    p_attempt_id
  )
  RETURNING external_search_requests.reservation_id
  INTO v_reservation_id;

  RETURN QUERY SELECT
    v_reservation_id,
    'reserved'::text,
    true,
    NULL::text,
    v_daily_used + 1,
    v_rolling_used + 1;
END;
$$;

-- ---------------------------------------------------------------------------
-- 4. current attemptによる安全な完了記録
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION scouter.complete_external_search_request(
  p_reservation_id bigint,
  p_run_id uuid,
  p_attempt_id uuid,
  p_outcome text,
  p_http_status_class smallint DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_updated boolean;
BEGIN
  IF p_reservation_id IS NULL OR p_run_id IS NULL OR p_attempt_id IS NULL THEN
    RAISE EXCEPTION 'reservation_id, run_id and attempt_id are required';
  END IF;
  IF p_outcome IS NULL
     OR p_outcome NOT IN ('succeeded', 'http_error', 'timeout', 'invalid_response')
  THEN
    RAISE EXCEPTION 'invalid external search outcome';
  END IF;
  IF p_http_status_class IS NOT NULL AND p_http_status_class NOT IN (2, 4, 5) THEN
    RAISE EXCEPTION 'http_status_class must be 2, 4, 5 or null';
  END IF;
  IF (p_outcome = 'succeeded' AND p_http_status_class IS DISTINCT FROM 2)
     OR (p_outcome = 'http_error' AND (
       p_http_status_class IS NULL OR p_http_status_class NOT IN (4, 5)
     ))
     OR (p_outcome = 'timeout' AND p_http_status_class IS NOT NULL)
     OR (p_outcome = 'invalid_response' AND p_http_status_class IS NOT NULL
         AND p_http_status_class <> 2)
  THEN
    RAISE EXCEPTION 'outcome and http_status_class are inconsistent';
  END IF;

  -- complete_job_run_attemptと同じjob fence。旧workerはfalseとなり、
  -- request行を更新できない。
  PERFORM 1
  FROM jquants_ingest.job_runs jr
  WHERE jr.run_id = p_run_id
    AND jr.job_name = 'scouter-analyst-target-monitor'
    AND jr.status = 'running'
    AND jr.attempt_id = p_attempt_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN false;
  END IF;

  UPDATE scouter.external_search_requests r
  SET
    outcome = p_outcome,
    http_status_class = p_http_status_class,
    completed_at = clock_timestamp()
  WHERE r.reservation_id = p_reservation_id
    AND r.run_id = p_run_id
    AND r.attempt_id = p_attempt_id
    AND r.outcome IS NULL
  RETURNING true INTO v_updated;

  RETURN coalesce(v_updated, false);
END;
$$;

REVOKE ALL ON FUNCTION scouter.reserve_external_search_request(text, date, text, uuid, uuid)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION scouter.reserve_external_search_request(text, date, text, uuid, uuid)
  TO service_role;

REVOKE ALL ON FUNCTION scouter.complete_external_search_request(bigint, uuid, uuid, text, smallint)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION scouter.complete_external_search_request(bigint, uuid, uuid, text, smallint)
  TO service_role;
