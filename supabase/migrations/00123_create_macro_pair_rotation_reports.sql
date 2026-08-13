-- 00123_create_macro_pair_rotation_reports.sql
-- マクロ対立軸・週次ローテーションレポートの共有DB契約。
-- 正本: ../docs/PLANS-macro-pair-rotation-report-2026-08.md
--
-- 設計要点:
--   * report_period_date（JST週の土曜）を週次冪等キーとし、reportは完全immutable。
--   * メール送信状態は1:1 outboxへ分離し、直接DMLを禁止してRPCだけで遷移させる。
--   * timeout/lease切れはunknownへ隔離し、自動再送しない。
--   * pg_cronが唯一の定時起動元。manifestはenabled=falseで安全に導入する。

-- ============================================================================
-- 1. Immutable report snapshot
-- ============================================================================

CREATE TABLE scouter.macro_pair_rotation_reports (
  report_period_date    DATE PRIMARY KEY,
  as_of_date            DATE NOT NULL,
  comparison_date       DATE NOT NULL,
  schema_version        SMALLINT NOT NULL,
  strategy_version      TEXT NOT NULL,
  information_cutoff_at TIMESTAMPTZ NOT NULL,
  summary               TEXT NOT NULL,
  axes                  JSONB NOT NULL,
  caveats               JSONB NOT NULL,
  contract_hash         TEXT NOT NULL,
  generated_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

  CONSTRAINT macro_pair_rotation_period_is_saturday_chk
    CHECK (extract(isodow FROM report_period_date) = 6),
  CONSTRAINT macro_pair_rotation_dates_chk
    CHECK (comparison_date < as_of_date AND as_of_date <= report_period_date),
  CONSTRAINT macro_pair_rotation_schema_version_chk
    CHECK (schema_version = 1),
  CONSTRAINT macro_pair_rotation_strategy_version_chk
    CHECK (char_length(btrim(strategy_version)) BETWEEN 1 AND 100),
  CONSTRAINT macro_pair_rotation_summary_chk
    CHECK (char_length(btrim(summary)) BETWEEN 1 AND 4000),
  CONSTRAINT macro_pair_rotation_caveats_chk
    CHECK (
      jsonb_typeof(caveats) = 'array'
      AND jsonb_array_length(caveats) BETWEEN 1 AND 32
    ),
  CONSTRAINT macro_pair_rotation_contract_hash_chk
    CHECK (
      contract_hash = 'a7f79568c12767e7ebeb4174c48c12264a959fd380a2f3e26c5fe43cb951073e'
    ),
  CONSTRAINT macro_pair_rotation_axes_v1_chk
    CHECK (
      jsonb_typeof(axes) = 'array'
      AND jsonb_array_length(axes) = 4
      AND jsonb_array_length(jsonb_path_query_array(axes, '$[*] ? (@.axisId == "rates")')) = 1
      AND jsonb_array_length(jsonb_path_query_array(axes, '$[*] ? (@.axisId == "oil")')) = 1
      AND jsonb_array_length(jsonb_path_query_array(axes, '$[*] ? (@.axisId == "fx")')) = 1
      AND jsonb_array_length(jsonb_path_query_array(axes, '$[*] ? (@.axisId == "risk")')) = 1
      AND jsonb_array_length(jsonb_path_query_array(axes, '$[*] ? (@.validationStatus == "unvalidated")')) = 4
      AND jsonb_array_length(jsonb_path_query_array(axes, '$[*] ? (@.axisId == "rates" && @.driver.seriesId == "mof_jgb_20y")')) = 1
      AND jsonb_array_length(jsonb_path_query_array(axes, '$[*] ? (@.axisId == "oil" && @.driver.seriesId == "DCOILWTICO")')) = 1
      AND jsonb_array_length(jsonb_path_query_array(axes, '$[*] ? (@.axisId == "fx" && @.driver.seriesId == "DEXJPUS")')) = 1
      AND jsonb_array_length(jsonb_path_query_array(axes, '$[*] ? (@.axisId == "risk" && @.driver.seriesId == "VIXCLS")')) = 1
    )
);

COMMENT ON TABLE scouter.macro_pair_rotation_reports IS
  'マクロ4対立軸の週次immutable snapshot。予測力未検証の観測レポートであり売買推奨ではない。';
COMMENT ON COLUMN scouter.macro_pair_rotation_reports.report_period_date IS
  'JST週の土曜日。週次冪等性・job_runs.target_date・outboxの正本キー。';
COMMENT ON COLUMN scouter.macro_pair_rotation_reports.as_of_date IS
  'report_period_date以前の直近営業日。ETF current観測日。';
COMMENT ON COLUMN scouter.macro_pair_rotation_reports.comparison_date IS
  'as_of_dateをindex 0とする営業日降順列のindex 20。';
COMMENT ON COLUMN scouter.macro_pair_rotation_reports.information_cutoff_at IS
  'latest-storedマクロ8点を単一statement snapshotで観測したDB時刻。公表・改訂時刻の保証ではない。';
COMMENT ON COLUMN scouter.macro_pair_rotation_reports.contract_hash IS
  'docs/contracts/macro-pair-rotation-v1.schema.jsonのSHA-256。';

-- ============================================================================
-- 2. Mutable email outbox (reportと1:1)
-- ============================================================================

CREATE TABLE scouter.macro_pair_rotation_email_outbox (
  report_period_date  DATE PRIMARY KEY
                      REFERENCES scouter.macro_pair_rotation_reports(report_period_date)
                      ON UPDATE RESTRICT ON DELETE RESTRICT,
  status              TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending','sending','sent','failed','unknown')),
  idempotency_key     TEXT NOT NULL UNIQUE,
  attempt_id          UUID,
  attempt_count       INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  lease_expires_at    TIMESTAMPTZ,
  payload_hash        TEXT CHECK (payload_hash IS NULL OR payload_hash ~ '^[0-9a-f]{64}$'),
  recipient           TEXT,
  subject             TEXT,
  render_version      TEXT,
  provider_message_id TEXT,
  sent_at             TIMESTAMPTZ,
  last_error          TEXT,
  reconciled_at       TIMESTAMPTZ,
  reconciliation_note TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

  CONSTRAINT macro_pair_rotation_outbox_idempotency_chk
    CHECK (idempotency_key ~ '^macro-pair-rotation:[0-9]{4}-[0-9]{2}-[0-9]{2}:.+$'),
  CONSTRAINT macro_pair_rotation_outbox_envelope_chk CHECK (
    (status = 'pending'
      AND attempt_id IS NULL AND attempt_count = 0 AND lease_expires_at IS NULL
      AND payload_hash IS NULL AND recipient IS NULL AND subject IS NULL
      AND render_version IS NULL AND provider_message_id IS NULL AND sent_at IS NULL
      AND last_error IS NULL AND reconciled_at IS NULL AND reconciliation_note IS NULL)
    OR
    (status = 'sending'
      AND attempt_id IS NOT NULL AND attempt_count > 0 AND lease_expires_at IS NOT NULL
      AND payload_hash IS NOT NULL AND recipient IS NOT NULL AND subject IS NOT NULL
      AND render_version IS NOT NULL AND provider_message_id IS NULL AND sent_at IS NULL
      AND last_error IS NULL AND reconciled_at IS NULL AND reconciliation_note IS NULL)
    OR
    (status IN ('failed','unknown')
      AND attempt_id IS NOT NULL AND attempt_count > 0 AND lease_expires_at IS NULL
      AND payload_hash IS NOT NULL AND recipient IS NOT NULL AND subject IS NOT NULL
      AND render_version IS NOT NULL AND provider_message_id IS NULL AND sent_at IS NULL
      AND char_length(btrim(last_error)) BETWEEN 1 AND 10000
      AND reconciled_at IS NULL AND reconciliation_note IS NULL)
    OR
    (status = 'sent'
      AND attempt_id IS NOT NULL AND attempt_count > 0 AND lease_expires_at IS NULL
      AND payload_hash IS NOT NULL AND recipient IS NOT NULL AND subject IS NOT NULL
      AND render_version IS NOT NULL
      AND char_length(btrim(provider_message_id)) BETWEEN 1 AND 500
      AND sent_at IS NOT NULL AND last_error IS NULL
      AND (
        (reconciled_at IS NULL AND reconciliation_note IS NULL)
        OR
        (reconciled_at IS NOT NULL
          AND char_length(btrim(reconciliation_note)) BETWEEN 1 AND 2000)
      ))
  )
);

COMMENT ON TABLE scouter.macro_pair_rotation_email_outbox IS
  '週次マクロ対立軸レポートの1:1メールoutbox。直接DMLは禁止し、claim/finalize/retry/reconcile RPCだけで状態遷移する。';
COMMENT ON COLUMN scouter.macro_pair_rotation_email_outbox.idempotency_key IS
  'macro-pair-rotation:<report_period_date>:<strategy_version>。平日manualと土曜定時で同じ週キーを使い、provider送信時も同じ値を使う。';
COMMENT ON COLUMN scouter.macro_pair_rotation_email_outbox.payload_hash IS
  'versioned rendererが生成したcanonical provider requestのSHA-256。初回claim時に固定しretryで不変。';

CREATE INDEX idx_macro_pair_rotation_outbox_status_lease
  ON scouter.macro_pair_rotation_email_outbox (status, lease_expires_at)
  WHERE status = 'sending';

-- ============================================================================
-- 3. Trigger-owned outbox creation + immutable/destructive-operation guards
-- ============================================================================

CREATE OR REPLACE FUNCTION scouter.create_macro_pair_rotation_outbox()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  INSERT INTO scouter.macro_pair_rotation_email_outbox (
    report_period_date,
    idempotency_key
  )
  VALUES (
    NEW.report_period_date,
    format('macro-pair-rotation:%s:%s', NEW.report_period_date, NEW.strategy_version)
  );
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_macro_pair_rotation_create_outbox
  AFTER INSERT ON scouter.macro_pair_rotation_reports
  FOR EACH ROW EXECUTE FUNCTION scouter.create_macro_pair_rotation_outbox();

CREATE OR REPLACE FUNCTION scouter.reject_macro_pair_rotation_report_mutation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  RAISE EXCEPTION 'macro_pair_rotation_reports is immutable (% is forbidden)', TG_OP
    USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER trg_macro_pair_rotation_report_no_update_delete
  BEFORE UPDATE OR DELETE ON scouter.macro_pair_rotation_reports
  FOR EACH ROW EXECUTE FUNCTION scouter.reject_macro_pair_rotation_report_mutation();
CREATE TRIGGER trg_macro_pair_rotation_report_no_truncate
  BEFORE TRUNCATE ON scouter.macro_pair_rotation_reports
  FOR EACH STATEMENT EXECUTE FUNCTION scouter.reject_macro_pair_rotation_report_mutation();

CREATE OR REPLACE FUNCTION scouter.reject_macro_pair_rotation_outbox_delete()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  RAISE EXCEPTION 'macro_pair_rotation_email_outbox cannot be %d', lower(TG_OP)
    USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER trg_macro_pair_rotation_outbox_no_delete
  BEFORE DELETE ON scouter.macro_pair_rotation_email_outbox
  FOR EACH ROW EXECUTE FUNCTION scouter.reject_macro_pair_rotation_outbox_delete();
CREATE TRIGGER trg_macro_pair_rotation_outbox_no_truncate
  BEFORE TRUNCATE ON scouter.macro_pair_rotation_email_outbox
  FOR EACH STATEMENT EXECUTE FUNCTION scouter.reject_macro_pair_rotation_outbox_delete();

-- Trigger entry points are never callable as ordinary functions.
REVOKE ALL ON FUNCTION scouter.create_macro_pair_rotation_outbox() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION scouter.reject_macro_pair_rotation_report_mutation() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION scouter.reject_macro_pair_rotation_outbox_delete() FROM PUBLIC, anon, authenticated, service_role;

-- ============================================================================
-- 4. RLS + least privilege
-- ============================================================================

ALTER TABLE scouter.macro_pair_rotation_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.macro_pair_rotation_reports FORCE ROW LEVEL SECURITY;
ALTER TABLE scouter.macro_pair_rotation_email_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.macro_pair_rotation_email_outbox FORCE ROW LEVEL SECURITY;

CREATE POLICY "service_role_select" ON scouter.macro_pair_rotation_reports
  FOR SELECT TO service_role USING (true);
CREATE POLICY "service_role_insert" ON scouter.macro_pair_rotation_reports
  FOR INSERT TO service_role WITH CHECK (true);
CREATE POLICY "service_role_select" ON scouter.macro_pair_rotation_email_outbox
  FOR SELECT TO service_role USING (true);

-- 00016のdefault privilegesを含め一度全剥奪し、必要な直接操作だけを戻す。
REVOKE ALL ON TABLE scouter.macro_pair_rotation_reports
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE scouter.macro_pair_rotation_email_outbox
  FROM PUBLIC, anon, authenticated, service_role;
GRANT SELECT, INSERT ON TABLE scouter.macro_pair_rotation_reports TO service_role;
GRANT SELECT ON TABLE scouter.macro_pair_rotation_email_outbox TO service_role;

-- ============================================================================
-- 5. latest-stored macro snapshot (4 series x current/prior = 8 rows)
--    One SQL statement = one MVCC snapshot. Missing points remain explicit NULL rows.
-- ============================================================================

CREATE OR REPLACE FUNCTION scouter.get_macro_pair_rotation_macro_snapshot(
  p_current_date DATE,
  p_prior_date DATE
)
RETURNS TABLE (
  series_id TEXT,
  point_kind TEXT,
  target_date DATE,
  indicator_date DATE,
  value NUMERIC,
  released_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  observed_at TIMESTAMPTZ,
  information_cutoff_at TIMESTAMPTZ
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = ''
AS $$
  WITH params AS (
    SELECT statement_timestamp() AS cutoff
    WHERE p_current_date IS NOT NULL
      AND p_prior_date IS NOT NULL
      AND p_prior_date < p_current_date
  ),
  series(series_id, series_order) AS (
    VALUES
      ('mof_jgb_20y'::text, 1),
      ('DCOILWTICO'::text, 2),
      ('DEXJPUS'::text, 3),
      ('VIXCLS'::text, 4)
  ),
  points(point_kind, target_date, point_order) AS (
    VALUES
      ('current'::text, p_current_date, 1),
      ('prior'::text, p_prior_date, 2)
  )
  SELECT
    s.series_id,
    p.point_kind,
    p.target_date,
    m.indicator_date,
    m.value,
    m.released_at,
    m.updated_at,
    x.cutoff AS observed_at,
    x.cutoff AS information_cutoff_at
  FROM params x
  CROSS JOIN series s
  CROSS JOIN points p
  LEFT JOIN LATERAL (
    SELECT
      mid.indicator_date,
      mid.value,
      mid.released_at,
      mid.updated_at
    FROM jquants_core.macro_indicator_daily mid
    WHERE mid.series_id = s.series_id
      AND mid.indicator_date <= p.target_date
      AND mid.value IS NOT NULL
    ORDER BY mid.indicator_date DESC
    LIMIT 1
  ) m ON true
  ORDER BY s.series_order, p.point_order
$$;

-- ============================================================================
-- 6. Outbox state transition RPCs
-- ============================================================================

CREATE OR REPLACE FUNCTION scouter.claim_macro_pair_rotation_email(
  p_report_period_date DATE,
  p_payload_hash TEXT,
  p_recipient TEXT,
  p_subject TEXT,
  p_render_version TEXT,
  p_lease_seconds INTEGER DEFAULT 900
)
RETURNS TABLE (
  report_period_date DATE,
  attempt_id UUID,
  lease_expires_at TIMESTAMPTZ,
  idempotency_key TEXT,
  payload_hash TEXT,
  recipient TEXT,
  subject TEXT,
  render_version TEXT,
  report JSONB
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_now TIMESTAMPTZ := clock_timestamp();
  v_attempt_id UUID := gen_random_uuid();
  v_claimed BOOLEAN;
BEGIN
  IF p_report_period_date IS NULL THEN
    RAISE EXCEPTION 'report_period_date is required';
  END IF;
  IF p_payload_hash IS NULL OR p_payload_hash !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'payload_hash must be lowercase SHA-256';
  END IF;
  IF p_recipient IS NULL OR char_length(btrim(p_recipient)) NOT BETWEEN 3 AND 320 THEN
    RAISE EXCEPTION 'recipient is required';
  END IF;
  IF p_subject IS NULL OR char_length(btrim(p_subject)) NOT BETWEEN 1 AND 500 THEN
    RAISE EXCEPTION 'subject is required';
  END IF;
  IF p_render_version IS NULL OR char_length(btrim(p_render_version)) NOT BETWEEN 1 AND 100 THEN
    RAISE EXCEPTION 'render_version is required';
  END IF;
  IF p_lease_seconds IS NULL OR p_lease_seconds < 60 OR p_lease_seconds > 3600 THEN
    RAISE EXCEPTION 'lease_seconds must be between 60 and 3600';
  END IF;

  UPDATE scouter.macro_pair_rotation_email_outbox o
  SET
    status = 'sending',
    attempt_id = v_attempt_id,
    attempt_count = o.attempt_count + 1,
    lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
    payload_hash = p_payload_hash,
    recipient = btrim(p_recipient),
    subject = p_subject,
    render_version = btrim(p_render_version),
    last_error = NULL,
    updated_at = v_now
  WHERE o.report_period_date = p_report_period_date
    AND o.status = 'pending'
  RETURNING true INTO v_claimed;

  IF NOT coalesce(v_claimed, false) THEN
    RETURN;
  END IF;

  RETURN QUERY
  SELECT
    o.report_period_date,
    o.attempt_id,
    o.lease_expires_at,
    o.idempotency_key,
    o.payload_hash,
    o.recipient,
    o.subject,
    o.render_version,
    to_jsonb(r)
  FROM scouter.macro_pair_rotation_email_outbox o
  JOIN scouter.macro_pair_rotation_reports r USING (report_period_date)
  WHERE o.report_period_date = p_report_period_date
    AND o.status = 'sending'
    AND o.attempt_id = v_attempt_id;
END;
$$;

CREATE OR REPLACE FUNCTION scouter.finalize_macro_pair_rotation_email(
  p_report_period_date DATE,
  p_attempt_id UUID,
  p_outcome TEXT,
  p_provider_message_id TEXT DEFAULT NULL,
  p_error_message TEXT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_updated BOOLEAN;
BEGIN
  IF p_outcome NOT IN ('sent','failed','unknown') THEN
    RAISE EXCEPTION 'invalid email outcome: %', p_outcome;
  END IF;
  IF p_outcome = 'sent'
     AND (p_provider_message_id IS NULL OR btrim(p_provider_message_id) = '') THEN
    RAISE EXCEPTION 'provider_message_id is required for sent';
  END IF;
  IF p_outcome IN ('failed','unknown')
     AND (p_error_message IS NULL OR btrim(p_error_message) = '') THEN
    RAISE EXCEPTION 'error_message is required for %', p_outcome;
  END IF;

  UPDATE scouter.macro_pair_rotation_email_outbox o
  SET
    status = p_outcome,
    lease_expires_at = NULL,
    provider_message_id = CASE WHEN p_outcome = 'sent' THEN btrim(p_provider_message_id) ELSE NULL END,
    sent_at = CASE WHEN p_outcome = 'sent' THEN clock_timestamp() ELSE NULL END,
    last_error = CASE
      WHEN p_outcome IN ('failed','unknown') THEN left(btrim(p_error_message), 10000)
      ELSE NULL
    END,
    updated_at = clock_timestamp()
  WHERE o.report_period_date = p_report_period_date
    AND o.status = 'sending'
    AND o.attempt_id = p_attempt_id
  RETURNING true INTO v_updated;

  RETURN coalesce(v_updated, false);
END;
$$;

CREATE OR REPLACE FUNCTION scouter.retry_macro_pair_rotation_email(
  p_report_period_date DATE,
  p_payload_hash TEXT,
  p_recipient TEXT,
  p_subject TEXT,
  p_render_version TEXT,
  p_lease_seconds INTEGER DEFAULT 900
)
RETURNS TABLE (
  report_period_date DATE,
  attempt_id UUID,
  lease_expires_at TIMESTAMPTZ,
  idempotency_key TEXT,
  payload_hash TEXT,
  recipient TEXT,
  subject TEXT,
  render_version TEXT,
  report JSONB
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_now TIMESTAMPTZ := clock_timestamp();
  v_attempt_id UUID := gen_random_uuid();
  v_row scouter.macro_pair_rotation_email_outbox%ROWTYPE;
BEGIN
  IF p_lease_seconds IS NULL OR p_lease_seconds < 60 OR p_lease_seconds > 3600 THEN
    RAISE EXCEPTION 'lease_seconds must be between 60 and 3600';
  END IF;

  SELECT o.* INTO v_row
  FROM scouter.macro_pair_rotation_email_outbox o
  WHERE o.report_period_date = p_report_period_date
  FOR UPDATE;

  IF NOT FOUND OR v_row.status <> 'failed' THEN
    RETURN;
  END IF;
  IF v_row.created_at < v_now - interval '24 hours' THEN
    RAISE EXCEPTION 'failed email is outside the 24 hour manual retry window';
  END IF;
  IF p_payload_hash IS DISTINCT FROM v_row.payload_hash
     OR btrim(p_recipient) IS DISTINCT FROM v_row.recipient
     OR p_subject IS DISTINCT FROM v_row.subject
     OR btrim(p_render_version) IS DISTINCT FROM v_row.render_version THEN
    RAISE EXCEPTION 'retry envelope differs from the frozen first-attempt envelope';
  END IF;

  UPDATE scouter.macro_pair_rotation_email_outbox o
  SET
    status = 'sending',
    attempt_id = v_attempt_id,
    attempt_count = o.attempt_count + 1,
    lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
    last_error = NULL,
    updated_at = v_now
  WHERE o.report_period_date = p_report_period_date
    AND o.status = 'failed';

  RETURN QUERY
  SELECT
    o.report_period_date,
    o.attempt_id,
    o.lease_expires_at,
    o.idempotency_key,
    o.payload_hash,
    o.recipient,
    o.subject,
    o.render_version,
    to_jsonb(r)
  FROM scouter.macro_pair_rotation_email_outbox o
  JOIN scouter.macro_pair_rotation_reports r USING (report_period_date)
  WHERE o.report_period_date = p_report_period_date
    AND o.status = 'sending'
    AND o.attempt_id = v_attempt_id;
END;
$$;

CREATE OR REPLACE FUNCTION scouter.reconcile_macro_pair_rotation_email(
  p_report_period_date DATE,
  p_provider_message_id TEXT,
  p_evidence TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_updated BOOLEAN;
BEGIN
  IF p_provider_message_id IS NULL OR char_length(btrim(p_provider_message_id)) NOT BETWEEN 1 AND 500 THEN
    RAISE EXCEPTION 'provider_message_id is required';
  END IF;
  IF p_evidence IS NULL OR char_length(btrim(p_evidence)) NOT BETWEEN 1 AND 2000 THEN
    RAISE EXCEPTION 'reconciliation evidence is required';
  END IF;

  UPDATE scouter.macro_pair_rotation_email_outbox o
  SET
    status = 'sent',
    provider_message_id = btrim(p_provider_message_id),
    sent_at = clock_timestamp(),
    last_error = NULL,
    reconciled_at = clock_timestamp(),
    reconciliation_note = btrim(p_evidence),
    updated_at = clock_timestamp()
  WHERE o.report_period_date = p_report_period_date
    AND o.status = 'unknown'
  RETURNING true INTO v_updated;

  RETURN coalesce(v_updated, false);
END;
$$;

CREATE OR REPLACE FUNCTION scouter.recover_expired_macro_pair_rotation_emails()
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_count INTEGER;
BEGIN
  WITH expired AS (
    SELECT o.report_period_date, o.attempt_id, o.lease_expires_at
    FROM scouter.macro_pair_rotation_email_outbox o
    WHERE o.status = 'sending'
      AND o.lease_expires_at < clock_timestamp()
    FOR UPDATE SKIP LOCKED
  ),
  isolated AS (
    UPDATE scouter.macro_pair_rotation_email_outbox o
    SET
      status = 'unknown',
      lease_expires_at = NULL,
      last_error = 'sending lease expired before a provider outcome was durably recorded; automatic retry is forbidden',
      updated_at = clock_timestamp()
    FROM expired e
    WHERE o.report_period_date = e.report_period_date
      AND o.status = 'sending'
      AND o.attempt_id = e.attempt_id
      AND o.lease_expires_at = e.lease_expires_at
    RETURNING 1
  )
  SELECT count(*)::integer INTO v_count FROM isolated;

  RETURN v_count;
END;
$$;

-- Exact signatures: revoke implicit PUBLIC execute and expose only application RPCs.
REVOKE ALL ON FUNCTION scouter.get_macro_pair_rotation_macro_snapshot(date, date)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION scouter.claim_macro_pair_rotation_email(date, text, text, text, text, integer)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION scouter.finalize_macro_pair_rotation_email(date, uuid, text, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION scouter.retry_macro_pair_rotation_email(date, text, text, text, text, integer)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION scouter.reconcile_macro_pair_rotation_email(date, text, text)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION scouter.recover_expired_macro_pair_rotation_emails()
  FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION scouter.get_macro_pair_rotation_macro_snapshot(date, date) TO service_role;
GRANT EXECUTE ON FUNCTION scouter.claim_macro_pair_rotation_email(date, text, text, text, text, integer) TO service_role;
GRANT EXECUTE ON FUNCTION scouter.finalize_macro_pair_rotation_email(date, uuid, text, text, text) TO service_role;
GRANT EXECUTE ON FUNCTION scouter.retry_macro_pair_rotation_email(date, text, text, text, text, integer) TO service_role;
GRANT EXECUTE ON FUNCTION scouter.reconcile_macro_pair_rotation_email(date, text, text) TO service_role;
GRANT EXECUTE ON FUNCTION scouter.recover_expired_macro_pair_rotation_emails() TO service_role;

-- ============================================================================
-- 7. Job name allow-list
-- ============================================================================

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
    'scouter-thesis-lens',
    'scouter-overheat',
    'scouter-macro-pair-rotation-report'
  ));

-- ============================================================================
-- 8. Fail-closed manifest + dispatch cron (disabled canary)
-- ============================================================================

DO $$
DECLARE
  w ops.expected_workflows%ROWTYPE;
BEGIN
  SELECT * INTO w
  FROM ops.expected_workflows e
  WHERE e.workflow_file = 'macro-pair-rotation-report.yml';

  IF FOUND THEN
    IF w.repo <> 'JapanStockScouter'
       OR w.ref <> 'main'
       OR w.friendly_name <> 'Weekly Macro Pair Rotation Report'
       OR w.schedule_utc <> '30 1 * * 6'
       OR w.kind <> 'weekly'
       OR w.deadline_jst IS NOT NULL
       OR w.job_name <> 'scouter-macro-pair-rotation-report'
       OR w.enabled <> false THEN
      RAISE EXCEPTION 'macro-pair-rotation-report.yml manifest exists with a different contract';
    END IF;
  ELSE
    INSERT INTO ops.expected_workflows (
      workflow_file, repo, friendly_name, ref, schedule_utc,
      kind, deadline_jst, job_name, enabled, notes
    ) VALUES (
      'macro-pair-rotation-report.yml',
      'JapanStockScouter',
      'Weekly Macro Pair Rotation Report',
      'main',
      '30 1 * * 6',
      'weekly',
      NULL,
      'scouter-macro-pair-rotation-report',
      false,
      'メール有・毎週土曜10:30 JST。pg_cronのみ。dry-run/manual canary後にenabled=trueへ切替'
    );
  END IF;
END;
$$;

DO $$
DECLARE
  v_schedule TEXT;
  v_command TEXT;
BEGIN
  SELECT j.schedule, j.command INTO v_schedule, v_command
  FROM cron.job j
  WHERE j.jobname = 'dispatch-macro-pair-rotation-report';

  IF FOUND AND (
    v_schedule <> '30 1 * * 6'
    OR btrim(v_command) <> 'SELECT ops.dispatch_by_name(''macro-pair-rotation-report.yml'')'
  ) THEN
    RAISE EXCEPTION 'dispatch-macro-pair-rotation-report cron exists with a different contract';
  END IF;
END;
$$;

SELECT cron.schedule(
  'dispatch-macro-pair-rotation-report',
  '30 1 * * 6',
  $$ SELECT ops.dispatch_by_name('macro-pair-rotation-report.yml') $$
);

-- ============================================================================
-- 9. Dedicated weekly freshness (Saturday 11:15 JST)
-- ============================================================================

CREATE OR REPLACE FUNCTION ops.check_macro_pair_rotation_weekly_freshness()
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  w ops.expected_workflows%ROWTYPE;
  v_now TIMESTAMPTZ := clock_timestamp();
  v_now_jst TIMESTAMP;
  v_period DATE;
  v_expected_as_of DATE;
  v_ok BOOLEAN;
BEGIN
  SELECT * INTO w
  FROM ops.expected_workflows e
  WHERE e.workflow_file = 'macro-pair-rotation-report.yml';

  IF NOT FOUND
     OR w.repo <> 'JapanStockScouter'
     OR w.ref <> 'main'
     OR w.friendly_name <> 'Weekly Macro Pair Rotation Report'
     OR w.schedule_utc <> '30 1 * * 6'
     OR w.kind <> 'weekly'
     OR w.deadline_jst IS NOT NULL
     OR w.job_name <> 'scouter-macro-pair-rotation-report' THEN
    RAISE EXCEPTION 'macro pair rotation weekly freshness manifest contract mismatch';
  END IF;

  -- Safe rollout: disabled manifest means dispatch and freshness are both no-op.
  IF NOT w.enabled THEN
    RETURN;
  END IF;

  -- Also fail closed if the enabled manifest no longer owns the expected dispatch cron.
  IF NOT EXISTS (
    SELECT 1
    FROM cron.job j
    WHERE j.jobname = 'dispatch-macro-pair-rotation-report'
      AND j.schedule = '30 1 * * 6'
      AND btrim(j.command) = 'SELECT ops.dispatch_by_name(''macro-pair-rotation-report.yml'')'
  ) THEN
    RAISE EXCEPTION 'macro pair rotation dispatch cron contract mismatch';
  END IF;

  v_now_jst := timezone('Asia/Tokyo', v_now);
  IF extract(isodow FROM v_now_jst) <> 6 OR v_now_jst::time < time '11:15' THEN
    RETURN;
  END IF;
  v_period := v_now_jst::date;

  -- A crashed/timed-out sender must become unknown before success evaluation.
  PERFORM scouter.recover_expired_macro_pair_rotation_emails();

  SELECT max(c.calendar_date) INTO v_expected_as_of
  FROM jquants_core.trading_calendar c
  WHERE c.is_business_day = true
    AND c.calendar_date <= v_period;

  -- 成功条件は「当週のreportが保存され、そのoutboxがsent」であること。
  -- job_runs.status='success' を OR で代替しない: sendClaimedEmailは競合送信者を検出した場合
  -- （outboxがsendingのまま送信者が落ちた等）にもjobをsuccessで終えるため、job_runsだけを
  -- 根拠にすると未送信を「未達なし」と誤判定する。job_runsは通知本文の診断情報に留める。
  SELECT EXISTS (
    SELECT 1
    FROM scouter.macro_pair_rotation_reports r
    JOIN scouter.macro_pair_rotation_email_outbox o USING (report_period_date)
    WHERE r.report_period_date = v_period
      AND r.as_of_date = v_expected_as_of
      AND o.status = 'sent'
  )
  INTO v_ok;

  IF coalesce(v_ok, false) THEN
    RETURN;
  END IF;

  IF EXISTS (
    SELECT 1 FROM ops.freshness_alert_log f
    WHERE f.workflow_file = 'macro-pair-rotation-report.yml'
      AND f.check_date = v_period
  ) THEN
    RETURN;
  END IF;

  IF ops.notify(
       '[JapanStock] 週次マクロ対立軸レポート未達',
       format(
         '<p>当週のマクロ対立軸レポートが未達です。</p><ul><li>report_period_date: %s</li><li>expected_as_of_date: %s</li></ul><p>job_runs / outbox unknown・failed / workflow dispatchを確認してください。</p>',
         v_period,
         coalesce(v_expected_as_of::text, 'trading_calendar missing')
       )
     ) IS NOT NULL THEN
    INSERT INTO ops.freshness_alert_log (workflow_file, check_date)
    VALUES ('macro-pair-rotation-report.yml', v_period)
    ON CONFLICT DO NOTHING;
  END IF;
END;
$$;

REVOKE ALL ON FUNCTION ops.check_macro_pair_rotation_weekly_freshness()
  FROM PUBLIC, anon, authenticated, service_role;

DO $$
DECLARE
  v_schedule TEXT;
  v_command TEXT;
BEGIN
  SELECT j.schedule, j.command INTO v_schedule, v_command
  FROM cron.job j
  WHERE j.jobname = 'ops-macro-pair-rotation-freshness';

  IF FOUND AND (
    v_schedule <> '15 2 * * 6'
    OR btrim(v_command) <> 'SELECT ops.check_macro_pair_rotation_weekly_freshness()'
  ) THEN
    RAISE EXCEPTION 'ops-macro-pair-rotation-freshness cron exists with a different contract';
  END IF;
END;
$$;

SELECT cron.schedule(
  'ops-macro-pair-rotation-freshness',
  '15 2 * * 6',
  $$ SELECT ops.check_macro_pair_rotation_weekly_freshness() $$
);
