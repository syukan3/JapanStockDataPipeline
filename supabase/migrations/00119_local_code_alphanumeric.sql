-- 00119_local_code_alphanumeric.sql
-- 英字入りの銘柄コード（例: キオクシア 285A → local_code 285A0）を受け付けられるようにする。
--
-- 【原因】JPX は 2024年以降、証券コードの4桁目に英字を含む新形式を払い出している。
-- jquants_core 側は J-Quants の値をそのまま格納するため元から問題ないが、
-- scouter スキーマの2テーブル（と1関数）が銘柄コードを「数字のみ」で CHECK していたため、
-- 英字コード銘柄はレーティング履歴の投入も外部検索の予約もできなかった。
--
-- 【修正】数字限定の判定を 4桁目だけ英字を許す形へ緩める。
--   * 5文字固定: '^[0-9]{3}[0-9A-Z][0-9]$'
--   * 4/5文字:   '^[0-9]{3}[0-9A-Z][0-9]?$'
-- いずれも旧条件の上位集合なので、既存行は全て新条件を満たす（ADD CONSTRAINT の検証で落ちない）。
-- 2026-08-07 時点の現行上場 4,445 件のうち英字コードは 434 件で、4桁目以外に英字を持つ銘柄は 0 件
-- （jquants_core.equity_master で確認済み）。
-- Portfolio 側の domain portfolio.local_code_t は Portfolio リポの 00019 で同時に直す。

-- ---------------------------------------------------------------------------
-- 1) scouter.analyst_rating_history.local_code（5文字固定）
-- ---------------------------------------------------------------------------

-- 暗黙生成の制約名に依存せず、local_code に掛かる CHECK を落としてから明示名で貼り直す。
DO $do$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT c.conname
    FROM pg_catalog.pg_constraint c
    WHERE c.conrelid = 'scouter.analyst_rating_history'::pg_catalog.regclass
      AND c.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(c.oid) LIKE '%local_code%'
  LOOP
    EXECUTE pg_catalog.format(
      'ALTER TABLE scouter.analyst_rating_history DROP CONSTRAINT %I', r.conname
    );
  END LOOP;
END
$do$;

ALTER TABLE scouter.analyst_rating_history
  ADD CONSTRAINT analyst_rating_history_local_code_chk
  CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$');

-- ---------------------------------------------------------------------------
-- 2) scouter.external_search_requests.local_code（4文字/5文字）
-- ---------------------------------------------------------------------------

ALTER TABLE scouter.external_search_requests
  DROP CONSTRAINT IF EXISTS external_search_requests_local_code_chk;

ALTER TABLE scouter.external_search_requests
  ADD CONSTRAINT external_search_requests_local_code_chk
  CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]?$');

-- ---------------------------------------------------------------------------
-- 3) scouter.reserve_external_search_request の guard
--    （00086 から guard の正規表現とエラーメッセージ以外は一切変更していない）
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
  IF p_local_code IS NULL OR p_local_code !~ '^[0-9]{3}[0-9A-Z][0-9]?$' THEN
    RAISE EXCEPTION 'local_code must be a 4 or 5 character stock code';
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

REVOKE ALL ON FUNCTION scouter.reserve_external_search_request(text, date, text, uuid, uuid)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION scouter.reserve_external_search_request(text, date, text, uuid, uuid)
  TO service_role;

-- ---------------------------------------------------------------------------
-- 4) 適用検証
-- ---------------------------------------------------------------------------

DO $do$
DECLARE
  v_ok boolean;
BEGIN
  SELECT '285A0' ~ '^[0-9]{3}[0-9A-Z][0-9]$'
     AND '63150' ~ '^[0-9]{3}[0-9A-Z][0-9]$'
     AND '285A'  ~ '^[0-9]{3}[0-9A-Z][0-9]?$'
     AND NOT ('285a0' ~ '^[0-9]{3}[0-9A-Z][0-9]$')
    INTO v_ok;
  IF NOT v_ok THEN
    RAISE EXCEPTION 'local_code pattern sanity check failed';
  END IF;
END
$do$;
