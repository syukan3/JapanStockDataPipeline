-- 収集レーティング履歴の保存を、attempt-fencedなRPC 1本へ寄せる。
-- 計画書: docs/PLANS-analyst-target-monitor-2026-07.md（ルートリポ）§11
--
-- 00113 は素のテーブルだけを作ったため、Scouterが直接upsertしていた。それだと
--   (1) 保持期間: 一度入った行が3ヶ月を過ぎても消えず、画面に古い値が残り続ける
--   (2) fence: staleとして再claimされた旧attemptの収集結果が履歴へ書き込める
--       （本機能の他のDB副作用はすべて run_id + attempt_id でfenceしている）
-- の2点を満たせない。upsertとpruneを同一トランザクションで、job fenceの内側で行う。

CREATE OR REPLACE FUNCTION scouter.record_analyst_rating_history(
  p_run_id         uuid,
  p_attempt_id     uuid,
  p_scan_date      date,
  -- 今回ページを取得できた銘柄。pruneはこの銘柄だけに効かせ、取得できなかった
  -- 銘柄の既存履歴を巻き込んで消さない。
  p_local_codes    text[],
  p_rows           jsonb,
  p_retention_days integer
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_now    timestamptz := clock_timestamp();
  v_cutoff date;
BEGIN
  IF (SELECT auth.role()) IS DISTINCT FROM 'service_role' THEN
    RAISE EXCEPTION 'service_role required' USING ERRCODE = '42501';
  END IF;
  IF p_run_id IS NULL OR p_attempt_id IS NULL OR p_scan_date IS NULL THEN
    RAISE EXCEPTION 'run_id, attempt_id and scan_date are required';
  END IF;
  IF jsonb_typeof(p_rows) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'rows must be a JSON array';
  END IF;
  IF p_retention_days IS NULL OR p_retention_days <= 0 OR p_retention_days > 3650 THEN
    RAISE EXCEPTION 'retention_days must be between 1 and 3650';
  END IF;

  -- complete_job_run_attemptと同じjob fence。旧attemptは副作用なしでfalseを返す。
  PERFORM 1
  FROM jquants_ingest.job_runs jr
  WHERE jr.run_id = p_run_id
    AND jr.job_name = 'scouter-analyst-target-monitor'
    AND jr.target_date = p_scan_date
    AND jr.status = 'running'
    AND jr.attempt_id = p_attempt_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN false;
  END IF;

  v_cutoff := p_scan_date - p_retention_days;

  INSERT INTO scouter.analyst_rating_history AS h (
    event_fingerprint,
    local_code,
    firm_key,
    firm_name,
    published_on,
    rating,
    previous_target_price,
    target_price,
    source_url,
    source_domain,
    first_seen_at,
    last_seen_at
  )
  SELECT
    r.value ->> 'event_fingerprint',
    r.value ->> 'local_code',
    lower(btrim(r.value ->> 'firm_key')),
    btrim(r.value ->> 'firm_name'),
    (r.value ->> 'published_on')::date,
    nullif(btrim(r.value ->> 'rating'), ''),
    (r.value ->> 'previous_target_price')::numeric,
    (r.value ->> 'target_price')::numeric,
    btrim(r.value ->> 'source_url'),
    lower(btrim(r.value ->> 'source_domain')),
    v_now,
    v_now
  FROM jsonb_array_elements(p_rows) AS r(value)
  -- 保持期間外・未来日の行は入口で拒否する（収集側のバグでも古い値を残さない）
  WHERE (r.value ->> 'published_on')::date >= v_cutoff
    AND (r.value ->> 'published_on')::date <= p_scan_date
  ON CONFLICT (event_fingerprint) DO UPDATE
  SET
    -- 出典の表記が変わっても事実側を最新に保つ。first_seen_at は据え置く。
    firm_name             = EXCLUDED.firm_name,
    rating                = EXCLUDED.rating,
    previous_target_price = EXCLUDED.previous_target_price,
    source_url            = EXCLUDED.source_url,
    source_domain         = EXCLUDED.source_domain,
    last_seen_at          = EXCLUDED.last_seen_at;

  -- 保持期間を過ぎた行を落とす。画面側のフィルタに頼らず、DBの内容自体を
  -- 「直近 p_retention_days 日」に保つ。
  IF p_local_codes IS NOT NULL AND array_length(p_local_codes, 1) > 0 THEN
    DELETE FROM scouter.analyst_rating_history h
    WHERE h.local_code = ANY (p_local_codes)
      AND h.published_on < v_cutoff;
  END IF;

  RETURN true;
END;
$$;

REVOKE ALL ON FUNCTION scouter.record_analyst_rating_history(
  uuid, uuid, date, text[], jsonb, integer
) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION scouter.record_analyst_rating_history(
  uuid, uuid, date, text[], jsonb, integer
) TO service_role;

COMMENT ON FUNCTION scouter.record_analyst_rating_history(
  uuid, uuid, date, text[], jsonb, integer
) IS
  '収集レーティング履歴のupsertと保持期間pruneを、job attempt fenceの内側で原子的に行う。旧attemptはfalseを返し副作用を持たない';
