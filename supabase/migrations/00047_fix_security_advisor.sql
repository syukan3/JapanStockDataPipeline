-- 00047_fix_security_advisor.sql
-- Supabase Security Advisor で検出された問題を修正
--
-- ERROR: scouter.hd_ai_meta_evaluations, scouter.macro_ai_meta_evaluations で RLS 未有効
-- WARN:  jquants_core.get_latest_macro_indicators, get_indicator_history で search_path 未設定
-- INFO:  jquants_ingest.job_* テーブルで RLS 有効だがポリシーなし

-- ============================================
-- 1. ERROR: scouter テーブルに RLS + ポリシー追加
-- ============================================

ALTER TABLE scouter.macro_ai_meta_evaluations ENABLE ROW LEVEL SECURITY;

CREATE POLICY "authenticated_select"
  ON scouter.macro_ai_meta_evaluations
  FOR SELECT TO authenticated USING (TRUE);

CREATE POLICY "service_role_all"
  ON scouter.macro_ai_meta_evaluations
  FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);

ALTER TABLE scouter.hd_ai_meta_evaluations ENABLE ROW LEVEL SECURITY;

CREATE POLICY "authenticated_select"
  ON scouter.hd_ai_meta_evaluations
  FOR SELECT TO authenticated USING (TRUE);

CREATE POLICY "service_role_all"
  ON scouter.hd_ai_meta_evaluations
  FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);

-- ============================================
-- 2. WARN: 関数に search_path を設定
-- ============================================

CREATE OR REPLACE FUNCTION jquants_core.get_latest_macro_indicators(
  p_series_ids TEXT[],
  p_eval_date DATE
)
RETURNS TABLE (
  series_id TEXT,
  indicator_date DATE,
  value NUMERIC
)
LANGUAGE sql STABLE
SET search_path = ''
AS $$
  SELECT DISTINCT ON (mid.series_id)
    mid.series_id,
    mid.indicator_date,
    mid.value
  FROM jquants_core.macro_indicator_daily mid
  WHERE mid.series_id = ANY(p_series_ids)
    AND mid.released_at <= (p_eval_date || 'T23:59:59+09:00')::timestamptz
    AND mid.indicator_date <= p_eval_date
  ORDER BY mid.series_id, mid.indicator_date DESC, mid.released_at DESC
$$;

CREATE OR REPLACE FUNCTION jquants_core.get_indicator_history(
  p_series_ids TEXT[],
  p_eval_date DATE,
  p_limit INT DEFAULT 30
)
RETURNS TABLE (
  series_id TEXT,
  indicator_date DATE,
  value NUMERIC
)
LANGUAGE sql STABLE
SET search_path = ''
AS $$
  SELECT h.series_id, h.indicator_date, h.value
  FROM unnest(p_series_ids) AS s(sid)
  CROSS JOIN LATERAL (
    SELECT mid.series_id, mid.indicator_date, mid.value
    FROM jquants_core.macro_indicator_daily mid
    WHERE mid.series_id = s.sid
      AND mid.indicator_date <= p_eval_date
    ORDER BY mid.indicator_date DESC
    LIMIT p_limit
  ) h
  ORDER BY h.series_id, h.indicator_date ASC;
$$;

-- ============================================
-- 3. INFO: jquants_ingest job テーブルに service_role ポリシー追加
--    RLS有効 + ポリシーなし → service_role 明示ポリシーで Advisor 警告解消
-- ============================================

CREATE POLICY "service_role_all"
  ON jquants_ingest.job_heartbeat
  FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY "service_role_all"
  ON jquants_ingest.job_locks
  FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY "service_role_all"
  ON jquants_ingest.job_run_items
  FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY "service_role_all"
  ON jquants_ingest.job_runs
  FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);
