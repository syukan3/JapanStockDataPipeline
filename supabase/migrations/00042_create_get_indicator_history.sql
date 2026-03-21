-- 指標の時系列履歴を効率的に取得するRPC関数
-- LATERAL join で各 series_id ごとに最大 p_limit 件を返す
-- 既存インデックス idx_macro_indicator_series (series_id, indicator_date DESC) を活用

CREATE OR REPLACE FUNCTION jquants_core.get_indicator_history(
  p_series_ids TEXT[],
  p_eval_date DATE,
  p_limit INT DEFAULT 30
)
RETURNS TABLE (
  series_id TEXT,
  indicator_date DATE,
  value NUMERIC
) AS $$
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
$$ LANGUAGE sql STABLE;

-- service_role に EXECUTE 権限を付与
GRANT EXECUTE ON FUNCTION jquants_core.get_indicator_history(TEXT[], DATE, INT)
  TO service_role;
