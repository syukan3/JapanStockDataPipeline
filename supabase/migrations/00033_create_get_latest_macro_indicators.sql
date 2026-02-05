-- N+1クエリ問題解消のためのRPC関数
-- 複数のseries_idに対して、as-ofルールで最新の1行ずつを返す

CREATE OR REPLACE FUNCTION jquants_core.get_latest_macro_indicators(
  p_series_ids TEXT[],
  p_eval_date DATE
)
RETURNS TABLE (
  series_id TEXT,
  indicator_date DATE,
  value NUMERIC
) AS $$
  SELECT DISTINCT ON (mid.series_id)
    mid.series_id,
    mid.indicator_date,
    mid.value
  FROM jquants_core.macro_indicator_daily mid
  WHERE mid.series_id = ANY(p_series_ids)
    AND mid.released_at <= (p_eval_date || 'T23:59:59+09:00')::timestamptz
    AND mid.indicator_date <= p_eval_date
  ORDER BY mid.series_id, mid.indicator_date DESC, mid.released_at DESC
$$ LANGUAGE sql STABLE;

COMMENT ON FUNCTION jquants_core.get_latest_macro_indicators IS
  'As-of整合で各series_idの最新指標値を取得（DISTINCT ONで1行ずつ）';
