-- as-of クエリ最適化用インデックス
-- パターン: WHERE series_id = $1 AND released_at <= $2 ORDER BY indicator_date DESC LIMIT 1
CREATE INDEX CONCURRENTLY idx_macro_indicator_asof
  ON jquants_core.macro_indicator_daily (series_id, released_at DESC, indicator_date DESC);
