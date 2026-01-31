-- 00012_optimize_views_and_rpc.sql
-- v_data_freshness の count(*) を概算行数に置換 + distinct_dates RPC 関数追加

-- ============================================
-- 1. v_data_freshness: count(*) → pg_stat 概算行数
-- ============================================
CREATE OR REPLACE VIEW jquants_ingest.v_data_freshness AS
SELECT
  'equity_bar_daily' AS dataset,
  max(trade_date) AS latest_date,
  (SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'jquants_core' AND relname = 'equity_bar_daily') AS total_rows
FROM jquants_core.equity_bar_daily
UNION ALL
SELECT
  'trading_calendar',
  max(calendar_date),
  (SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'jquants_core' AND relname = 'trading_calendar')
FROM jquants_core.trading_calendar
UNION ALL
SELECT
  'topix_bar_daily',
  max(trade_date),
  (SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'jquants_core' AND relname = 'topix_bar_daily')
FROM jquants_core.topix_bar_daily
UNION ALL
SELECT
  'equity_master',
  max(valid_from),
  (SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'jquants_core' AND relname = 'equity_master')
FROM jquants_core.equity_master
WHERE is_current = true
UNION ALL
SELECT
  'financial_disclosure',
  max(disclosed_date),
  (SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'jquants_core' AND relname = 'financial_disclosure')
FROM jquants_core.financial_disclosure
UNION ALL
SELECT
  'earnings_calendar',
  max(announcement_date),
  (SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'jquants_core' AND relname = 'earnings_calendar')
FROM jquants_core.earnings_calendar
UNION ALL
SELECT
  'investor_type_trading',
  max(end_date),
  (SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'jquants_core' AND relname = 'investor_type_trading')
FROM jquants_core.investor_type_trading;

COMMENT ON VIEW jquants_ingest.v_data_freshness IS 'データセット別の鮮度確認 (最新日付と概算件数)';

-- ============================================
-- 2. distinct_dates RPC: DISTINCT で重複行転送を回避
-- ============================================
CREATE OR REPLACE FUNCTION jquants_core.distinct_dates(
  p_schema text,
  p_table text,
  p_date_column text,
  p_start text,
  p_end text
)
RETURNS TABLE(date_value text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
BEGIN
  -- テーブル名・カラム名をバリデーション（SQLインジェクション防止）
  IF p_schema NOT IN ('jquants_core', 'jquants_ingest') THEN
    RAISE EXCEPTION 'Invalid schema: %', p_schema;
  END IF;
  IF p_table !~ '^[a-z][a-z0-9_]*$' OR p_date_column !~ '^[a-z][a-z0-9_]*$' THEN
    RAISE EXCEPTION 'Invalid table or column name';
  END IF;

  RETURN QUERY EXECUTE format(
    'SELECT DISTINCT %I::text AS date_value FROM %I.%I WHERE %I >= $1 AND %I <= $2 ORDER BY date_value',
    p_date_column, p_schema, p_table, p_date_column, p_date_column
  ) USING p_start, p_end;
END;
$$;

-- service_role のみ実行可能
REVOKE ALL ON FUNCTION jquants_core.distinct_dates(text, text, text, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jquants_core.distinct_dates(text, text, text, text, text) TO service_role;
