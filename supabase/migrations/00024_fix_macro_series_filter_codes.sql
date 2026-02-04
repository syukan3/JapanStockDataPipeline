-- macro_series_metadata のフィルタ条件をコード値に修正
-- e-Stat API のサーバーサイドフィルタ用にコード値を使用する

-- estat_ci_leading: tab=100(CI指数), cat01=100(先行指数)
UPDATE jquants_core.macro_series_metadata
SET
  source_filter = '{"tab": "100", "cat01": "100"}'::jsonb,
  last_fetched_at = NULL,
  last_value_date = NULL
WHERE series_id = 'estat_ci_leading';

-- estat_core_cpi: tab=1(指数), cat01=0161(生鮮食品を除く総合), area=00000(全国)
UPDATE jquants_core.macro_series_metadata
SET
  source_filter = '{"tab": "1", "cat01": "0161", "area": "00000"}'::jsonb,
  last_fetched_at = NULL,
  last_value_date = NULL
WHERE series_id = 'estat_core_cpi';

-- 既存のe-Statデータを削除（新しいフィルタで再取得するため）
DELETE FROM jquants_core.macro_indicator_daily
WHERE series_id IN ('estat_ci_leading', 'estat_core_cpi');
