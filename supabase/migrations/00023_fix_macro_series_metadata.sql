-- macro_series_metadata の修正
-- 1. NAPM: FREDから2016年に削除されたため削除
-- 2. estat_ci_leading: 統計表IDとフィルタ条件を修正
-- 3. estat_core_cpi: 統計表IDとフィルタ条件を修正（2020年基準に変更）

-- NAPM を削除（FREDから提供終了）
DELETE FROM jquants_core.macro_series_metadata
WHERE series_id = 'NAPM';

-- 関連するデータも削除
DELETE FROM jquants_core.macro_indicator_daily
WHERE series_id = 'NAPM';

-- estat_ci_leading を修正
UPDATE jquants_core.macro_series_metadata
SET
  source_series_id = '0003446461',
  source_filter = '{"tab": "CI指数", "cat01": "先行指数"}'::jsonb,
  last_fetched_at = NULL,
  last_value_date = NULL
WHERE series_id = 'estat_ci_leading';

-- estat_core_cpi を修正（2020年基準に変更）
UPDATE jquants_core.macro_series_metadata
SET
  source_series_id = '0003427113',
  source_filter = '{"tab": "指数", "cat01": "0161 生鮮食品を除く総合", "area": "全国"}'::jsonb,
  last_fetched_at = NULL,
  last_value_date = NULL
WHERE series_id = 'estat_core_cpi';
