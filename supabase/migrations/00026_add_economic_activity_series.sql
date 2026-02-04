-- 経済活動指標を追加
-- 雇用、消費、生産、住宅関連

INSERT INTO jquants_core.macro_series_metadata (
  series_id, source, source_series_id, source_filter,
  category, region, name_en, name_ja, frequency
) VALUES
  -- 雇用（週次先行指標）
  ('ICSA', 'fred', 'ICSA', NULL,
   'business_cycle', 'us', 'Initial Unemployment Claims', '新規失業保険申請件数', 'weekly'),
  -- 消費
  ('UMCSENT', 'fred', 'UMCSENT', NULL,
   'business_cycle', 'us', 'Consumer Sentiment (U of Michigan)', 'ミシガン大消費者信頼感指数', 'monthly'),
  ('RSXFS', 'fred', 'RSXFS', NULL,
   'business_cycle', 'us', 'Retail Sales (Advance)', '小売売上高（速報）', 'monthly'),
  -- 生産
  ('INDPRO', 'fred', 'INDPRO', NULL,
   'business_cycle', 'us', 'Industrial Production Index', '鉱工業生産指数', 'monthly'),
  -- 住宅
  ('HOUST', 'fred', 'HOUST', NULL,
   'business_cycle', 'us', 'Housing Starts', '住宅着工件数', 'monthly')
ON CONFLICT (series_id) DO NOTHING;
