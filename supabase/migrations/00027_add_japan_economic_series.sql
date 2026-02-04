-- 日本経済指標を追加（FRED経由）
-- 雇用、生産、消費、金融、貿易

INSERT INTO jquants_core.macro_series_metadata (
  series_id, source, source_series_id, source_filter,
  category, region, name_en, name_ja, frequency
) VALUES
  -- 雇用
  ('LRUN64TTJPM156S', 'fred', 'LRUN64TTJPM156S', NULL,
   'business_cycle', 'jp', 'Japan Unemployment Rate (15-64)', '日本失業率（15-64歳）', 'monthly'),
  -- 生産
  ('JPNPROINDMISMEI', 'fred', 'JPNPROINDMISMEI', NULL,
   'business_cycle', 'jp', 'Japan Industrial Production', '日本鉱工業生産指数', 'monthly'),
  -- 消費
  ('CSCICP02JPM460S', 'fred', 'CSCICP02JPM460S', NULL,
   'business_cycle', 'jp', 'Japan Consumer Confidence', '日本消費者信頼感指数', 'monthly'),
  -- 金融
  ('MYAGM2JPM189S', 'fred', 'MYAGM2JPM189S', NULL,
   'financial', 'jp', 'Japan M2 Money Supply', '日本M2マネーストック', 'monthly'),
  -- 貿易
  ('XTNTVA01JPM664S', 'fred', 'XTNTVA01JPM664S', NULL,
   'business_cycle', 'jp', 'Japan Trade Balance', '日本貿易収支', 'monthly')
ON CONFLICT (series_id) DO NOTHING;
