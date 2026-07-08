-- マクロ指標拡張: 米長期金利(DGS10/DGS30)生値 + JGB超長期利回り(20y/30y)
-- 背景: 米30年債利回り急騰の分析で、既存のT10Y2Y(10y-2yスプレッド)だけでは
-- 超長期の財政・タームプレミアム要因を捉えられないことが判明したため追加。

-- source CHECK制約を拡張（新規ソース 'mof' = 財務省国債金利情報 を許容）
ALTER TABLE jquants_core.macro_indicator_daily
  DROP CONSTRAINT IF EXISTS macro_indicator_daily_source_check;
ALTER TABLE jquants_core.macro_indicator_daily
  ADD CONSTRAINT macro_indicator_daily_source_check
  CHECK (source IN ('fred', 'estat', 'mof'));

ALTER TABLE jquants_core.macro_series_metadata
  DROP CONSTRAINT IF EXISTS macro_series_metadata_source_check;
ALTER TABLE jquants_core.macro_series_metadata
  ADD CONSTRAINT macro_series_metadata_source_check
  CHECK (source IN ('fred', 'estat', 'mof'));

-- 新規系列の登録
INSERT INTO jquants_core.macro_series_metadata (
  series_id, source, source_series_id, source_filter,
  category, region, name_en, name_ja, frequency
) VALUES
  -- 米長期金利（生値。既存T10Y2Yはスプレッドのみで両端が同時に動くケースを見落とすため追加）
  ('DGS10', 'fred', 'DGS10', NULL,
   'interest_rate', 'us', '10-Year Treasury Yield', '米10年債利回り', 'daily'),
  ('DGS30', 'fred', 'DGS30', NULL,
   'interest_rate', 'us', '30-Year Treasury Yield', '米30年債利回り', 'daily'),
  -- JGB超長期（財務省「国債金利情報」日次CSVより。生保のJGB離れ・BOJ QTで
  -- 超長期JGBがボラの震源になっており、既存のIRLTLT01JPM156N(10年・月次)では捉えられない）
  ('mof_jgb_20y', 'mof', 'jgbcm_20y', NULL,
   'interest_rate', 'jp', 'JGB 20Y Yield', 'JGB20年利回り', 'daily'),
  ('mof_jgb_30y', 'mof', 'jgbcm_30y', NULL,
   'interest_rate', 'jp', 'JGB 30Y Yield', 'JGB30年利回り', 'daily')
ON CONFLICT (series_id) DO NOTHING;
