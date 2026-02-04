-- コモディティ・為替指標を追加
-- WTI原油、銅価格、ドルインデックス

INSERT INTO jquants_core.macro_series_metadata (
  series_id, source, source_series_id, source_filter,
  category, region, name_en, name_ja, frequency
) VALUES
  -- コモディティ
  ('DCOILWTICO', 'fred', 'DCOILWTICO', NULL,
   'commodity', 'global', 'WTI Crude Oil Price', 'WTI原油価格', 'daily'),
  ('PCOPPUSDM', 'fred', 'PCOPPUSDM', NULL,
   'commodity', 'global', 'Global Copper Price', '銅価格（グローバル）', 'monthly'),
  -- 為替（ドル全体）
  ('DTWEXBGS', 'fred', 'DTWEXBGS', NULL,
   'fx', 'us', 'US Dollar Index (Broad)', 'ドルインデックス（広義）', 'daily')
ON CONFLICT (series_id) DO NOTHING;
