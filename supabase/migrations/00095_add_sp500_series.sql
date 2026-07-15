-- ベンチマーク拡張: S&P500系列の登録
-- 背景: ダッシュボードの資産推移チャートに日経平均に加えS&P500を比較ベンチマークとして
-- 追加するため。Cron D（マクロ）は macro_series_metadata を読んで対象系列を決める設計のため、
-- メタデータ1行のINSERTだけで次回実行時に自動収集される（初回730日バックフィル）。
-- 注意: FREDの SP500 系列は約10年分の日次のみ提供（S&P DJのライセンス制約）・配当なし価格指数。

INSERT INTO jquants_core.macro_series_metadata (
  series_id, source, source_series_id, source_filter,
  category, region, name_en, name_ja, frequency
) VALUES
  ('SP500', 'fred', 'SP500', NULL,
   'market', 'us', 'S&P 500 Index', 'S&P500種指数', 'daily')
ON CONFLICT (series_id) DO NOTHING;
