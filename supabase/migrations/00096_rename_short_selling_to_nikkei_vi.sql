-- 00096_rename_short_selling_to_nikkei_vi.sql
-- P0修正: analytics.market_indicators.short_selling_ratio 列の実体は日経VI（誤ラベル）。
--
-- nikkei225jp.com daily2 col[11] は空売り比率ではなく、日経平均ボラティリティー・インデックス
-- （日経VI）の終値である。史実値で確認済み:
--   - 2024-08-05 = 70.69（令和のブラックマンデー・リーマン以来の最高。日経公式/報道と一致）
--   - 2020-03-16 = 60.67（コロナショック）
--   - 2026-07-15 ≈ 32.2（日経公式終値と一致・前日35.39からの日次変化も一致）
-- 空売り比率は常時35〜50%で、暴落日に70へ跳ねることはない。値はVIとして正しく蓄積済みのため
-- 列名のみ改める（データ温存・型/インデックス/RLSは維持される）。
--
-- 空売り比率そのものは信頼できる無料の機械可読ソースが無いためいったん廃止する
-- （再追加は J-Quants Standard ¥3,300/月 の課金判断とセット）。
--
-- ⚠️ 適用前提: この列は Portfolio（/market・アドバイザー）と Scouter（macro-ai / macro-regime）が
--   参照している。列名リネームは PostgREST の select を破壊するため、両リポの追随コードが
--   デプロイ/マージされてから本番適用すること。

alter table analytics.market_indicators
  rename column short_selling_ratio to nikkei_vi;

comment on column analytics.market_indicators.nikkei_vi is
  '日経平均ボラティリティー・インデックス（日経VI）終値[ポイント]。nikkei225jp.com daily2 col[11]。旧名 short_selling_ratio は誤ラベルで、実体は当初から日経VI（空売り比率ではない）。';

comment on table analytics.market_indicators is
  '市場全体指標の日次時系列（日経平均PER/EPS・騰落レシオ・新高値新安値・売買代金・日経VI・信用評価損益率・NT倍率）。ソース別バッチが担当カラムのみをupsertする。';
