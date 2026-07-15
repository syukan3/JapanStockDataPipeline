-- 00096_rename_short_selling_to_nikkei_vi.sql
-- P0修正: analytics.market_indicators.short_selling_ratio 列の実体は日経VI（誤ラベル）。
-- あわせて本物の空売り比率を正しい成分列から復活させる。
--
-- (1) 誤ラベル根治: nikkei225jp.com daily2 col[11] は空売り比率ではなく、日経平均
--     ボラティリティー・インデックス（日経VI）の終値である。史実値で確認済み:
--       - 2024-08-05 = 70.69（令和のブラックマンデー・リーマン以来の最高。日経公式/報道と一致）
--       - 2020-03-16 = 60.67（コロナショック）
--       - 2026-07-15 ≈ 32.2（日経公式終値と一致・前日35.39からの日次変化も一致）
--     空売り比率は常時35〜45%で暴落日に70へ跳ねることはない。値はVIとして正しく蓄積済みの
--     ため列名のみ改める（データ温存・型/インデックス/RLSは維持される）。
--
-- (2) 本物の空売り比率を復活: daily2 の col[22]=空売り比率(価格規制あり)[%]・
--     col[24]=空売り比率(価格規制なし)[%] を新2列に保存する。内部整合
--     （col[22]=col[21]/(col[19]+col[21]+col[23]) 等）と JPX/東証 公表の空売り比率
--     （2026-07-15=39.30% / 07-14=34.50% / 07-13=38.90% / 07-06=36.30%）に col[22]+col[24] が
--     全て一致することを確認済み。合計は Portfolio 側で restricted+unrestricted として算出する
--     （DBには成分のみ保存）。
--
-- ⚠️ 適用前提（3リポ同期）: 旧 short_selling_ratio 列は Portfolio（/market・アドバイザー）と
--   Scouter（macro-ai / macro-regime）が参照している。リネームは PostgREST の select を
--   破壊するため、DataPipeline（本ブランチ）・Portfolio（本ブランチ）・Scouter（Issue D=追随担当）の
--   追随コードが全てマージされてから本番適用すること。適用後にシード/バックフィルで
--   nikkei_vi と空売り比率2成分を daily2.json 履歴から埋め直す。

alter table analytics.market_indicators
  rename column short_selling_ratio to nikkei_vi;

comment on column analytics.market_indicators.nikkei_vi is
  '日経平均ボラティリティー・インデックス（日経VI）終値[ポイント]。nikkei225jp.com daily2 col[11]。旧名 short_selling_ratio は誤ラベルで、実体は当初から日経VI（空売り比率ではない）。';

alter table analytics.market_indicators
  add column if not exists short_selling_ratio_restricted   numeric(6,2),
  add column if not exists short_selling_ratio_unrestricted numeric(6,2);

comment on column analytics.market_indicators.short_selling_ratio_restricted is
  '空売り比率（価格規制あり）[%]。nikkei225jp.com daily2 col[22]=col[21]/(col[19]+col[21]+col[23])。東証全体の売り注文代金に占める価格規制ありの空売りの割合。';
comment on column analytics.market_indicators.short_selling_ratio_unrestricted is
  '空売り比率（価格規制なし）[%]。nikkei225jp.com daily2 col[24]=col[23]/(col[19]+col[21]+col[23])。空売り比率合計は restricted+unrestricted（Portfolio側で算出）。';

comment on table analytics.market_indicators is
  '市場全体指標の日次時系列（日経平均PER/EPS・騰落レシオ・新高値新安値・売買代金・日経VI・空売り比率(規制あり/なし)・信用評価損益率・NT倍率）。ソース別バッチが担当カラムのみをupsertする。';
