-- 00097_add_breadth_pct_above_sma.sql
-- 構成銘柄のSMA上回り比率(breadth %)。指数上昇の内部の広がりを検出する。
-- 計算はプライム(旧一部)ユニバース・調整後終値ベース(00093のadj再基準化と整合)。
-- 分母は「当日終値とSMA(25/200)が両方揃う銘柄数」のみ(上場直後銘柄等はSMAが計算できる
-- までその銘柄自体を分母から除外。ユニバース全銘柄が算出不能な日は0ではなくnull)。
-- 計算経路は src/lib/analytics/market-breadth.ts（既存breadth系と同一データ取得・ユニバース）。
-- 書き込み経路は既存の breadth グループ（advancers等）に相乗り（00068の担当カラム規約に従う）。

alter table analytics.market_indicators
  add column if not exists pct_above_sma25  numeric(5,1),
  add column if not exists pct_above_sma200 numeric(5,1);

comment on column analytics.market_indicators.pct_above_sma25 is
  'SMA25(25営業日移動平均)を上回るプライムユニバース銘柄の比率[%]。調整後終値ベース。分母=当日終値とSMA25が揃う銘柄数（算出不能な銘柄は分母除外。全銘柄不足日はnull）。';
comment on column analytics.market_indicators.pct_above_sma200 is
  'SMA200(200営業日移動平均)を上回るプライムユニバース銘柄の比率[%]。調整後終値ベース。分母規則はpct_above_sma25と同様（上場直後等の算出不能銘柄は分母除外。全銘柄不足日はnull）。';
