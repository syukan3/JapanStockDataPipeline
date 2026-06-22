-- 00051_create_stock_screen_view.sql
-- スクリーナー用の結合ビュー: ファンダ(stock_metrics) × テクニカル(technical_metrics)。
-- Portfolio のスクリーナーが単一読み取りでフィルタ/ソート/limit をDB側に委譲できるようにする。
--
-- 原子的公開(部分スナップショットを見せない):
--   technical_metrics への一括 upsert はバッチ分割されるため、ビューが max(as_of_date) を
--   見ると新規日付の書き込み途中（数秒）に部分公開ウィンドウが生じる。これを避けるため
--   「公開済み日付マーカー」technical_publication を導入し、ビューはマーカーが指す日付だけを見る。
--   バッチは全件 upsert 成功後にマーカーを単一行 UPDATE で原子的に切り替える。
--
-- 設計:
--   - 各テーブルの「最新/公開日」を local_code で LEFT JOIN（as_of_date のズレ耐性）。
--     technical 側が continue-on-error で遅れても fundamental 側は欠落しない。
--   - technical 未投入（マーカー NULL）でも technical 列が NULL になるだけで fundamental は出る。
--   - security_invoker=on: 呼び出しロール(authenticated)の権限で評価（security-definer-view 警告回避）。
--   - 公開マーカー analytics.technical_publication は 00050 で作成済み。

create or replace view analytics.stock_screen
with (security_invoker = on) as
select
  m.as_of_date,
  m.local_code,
  m.sector17_code,
  m.sector17_name,
  m.market_cap,
  m.per,
  m.pbr,
  m.dividend_yield,
  m.roe,
  m.value_pct,
  m.quality_pct,
  m.momentum_pct,
  m.total_score,
  m.ret_1y,
  -- テクニカル（公開済みスナップショットのみ）
  t.rsi_14,
  t.dev_25,
  t.dev_75,
  t.dev_200,
  t.above_sma200,
  t.cross_25_75,
  t.cross_25_75_age,
  t.macd_hist,
  t.bb_percent_b,
  t.stoch_k,
  t.atr_pct,
  t.vol_ratio_20,
  t.ichimoku_state
from analytics.stock_metrics m
left join analytics.technical_metrics t
  on t.local_code = m.local_code
 and t.as_of_date = (select published_as_of_date from analytics.technical_publication where id)
where m.as_of_date = (select max(as_of_date) from analytics.stock_metrics);

comment on view analytics.stock_screen is
  'スクリーナー用: stock_metrics(最新) × technical_metrics(公開済み日) を local_code で LEFT JOIN';

grant select on analytics.stock_screen to authenticated;
