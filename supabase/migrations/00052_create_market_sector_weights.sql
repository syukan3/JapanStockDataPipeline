-- 00052_create_market_sector_weights.sql
-- 市場全体(全上場銘柄)の17業種別 時価総額ウェイト。
-- Portfolio の「セクター別配分」カードで、現保有構成と市場(TOPIX近似)の差
-- （オーバー/アンダーウェイト）を示すための参照ビュー。
--
-- 設計:
--   - analytics.stock_metrics の最新 as_of_date を集計（screener ビュー 00051 と同様に
--     max(as_of_date) を採用。fundamental は publication マーカー不要）。
--   - 事前に業種(~18)へ集約して返すため、PostgREST の行上限(既定1000)に当たらず、
--     クライアント側で全銘柄を取得して集計する必要がない。
--   - security_invoker=on: 呼び出しロールの権限で評価（security-definer-view 警告回避・00047 準拠）。
--   - 時価総額加重は浮動株調整なしの単純時価総額のため TOPIX の「近似」である点に留意。

create or replace view analytics.market_sector_weights
with (security_invoker = on) as
with latest as (
  select sector17_code, sector17_name, market_cap
  from analytics.stock_metrics
  where as_of_date = (select max(as_of_date) from analytics.stock_metrics)
    and market_cap is not null
    and sector17_name is not null
),
agg as (
  select sector17_code, sector17_name, sum(market_cap) as sector_cap
  from latest
  group by sector17_code, sector17_name
)
select
  sector17_code,
  sector17_name,
  sector_cap,
  round((sector_cap / nullif(sum(sector_cap) over (), 0) * 100)::numeric, 2) as weight_pct
from agg;

comment on view analytics.market_sector_weights is
  '全上場銘柄の17業種別 時価総額ウェイト(最新as_of_date)。市場(TOPIX近似)ベンチマーク。';

-- service_role は analytics の default privileges(00048)で ALL を保持。authenticated に SELECT を付与。
grant select on analytics.market_sector_weights to authenticated;
