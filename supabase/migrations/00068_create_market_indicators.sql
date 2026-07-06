-- 00068_create_market_indicators.sql
-- 市場全体指標の日次時系列（ワイド型・1日1行・全期間保持）。
-- 対象: 日経平均終値/PER/EPS、騰落数・騰落レシオ(25日)、新高値/新安値銘柄数、
--       プライム売買代金、空売り比率、信用評価損益率(週次)、TOPIX終値、NT倍率。
--
-- 書き込み経路（いずれも service_role のバッチ。担当カラムのみを同一shapeでupsertし、
-- 他ソースの担当カラムはpayloadに含めない = 暗黙NULL上書きしない）:
--   - scripts/cron/refresh-market-indicators.ts breadth系: equity_bar_daily から自前計算
--     (advancers/decliners/unchanged/adv_dec_ratio_25d/new_highs/new_lows/prime_turnover_value)
--   - 同 external系: Yahoo Finance(^N225) / nikkei225jp.com
--     (nikkei_close/nikkei_per/short_selling_ratio/margin_pl_ratio)
--   - 同 derived系: nikkei_eps(=close/per), topix_close(jquants_core.topix_bar_daily), nt_ratio
--   - scripts/seed/market-indicators.ts: 5年バックフィル（breadth は Scouter backtest.sqlite 由来）
--
-- 公開マーカーは使わない: 行は「部分的に埋まった状態」を許容する設計
-- （指標ごとに確定タイミングが異なる。NULL列は Portfolio 側が欠損として扱う）。
--
-- 注意: analytics は既に Exposed schema（stock_metrics/technical_metrics 稼働中）のため
--       公開設定の変更は不要。default privileges に依存せず GRANT/RLS を明示する。

create table if not exists analytics.market_indicators (
  as_of_date            date primary key,

  -- external: 日経平均（Yahoo Finance ^N225 / nikkei225jp.com daily2）
  nikkei_close          numeric(12,2),
  nikkei_per            numeric(8,2),
  -- external: 空売り比率[%]（nikkei225jp.com daily2 col[11]）
  short_selling_ratio   numeric(6,2),
  -- external: 信用評価損益率[%]（週次・二市場。値がある日=公表週末日のみ非NULL）
  margin_pl_ratio       numeric(6,2),

  -- breadth: プライム市場（2022-04-04再編前は東証一部）ユニバースの自前計算
  advancers             integer,
  decliners             integer,
  unchanged             integer,
  adv_dec_ratio_25d     numeric(8,2),   -- Σ値上がり銘柄数(25営業日) / Σ値下がり銘柄数(25営業日) * 100
  new_highs             integer,        -- 日経方式: 1-3月=前年来, 4月-=年初来（調整後高値ベース）
  new_lows              integer,
  prime_turnover_value  numeric(20,0),  -- 円

  -- derived
  nikkei_eps            numeric(10,2),  -- nikkei_close / nikkei_per（指数ベースEPS）
  topix_close           numeric(12,2),  -- jquants_core.topix_bar_daily.close の複製（表示用）
  nt_ratio              numeric(8,3),   -- nikkei_close / topix_close

  updated_at            timestamptz not null default now()
);

comment on table analytics.market_indicators is
  '市場全体指標の日次時系列（日経平均PER/EPS・騰落レシオ・新高値新安値・売買代金・空売り比率・信用評価損益率・NT倍率）。ソース別バッチが担当カラムのみをupsertする。';
comment on column analytics.market_indicators.margin_pl_ratio is
  '信用評価損益率[%]（二市場・週次公表）。公表対象の週末営業日のみ非NULL。';
comment on column analytics.market_indicators.new_highs is
  '新高値銘柄数（日経方式: 1-3月は前年来・4月以降は年初来。プライム＝旧一部ユニバース・調整後高値）';

create index if not exists idx_market_indicators_date
  on analytics.market_indicators (as_of_date desc);

alter table analytics.market_indicators enable row level security;

-- 冪等化: CREATE POLICY に IF NOT EXISTS が無いため作り直す（00050と同方針）
drop policy if exists "authenticated_select" on analytics.market_indicators;
create policy "authenticated_select"
  on analytics.market_indicators for select to authenticated using (true);
drop policy if exists "service_role_all" on analytics.market_indicators;
create policy "service_role_all"
  on analytics.market_indicators for all to service_role using (true) with check (true);

grant select on analytics.market_indicators to authenticated;
grant all on analytics.market_indicators to service_role;
