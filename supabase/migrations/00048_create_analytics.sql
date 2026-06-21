-- 00048_create_analytics.sql
-- 派生メトリクス用スキーマ: analytics
-- 全銘柄の日次スナップショット（バリュエーション/収益性/成長/モメンタム）＋
-- セクター内パーセンタイル・ファクタースコアを格納。
-- 入力は jquants_core（生データ）。Portfolio/Scouter 等が authenticated で参照する。
--
-- 注意: Supabase の Exposed schemas に analytics を追加する必要あり
-- （Management API の db_schema、または Dashboard > Settings > API）。

create schema if not exists analytics;
comment on schema analytics is 'jquants_core から日次計算する派生メトリクス（指標・偏差値・ファクタースコア）';

-- 最小権限: anon には付与しない。authenticated は SELECT、service_role は ALL。
grant usage on schema analytics to authenticated, service_role;
alter default privileges in schema analytics grant select on tables to authenticated;
alter default privileges in schema analytics grant all on tables to service_role;
alter default privileges in schema analytics grant all on sequences to service_role;

-- ============================================================
-- stock_metrics: 銘柄ごとの日次メトリクス（最新通期実績＋最新株価ベース）
-- ============================================================
create table if not exists analytics.stock_metrics (
  as_of_date        date not null,        -- 採用した株価日
  local_code        text not null,
  sector17_code     text,
  sector17_name     text,
  fiscal_year_end   date,                 -- 採用した通期実績の決算期
  close             numeric(14,2),
  market_cap        numeric(20,2),        -- 時価総額(円)

  -- バリュエーション
  per               numeric(12,2),
  pbr               numeric(12,2),
  psr               numeric(12,2),
  dividend_yield    numeric(8,3),

  -- 収益性
  roe               numeric(8,2),
  roa               numeric(8,2),
  operating_margin  numeric(8,2),
  net_margin        numeric(8,2),

  -- 成長性（前年比 %）
  sales_yoy         numeric(12,2),
  op_yoy            numeric(12,2),
  np_yoy            numeric(12,2),

  -- 健全性・質
  equity_ratio      numeric(8,2),
  accrual_quality   numeric(8,2),         -- 営業CF / 純利益

  -- モメンタム（騰落率 %）
  ret_1m            numeric(12,2),
  ret_3m            numeric(12,2),
  ret_6m            numeric(12,2),
  ret_1y            numeric(12,2),

  -- セクター内パーセンタイル（0-100, 高いほど"良い"向きに正規化）
  value_pct         numeric(6,2),         -- 割安度（PER/PBR/PSR低いほど高い）
  quality_pct       numeric(6,2),         -- 質（ROE/利益率/自己資本比率高いほど高い）
  momentum_pct      numeric(6,2),         -- モメンタム（3/6ヶ月リターン高いほど高い）

  -- ファクター合成スコア（セクター内 z-score 平均）
  value_score       numeric(8,3),
  quality_score     numeric(8,3),
  momentum_score    numeric(8,3),
  total_score       numeric(8,3),

  sector_count      int,                  -- 同セクターの母数
  updated_at        timestamptz not null default now(),

  primary key (as_of_date, local_code)
);

comment on table analytics.stock_metrics is '銘柄別の日次派生メトリクス＋セクター内偏差値/ファクタースコア';

create index if not exists idx_stock_metrics_code
  on analytics.stock_metrics (local_code, as_of_date desc);
create index if not exists idx_stock_metrics_date
  on analytics.stock_metrics (as_of_date);
create index if not exists idx_stock_metrics_sector
  on analytics.stock_metrics (as_of_date, sector17_code);

-- refresh_stock_metrics() 関数は 00049 で追加（実データで検証後に確定）。
