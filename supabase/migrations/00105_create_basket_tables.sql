-- 00105_create_basket_tables.sql
-- テーマバスケット割安判定（第1号: 200A / 日経半導体株指数）の3テーブル。
-- 計画書: docs/PLANS-basket-valuation-2026-07.md（ルートリポ）
--
-- 設計要点:
--   - basket_definitions: バスケット定義（1行/バスケット）
--   - basket_constituents: 構成銘柄+ウエート係数（valid_from/to で年次入替を履歴管理）
--   - basket_metrics: 日次集計（1行/日/バスケット。加重バリュエーション+模擬指数+ETF実値）
--   - スコア（5軸合成）はDBに持たず Portfolio 側で basket_metrics 系列から都度計算する
--     （パーセンタイルは参照時点の全履歴に対して定義されるため事前計算しない）。
--
-- 書き込み経路:
--   - 初回バックフィル+構成銘柄投入: scripts/seed/basket-valuation.ts（service_role）
--   - 日次更新: scripts/cron/refresh-basket-metrics.ts（cron-a 後段ステップ、service_role）
--
-- 容量: metrics は 1行/営業日/バスケット（10年でも ~2,500行/バスケット、軽微）。
--
-- 注意: analytics は Exposed schema のため GRANT/RLS を明示する（00068/00104 と同方針）。

-- ============================================================
-- basket_definitions
-- ============================================================
create table if not exists analytics.basket_definitions (
  basket_id       text primary key,
  display_name    text not null,
  -- ETF実価格の突合先（jquants_core.equity_bar_daily.local_code、例 '200A0'）。NULL可
  benchmark_code  text,
  description     text,
  -- ウエート係数のアンカー日（直近定期見直し日）。basket_constituents.weight_factor の基準
  anchor_date     date,
  -- アンカー日の公式指数値（模擬指数の基準化に使用）。入手不可なら NULL（ETF基準価額で代替）
  anchor_index_level numeric(12,4),
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

comment on table analytics.basket_definitions is
  'テーマバスケット定義（例: 日経半導体株指数/200A）。構成は basket_constituents、日次集計は basket_metrics。';
comment on column analytics.basket_definitions.anchor_date is
  'ウエート係数アンカー日（直近定期見直し日）。weight_factor = 公式ウエート ÷ 同日時価総額シェア。';

-- ============================================================
-- basket_constituents
-- ============================================================
create table if not exists analytics.basket_constituents (
  basket_id        text not null references analytics.basket_definitions (basket_id),
  local_code       text not null,   -- 5桁 J-Quants Code
  -- 公式ウエート ÷ アンカー日時価総額シェア。日次ウエートは
  -- w_i(t) = weight_factor_i × mcap_i(t) / Σ_j (weight_factor_j × mcap_j(t))
  weight_factor    numeric(16,8) not null,
  -- アンカー日時点の公式（または機械キャップ近似）ウエート%（検証・表示用）
  official_weight  numeric(6,3),
  -- 半導体主業か（false = 定期見直し時5%キャップ枠。記録用）
  is_semicon_main  boolean not null default true,
  valid_from       date not null,
  valid_to         date,            -- NULL = 現行。年次入替・上場廃止で閉じる
  created_at       timestamptz not null default now(),

  primary key (basket_id, local_code, valid_from)
);

comment on table analytics.basket_constituents is
  'バスケット構成銘柄とウエート係数。年次入替は行を閉じて（valid_to）新行を追加する。';

create index if not exists idx_basket_constituents_current
  on analytics.basket_constituents (basket_id) where valid_to is null;

-- ============================================================
-- basket_metrics
-- ============================================================
create table if not exists analytics.basket_metrics (
  basket_id            text not null references analytics.basket_definitions (basket_id),
  as_of_date           date not null,
  -- 模擬指数（anchor_date = anchor_index_level で基準化した加重リターン連結）
  index_level          numeric(14,4),
  -- ベンチマークETFの調整後終値（J-Quants adj_close。分割調整後・通常の分配は未調整）
  etf_close            numeric(14,4),
  -- 加重バリュエーション（ウエートは当日 w_i(t)。分子分母とも算出可能な銘柄のみで再正規化）
  weighted_per         numeric(10,2),   -- 実績PER（時価総額合計 ÷ 純利益合計の調和的集計）
  weighted_per_forward numeric(10,2),   -- フォワードPER（会社予想EPSベース）
  weighted_pbr         numeric(10,2),
  weighted_psr         numeric(10,2),
  weighted_div_yield   numeric(7,3),    -- %
  -- サイクル位置チャート用: index_level ÷ weighted_per（模擬指数の一株利益水準）
  weighted_eps_level   numeric(14,4),
  -- 当日メトリクス算出に使えた構成銘柄のウエート合計%（欠損検知。100が理想）
  coverage_pct         numeric(5,1),
  updated_at           timestamptz not null default now(),

  primary key (basket_id, as_of_date)
);

comment on table analytics.basket_metrics is
  'バスケット日次集計。模擬指数レベル+加重バリュエーション+ベンチマークETF実値。1行/営業日/バスケット。';
comment on column analytics.basket_metrics.weighted_per is
  '加重PER = Σ(w_i × mcap_i) / Σ(w_i × earnings_i) 方式（調和集計）。赤字銘柄は分母に含める。';
comment on column analytics.basket_metrics.coverage_pct is
  '当日バリュエーション算出に採用できた構成銘柄のウエート合計%。90未満は品質警告対象。';

create index if not exists idx_basket_metrics_date
  on analytics.basket_metrics (basket_id, as_of_date desc);

-- ============================================================
-- RLS / GRANT（00068/00104 と同方針: authenticated SELECT + service_role ALL）
-- ============================================================
alter table analytics.basket_definitions  enable row level security;
alter table analytics.basket_constituents enable row level security;
alter table analytics.basket_metrics      enable row level security;

drop policy if exists "authenticated_select" on analytics.basket_definitions;
create policy "authenticated_select"
  on analytics.basket_definitions for select to authenticated using (true);
drop policy if exists "service_role_all" on analytics.basket_definitions;
create policy "service_role_all"
  on analytics.basket_definitions for all to service_role using (true) with check (true);

drop policy if exists "authenticated_select" on analytics.basket_constituents;
create policy "authenticated_select"
  on analytics.basket_constituents for select to authenticated using (true);
drop policy if exists "service_role_all" on analytics.basket_constituents;
create policy "service_role_all"
  on analytics.basket_constituents for all to service_role using (true) with check (true);

drop policy if exists "authenticated_select" on analytics.basket_metrics;
create policy "authenticated_select"
  on analytics.basket_metrics for select to authenticated using (true);
drop policy if exists "service_role_all" on analytics.basket_metrics;
create policy "service_role_all"
  on analytics.basket_metrics for all to service_role using (true) with check (true);

grant select on analytics.basket_definitions,  analytics.basket_constituents, analytics.basket_metrics to authenticated;
grant all    on analytics.basket_definitions,  analytics.basket_constituents, analytics.basket_metrics to service_role;
