-- 00104_create_short_selling_sector.sql
-- 業種別空売り比率（J-Quants Standard /v2/markets/short-ratio）の日次生値。
-- 33業種 × 各日の売り注文代金内訳（実注文 / 空売り規制あり / 空売り規制なし）[円]を保持する。
--
-- 用途: analytics.market_indicators の空売り比率2成分（short_selling_ratio_restricted/
--       unrestricted）を全業種合算で算出する公式ソース（環境変数 SHORT_RATIO_SOURCE=jquants
--       で有効化）。集計は scripts/cron/refresh-market-indicators.ts の fillShortSellingOfficial。
--
-- 書き込み経路: src/lib/jquants/endpoints/short-ratio.ts syncShortRatio（service_role バッチ・
--   ON CONFLICT (as_of_date, sector33_code) で冪等 upsert）。
--
-- 容量: 33業種 × 約245営業日/年 × 10年 ≈ 8.1万行（軽微）。
--
-- 注意: analytics は既に Exposed schema（market_indicators 等が稼働中）のため公開設定の
--       変更は不要。default privileges に依存せず GRANT/RLS を明示する（00068 と同方針）。

create table if not exists analytics.short_selling_sector (
  as_of_date                       date not null,
  sector33_code                    text not null,
  -- 実注文の売り代金[円]（価格規制の分母に含める）
  selling_ex_short_value           numeric(16,0),
  -- 空売り（価格規制あり）代金[円]
  short_with_restrictions_value    numeric(16,0),
  -- 空売り（価格規制なし）代金[円]
  short_without_restrictions_value numeric(16,0),
  updated_at                       timestamptz not null default now(),

  primary key (as_of_date, sector33_code)
);

comment on table analytics.short_selling_sector is
  '業種別空売り比率の日次生値（J-Quants /v2/markets/short-ratio）。33業種の売り注文代金内訳[円]。市場全体の空売り比率2成分は全業種合算で算出する。';
comment on column analytics.short_selling_sector.selling_ex_short_value is
  '実注文の売り代金[円]（SellExShortVa）。空売り比率の分母 = selling_ex_short + with_restrictions + without_restrictions。';
comment on column analytics.short_selling_sector.short_with_restrictions_value is
  '空売り（価格規制あり）代金[円]（ShrtWithResVa）。restricted% = Σこの列 / Σ分母 × 100。';
comment on column analytics.short_selling_sector.short_without_restrictions_value is
  '空売り（価格規制なし）代金[円]（ShrtNoResVa）。unrestricted% = Σこの列 / Σ分母 × 100。';

create index if not exists idx_short_selling_sector_date
  on analytics.short_selling_sector (as_of_date desc);

alter table analytics.short_selling_sector enable row level security;

-- 冪等化: CREATE POLICY に IF NOT EXISTS が無いため作り直す（00068と同方針）
drop policy if exists "authenticated_select" on analytics.short_selling_sector;
create policy "authenticated_select"
  on analytics.short_selling_sector for select to authenticated using (true);
drop policy if exists "service_role_all" on analytics.short_selling_sector;
create policy "service_role_all"
  on analytics.short_selling_sector for all to service_role using (true) with check (true);

grant select on analytics.short_selling_sector to authenticated;
grant all on analytics.short_selling_sector to service_role;
