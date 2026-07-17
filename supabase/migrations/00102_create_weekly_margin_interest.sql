-- 00102_create_weekly_margin_interest.sql
-- 銘柄別の信用取引週末残高（J-Quants Standard /v2/markets/margin-interest・週次）。
--
-- 書き込み経路: scripts/cron/cron-f-direct.ts（週次・冪等upsert・直近35日ウィンドウ再取得）
--               scripts/seed/weekly-margin-interest.ts（契約後バックフィル）
-- 保持ポリシー: 全銘柄は直近1年のみ。保有+ウォッチ銘柄（保護リスト）は全期間保持
--               （pruneWeeklyMarginInterest が application_date < now() - 1年 を削除）。
-- 貸借倍率は保存しない（表示側で long_total / short_total を計算）。
--
-- 注意: J-Quants Standard 未契約の間はテーブルは空のまま（作成のみで無害）。
--       申込日（application_date）ベース。営業日2日以下の週はデータ無し（欠落週が正常）。

create table if not exists jquants_core.weekly_margin_interest (
  local_code           text not null,
  application_date     date not null,

  -- 残高（株数。null許容: 信用/貸借区分により片側のみの銘柄がある）
  short_total          numeric(16,0),
  long_total           numeric(16,0),
  short_negotiable     numeric(16,0),   -- 一般信用 売り
  long_negotiable      numeric(16,0),   -- 一般信用 買い
  short_standardized   numeric(16,0),   -- 制度信用 売り
  long_standardized    numeric(16,0),   -- 制度信用 買い

  issue_type           smallint,        -- 1=信用銘柄, 2=貸借銘柄, 3=その他

  updated_at           timestamptz not null default now(),

  primary key (local_code, application_date)
);

comment on table jquants_core.weekly_margin_interest is
  '銘柄別信用取引週末残高（J-Quants Standard・週次・申込日ベース）。全銘柄1年保持+保有/ウォッチ銘柄は全期間保持。';
comment on column jquants_core.weekly_margin_interest.issue_type is
  '銘柄区分（1=信用銘柄, 2=貸借銘柄, 3=その他）';

create index if not exists idx_weekly_margin_interest_date
  on jquants_core.weekly_margin_interest (application_date);

alter table jquants_core.weekly_margin_interest enable row level security;

-- 冪等化: CREATE POLICY に IF NOT EXISTS が無いため作り直す（00068と同方針）
drop policy if exists "authenticated_select" on jquants_core.weekly_margin_interest;
create policy "authenticated_select"
  on jquants_core.weekly_margin_interest for select to authenticated using (true);
drop policy if exists "service_role_all" on jquants_core.weekly_margin_interest;
create policy "service_role_all"
  on jquants_core.weekly_margin_interest for all to service_role using (true) with check (true);

grant select on jquants_core.weekly_margin_interest to authenticated;
grant all on jquants_core.weekly_margin_interest to service_role;
