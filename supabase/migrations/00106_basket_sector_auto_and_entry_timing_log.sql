-- 00106_basket_sector_auto_and_entry_timing_log.sql
-- エントリータイミング判定 + バスケット拡張（銀行業 1615）の共有DB契約。
-- 計画書: docs/PLANS-entry-timing-2026-07.md（ルートリポ）
--
-- 内容:
--   1) analytics.basket_definitions に「業種自動導出」モードを追加する列（既存200A行は無影響）
--   2) scouter.entry_timing_signal_log（エントリータイミング3レンズの通知重複排止ログ）
--
-- 設計要点:
--   - constituent_source='curated'（既定・200A方式）: basket_constituentsを手動管理、年次入替
--   - constituent_source='sector33_auto'（銀行業等）: equity_master.sector33_codeから
--     scripts/cron/refresh-sector-basket-constituents.ts が日次で構成銘柄を自動導出・差分更新。
--     weight_factor は一律1固定（TOPIX-33業種はキャップ無しの単純時価総額加重のため）。

-- ============================================================
-- 1) analytics.basket_definitions 拡張
-- ============================================================
alter table analytics.basket_definitions
  add column if not exists constituent_source text not null default 'curated'
    check (constituent_source in ('curated', 'sector33_auto')),
  add column if not exists sector33_filter text;

comment on column analytics.basket_definitions.constituent_source is
  'curated=手動キュレーション(200A方式・年次入替) / sector33_auto=equity_master.sector33_codeから自動導出(銀行業等)';
comment on column analytics.basket_definitions.sector33_filter is
  'constituent_source=sector33_auto の時のみ有効。equity_master.sector33_name と一致させるフィルタ値（例: 銀行業）';

-- ============================================================
-- 2) scouter.entry_timing_signal_log
-- ============================================================
create table if not exists scouter.entry_timing_signal_log (
  basket_id   text not null,
  lens        text not null check (lens in ('reversion', 'demand_supply', 'trend')),
  as_of_date  date not null,
  active      boolean not null,
  detail      text,
  notified_at timestamptz,
  created_at  timestamptz not null default now(),
  primary key (basket_id, lens, as_of_date)
);

comment on table scouter.entry_timing_signal_log is
  'エントリータイミング3レンズ(逆張り/需給反転型/順張り)の日次評価ログ。新規activeへの遷移検知と通知重複排止に使用。';
comment on column scouter.entry_timing_signal_log.lens is
  'reversion=逆張り(平均回帰) / demand_supply=需給反転型 / trend=順張り(トレンドフォロー)';

create index if not exists idx_entry_timing_signal_log_basket_date
  on scouter.entry_timing_signal_log (basket_id, as_of_date desc);

alter table scouter.entry_timing_signal_log enable row level security;

drop policy if exists "service_role_all" on scouter.entry_timing_signal_log;
create policy "service_role_all" on scouter.entry_timing_signal_log
  for all to service_role using (true) with check (true);

-- 00016 の ALTER DEFAULT PRIVILEGES により authenticated へ自動SELECT付与されるが、
-- 本テーブルは分析用の内部ログであり公開の必要がないため明示的に剥奪する
-- （earnings_alert_log と同方針）。
revoke all on scouter.entry_timing_signal_log from anon, authenticated;
