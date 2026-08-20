-- 00124_create_equity_bar_weekly.sql
-- 保有・ウォッチ銘柄限定の長期週足テーブル + 分割再基準化 RPC。
-- 設計正本: docs/PLANS-longterm-log-chart-2026-08.md（JapanStock ルートリポ）
--
-- 目的: J-Quants Standard の10年窓から追跡銘柄（保有∪ウォッチ）だけ週足を蓄積し、
-- /stocks/[code] の長期（5年/10年）片対数チャートを成立させる。ローリング窓で
-- API から消えた過去分も自前系列として保全する（アーカイブ対象外・恒久保持）。
--
-- 分割・併合の再基準化は2段構え（詳細は計画書 §4.3-3）:
--   - イベント日を含む週: raw 週足は分割前後の価格が混在するため、00093 で再基準化済みの
--     equity_bar_daily から再集計して行ごと置換する
--   - イベント週より前の週: 既存 adj_* に増分係数のみ適用（価格×factor / 出来高÷factor。
--     raw から再計算しない——過去のイベント週の raw は混在基準のままだから）
-- 適用済みイベントは台帳 equity_bar_weekly_rebase_events に記録し、7日検知窓の再検知による
-- 二重適用を防ぐ。適用＋台帳記録は本 RPC の1トランザクションで原子化する
-- （supabase-js はクライアント側トランザクション不可）。
-- 複数イベントは呼び出し側がイベント日の降順（新しい順）で1件ずつ RPC を呼ぶこと。

-- ============================================================
-- 1) 週足テーブル（追跡銘柄のみ格納）
-- ============================================================
create table if not exists analytics.equity_bar_weekly (
  local_code        text not null check (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$'),
  week_start        date not null check (extract(isodow from week_start) = 1),
  week_end          date not null,
  open              numeric,
  high              numeric,
  low               numeric,
  close             numeric,
  volume            numeric,
  turnover_value    numeric,
  adjustment_factor numeric not null default 1,
  adj_open          numeric,
  adj_high          numeric,
  adj_low           numeric,
  adj_close         numeric,
  adj_volume        numeric,
  ingested_at       timestamptz not null default now(),
  primary key (local_code, week_start)
);

comment on table analytics.equity_bar_weekly is
  '追跡銘柄（保有∪ウォッチ）限定の週足。ISO週（week_start=月曜）キー。全銘柄は格納しない。アーカイブ対象外・恒久保持';
comment on column analytics.equity_bar_weekly.week_start is 'ISO週の月曜（安定キー。週途中の暫定更新でも同一行を上書き）';
comment on column analytics.equity_bar_weekly.week_end is '週内最終営業日（データ列。金曜とは限らない）';
comment on column analytics.equity_bar_weekly.adjustment_factor is '週内 daily adjustment_factor の積。≠1 はイベント週（raw が混在基準）の印';

-- ============================================================
-- 2) 適用済み再基準化イベント台帳
-- ============================================================
create table if not exists analytics.equity_bar_weekly_rebase_events (
  local_code        text not null check (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$'),
  event_date        date not null,
  adjustment_factor numeric not null,
  applied_at        timestamptz not null default now(),
  primary key (local_code, event_date)
);

comment on table analytics.equity_bar_weekly_rebase_events is
  'equity_bar_weekly へ増分係数を適用済みの分割・併合イベント台帳。7日検知窓の再検知による二重適用を防ぐ';

-- ============================================================
-- 3) RLS（00053 stock_metrics と同一構成: authenticated SELECT / service_role ALL）
-- ============================================================
alter table analytics.equity_bar_weekly enable row level security;

drop policy if exists "authenticated_select" on analytics.equity_bar_weekly;
create policy "authenticated_select"
  on analytics.equity_bar_weekly for select to authenticated using (true);

drop policy if exists "service_role_all" on analytics.equity_bar_weekly;
create policy "service_role_all"
  on analytics.equity_bar_weekly for all to service_role using (true) with check (true);

grant select on analytics.equity_bar_weekly to authenticated;
grant all on analytics.equity_bar_weekly to service_role;

-- 台帳は運用内部情報のため authenticated にも公開しない
alter table analytics.equity_bar_weekly_rebase_events enable row level security;

drop policy if exists "service_role_all" on analytics.equity_bar_weekly_rebase_events;
create policy "service_role_all"
  on analytics.equity_bar_weekly_rebase_events for all to service_role using (true) with check (true);

revoke all on analytics.equity_bar_weekly_rebase_events from anon;
revoke all on analytics.equity_bar_weekly_rebase_events from authenticated;
grant all on analytics.equity_bar_weekly_rebase_events to service_role;

-- ============================================================
-- 4) 分割・併合の週足再基準化 RPC（1イベント=1トランザクション）
-- ============================================================
-- 前提: 呼び出し時点で equity_bar_daily は 00093 の rebase 済み（Cron A のゲートで保証）。
-- 戻り値: 更新・置換した週足行数。台帳に記録済みのイベントは何もせず -1 を返す（冪等）。
-- p_factor が 0 以下の場合は例外（台帳 INSERT ごとロールバック）。
create or replace function analytics.apply_weekly_rebase_event(
  p_local_code text,
  p_event_date date,
  p_factor numeric
)
returns integer
language plpgsql
security definer
set search_path = ''
set statement_timeout = '60s'
as $$
declare
  v_inserted integer;
  v_week_start date;
  v_event_week integer := 0;
  v_prior integer := 0;
begin
  if p_factor is null or p_factor <= 0 then
    raise exception 'apply_weekly_rebase_event: invalid factor % for % on %',
      p_factor, p_local_code, p_event_date;
  end if;

  -- 台帳への記録が二重適用ガード。既記録なら無変更で抜ける。
  insert into analytics.equity_bar_weekly_rebase_events (local_code, event_date, adjustment_factor)
  values (p_local_code, p_event_date, p_factor)
  on conflict (local_code, event_date) do nothing;
  get diagnostics v_inserted = row_count;
  if v_inserted = 0 then
    return -1;
  end if;

  -- ISO週の月曜（date_trunc('week') は ISO 月曜起点）
  v_week_start := date_trunc('week', p_event_date)::date;

  -- (a) イベント週より前の全週: 既存 adj_* へ増分係数のみ適用（raw は触らない）
  --     adj_volume は「週合計 ÷ 係数」の週次近似（00093 の日次規約は各日丸め後に合計するため
  --     一般には微小差が出る）。価格系の numeric(18,6) 丸めもイベント毎に累積し得るが、
  --     いずれもチャート用途では無視できる誤差として意図的に許容する

  update analytics.equity_bar_weekly w
  set adj_open   = (w.adj_open  * p_factor)::numeric(18,6),
      adj_high   = (w.adj_high  * p_factor)::numeric(18,6),
      adj_low    = (w.adj_low   * p_factor)::numeric(18,6),
      adj_close  = (w.adj_close * p_factor)::numeric(18,6),
      adj_volume = case when w.adj_volume is null then null
                        else w.adj_volume / p_factor end
  where w.local_code = p_local_code
    and w.week_start < v_week_start;
  get diagnostics v_prior = row_count;

  -- (b) イベント日を含む週: 再基準化済み日足から再集計して行ごと置換
  --     集計規約は src/lib/analytics/weekly-bars.ts の aggregateWeeklyBars と一致させること:
  --     open=週初値 / high=週内最大 / low=週内最小 / close=週末値 / volume・turnover=合計 /
  --     adjustment_factor=週内係数の積 / adj系も同一規約 / week_end=週内最終営業日
  insert into analytics.equity_bar_weekly as w
    (local_code, week_start, week_end,
     open, high, low, close, volume, turnover_value,
     adjustment_factor, adj_open, adj_high, adj_low, adj_close, adj_volume)
  select
    d.local_code,
    v_week_start,
    max(d.trade_date),
    (array_agg(d.open  order by d.trade_date asc))[1],
    max(d.high),
    min(d.low),
    (array_agg(d.close order by d.trade_date desc))[1],
    sum(d.volume),
    sum(d.turnover_value),
    coalesce(jquants_core.numeric_product(coalesce(d.adjustment_factor, 1)), 1),
    (array_agg(d.adj_open  order by d.trade_date asc))[1],
    max(d.adj_high),
    min(d.adj_low),
    (array_agg(d.adj_close order by d.trade_date desc))[1],
    sum(d.adj_volume)
  from jquants_core.equity_bar_daily d
  where d.local_code = p_local_code
    and d.session = 'DAY'
    and d.trade_date >= v_week_start
    and d.trade_date < v_week_start + 7
  group by d.local_code
  on conflict (local_code, week_start) do update
  set week_end          = excluded.week_end,
      open              = excluded.open,
      high              = excluded.high,
      low               = excluded.low,
      close             = excluded.close,
      volume            = excluded.volume,
      turnover_value    = excluded.turnover_value,
      adjustment_factor = excluded.adjustment_factor,
      adj_open          = excluded.adj_open,
      adj_high          = excluded.adj_high,
      adj_low           = excluded.adj_low,
      adj_close         = excluded.adj_close,
      adj_volume        = excluded.adj_volume,
      ingested_at       = now();
  get diagnostics v_event_week = row_count;

  return v_prior + v_event_week;
end;
$$;

comment on function analytics.apply_weekly_rebase_event(text, date, numeric) is
  '分割・併合1イベントの週足再基準化（過去週=増分係数適用/イベント週=日足再集計置換/台帳記録を原子実行）。'
  '記録済みイベントは -1。呼び出しはイベント日の降順で1件ずつ（計画書 §4.3-3）';

-- 実行元は Cron A の service_role 呼び出しのみ
revoke execute on function analytics.apply_weekly_rebase_event(text, date, numeric) from public;
revoke execute on function analytics.apply_weekly_rebase_event(text, date, numeric) from anon;
revoke execute on function analytics.apply_weekly_rebase_event(text, date, numeric) from authenticated;
grant execute on function analytics.apply_weekly_rebase_event(text, date, numeric) to service_role;
