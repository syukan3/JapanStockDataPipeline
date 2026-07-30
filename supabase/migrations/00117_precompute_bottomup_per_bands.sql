-- 00117_precompute_bottomup_per_bands.sql
-- /baskets ボトムアップ軸のPERバンド集計を「クエリ時計算」から「夜間事前計算」へ移す。
--
-- 【経緯】00116 は get_bottomup_per_bands をクエリ時集計RPCとして導入した。単体では0.5〜7秒で
-- 正しく動く（Portfolio 側の同値検証 1,931件・不一致0で確認済み）が、getBasketSummaries が
-- 14バスケットを並列処理すると、他バスケットのページ取得負荷と重なって statement timeout
-- （既定8秒）に達し、TS経路への全件フォールバックが発生した（実測: RPC同時2に絞っても
-- タイムアウト6件・コールド57秒で改善ゼロ）。クエリ時集計はこのDBのCPU性能では成立しない。
--
-- 【本マイグレーションの構成】technical_metrics / refresh_stock_metrics(00049/00108) と同じ
-- 「Cron A で日次リフレッシュ→アプリは参照のみ」の形へ移す:
--   1. analytics.bottomup_per_bands … 事前計算テーブル（銘柄あたり1行・約1,900行）
--   2. analytics.refresh_bottomup_per_bands() … 00116 の集計SQLをそのまま INSERT..SELECT へ
--      移した日次リフレッシュ関数。対象は basket_constituents(valid_to is null) の全銘柄。
--      statement_timeout=180s はリポの確立パターン（00049/00108 で実証済み）。
--   3. analytics.get_bottomup_per_bands() … シグネチャ不変のまま中身をテーブル参照へ書換え。
--      Portfolio 側は無変更で乗る。鮮度ガード（5日超は例外）を持ち、stale ならエラー→
--      Portfolio が従来のTS経路へフォールバックする（silent に古い値を返さない）。
--
-- 【データ鮮度のセマンティクス】バンドは「直近リフレッシュ時点(JST)の2年窓」になる。
-- Portfolio の bundle キャッシュ自体が Cron A 後の revalidateTag でしか更新されないため、
-- ユーザー可視の鮮度は従来と変わらない。構成銘柄の入替もリフレッシュも同じ Cron A 内で
-- 連続実行されるため、新規銘柄が precompute に無い時間帯は実質生じない（生じた場合は
-- その銘柄の upside が null になり、翌リフレッシュで自己回復する）。
--
-- 【冪等性】refresh は DELETE→INSERT の全置換（対象 約1,900行）。何度呼んでも安全。

-- ============================================================
-- 1. 事前計算テーブル
-- ============================================================
create table if not exists analytics.bottomup_per_bands (
  local_code    text primary key,
  per_min       numeric,
  per_median    numeric,
  per_max       numeric,
  per_count     integer,
  sample_from   date,
  current_price numeric,
  forward_eps   numeric,
  -- リフレッシュ時に使った遡り基準日（JST今日 - 2年）。同値検証テストがTS経路へ渡す
  price_cutoff  date not null,
  refreshed_at  timestamptz not null default now()
);

comment on table analytics.bottomup_per_bands is
  '/baskets ボトムアップ軸の事前計算（過去trailing PERレンジ・現在値・翌期会社予想EPS）。Cron A の refresh_bottomup_per_bands() が日次全置換。参照は get_bottomup_per_bands() 経由。';

-- RLS / GRANT（00105 と同方針: authenticated SELECT + service_role ALL）
alter table analytics.bottomup_per_bands enable row level security;
drop policy if exists "authenticated_select" on analytics.bottomup_per_bands;
create policy "authenticated_select"
  on analytics.bottomup_per_bands for select to authenticated using (true);
drop policy if exists "service_role_all" on analytics.bottomup_per_bands;
create policy "service_role_all"
  on analytics.bottomup_per_bands for all to service_role using (true) with check (true);
grant select on analytics.bottomup_per_bands to authenticated;
grant all on analytics.bottomup_per_bands to service_role;

-- ============================================================
-- 2. 日次リフレッシュ関数（00116 の集計SQLを移設。セマンティクスは 00116 ヘッダ参照）
-- ============================================================
create or replace function analytics.refresh_bottomup_per_bands()
returns integer
language plpgsql
security definer
set search_path = ''
set statement_timeout = '180s'
as $$
declare
  v_cutoff date := ((now() at time zone 'Asia/Tokyo')::date - interval '2 years')::date;
  v_codes  text[];
  v_count  integer;
begin
  select array_agg(distinct c.local_code)
    into v_codes
  from analytics.basket_constituents c
  where c.valid_to is null;

  if v_codes is null then
    return 0; -- 構成銘柄が未投入の環境では何もしない（Cron の他ステップは継続）
  end if;

  delete from analytics.bottomup_per_bands;

  insert into analytics.bottomup_per_bands
    (local_code, per_min, per_median, per_max, per_count, sample_from,
     current_price, forward_eps, price_cutoff, refreshed_at)
  with codes as (
    select unnest(v_codes) as code
  ),
  fy as (
    select
      f.local_code        as code,
      f.fiscal_year_end   as fye,
      f.disclosed_date    as disclosed_date,
      f.eps               as eps,
      f.next_forecast_eps as next_forecast_eps,
      row_number() over (
        partition by f.local_code, f.fiscal_year_end
        order by f.disclosed_date desc, f.disclosure_id asc
      ) as rn_fye,
      row_number() over (
        partition by f.local_code
        order by f.fiscal_year_end desc, f.disclosed_date desc, f.disclosure_id asc
      ) as rn_code
    from jquants_core.financial_disclosure f
    where f.local_code = any(v_codes)
      and f.period_type = 'FY'
      and f.eps is not null
  ),
  fwd as (
    select fy.code, fy.next_forecast_eps from fy where fy.rn_code = 1
  ),
  actuals as (
    select fy.code, fy.fye, fy.disclosed_date, fy.eps::double precision as eps
    from fy
    where fy.rn_fye = 1 and fy.eps > 0 and fy.fye is not null and fy.disclosed_date is not null
  ),
  ranked as (
    select a.code, a.disclosed_date, a.eps,
           dense_rank() over (partition by a.code order by a.fye collate "C")::double precision as fye_rank
    from actuals a
  ),
  runmax as (
    select distinct r.code, r.disclosed_date as eff_from,
           max(array[r.fye_rank, r.eps]) over (partition by r.code order by r.disclosed_date) as best
    from ranked r
  ),
  eff_span as (
    select m.code, m.eff_from, m.best[2] as eps,
           lead(m.eff_from) over (partition by m.code order by m.eff_from) as eff_to
    from runmax m
  ),
  px as materialized (
    select b.local_code as code, b.trade_date, coalesce(b.adj_close, b.close) as price
    from jquants_core.equity_bar_daily b
    where b.local_code = any(v_codes)
      and b.trade_date >= v_cutoff
      and coalesce(b.adj_close, b.close) > 0
  ),
  cur as (
    select c.code, p.price
    from codes c
    cross join lateral (
      select coalesce(b.adj_close, b.close) as price
      from jquants_core.equity_bar_daily b
      where b.local_code = c.code
        and b.trade_date >= v_cutoff
        and coalesce(b.adj_close, b.close) > 0
      order by b.trade_date desc, b.session desc
      limit 1
    ) p
  ),
  per_rows as (
    select px.code, px.trade_date, px.price::double precision / e.eps as per
    from px
    join eff_span e
      on e.code = px.code
     and px.trade_date >= e.eff_from
     and (e.eff_to is null or px.trade_date < e.eff_to)
  ),
  bands as (
    select per_rows.code,
           count(*)::integer        as n,
           min(per_rows.trade_date) as sample_from,
           array_agg(per_rows.per order by per_rows.per) as sorted
    from per_rows
    group by per_rows.code
  ),
  stats as (
    select b.code, b.n, b.sample_from,
           b.sorted[1]   as lo,
           b.sorted[b.n] as hi,
           case
             when b.n % 2 = 1 then b.sorted[(b.n + 1) / 2]
             else (b.sorted[b.n / 2] + b.sorted[b.n / 2 + 1]) / 2::double precision
           end as med
    from bands b
    where b.n >= 5
  )
  select
    c.code,
    (floor(s.lo  * 100::double precision + 0.5) / 100::double precision)::numeric,
    (floor(s.med * 100::double precision + 0.5) / 100::double precision)::numeric,
    (floor(s.hi  * 100::double precision + 0.5) / 100::double precision)::numeric,
    s.n,
    s.sample_from,
    cur.price,
    fwd.next_forecast_eps,
    v_cutoff,
    now()
  from codes c
  left join stats s on s.code = c.code
  left join cur   on cur.code = c.code
  left join fwd   on fwd.code = c.code;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

comment on function analytics.refresh_bottomup_per_bands() is
  'bottomup_per_bands の日次全置換リフレッシュ。対象は basket_constituents(valid_to is null) の全銘柄、遡り基準日はJST今日-2年。Cron A から refresh_stock_metrics と同様に PostgREST RPC で呼ぶ。';

revoke execute on function analytics.refresh_bottomup_per_bands() from public, anon, authenticated;
grant execute on function analytics.refresh_bottomup_per_bands() to service_role;

-- ============================================================
-- 3. 参照RPCをテーブル読みへ書換え（シグネチャ不変・Portfolio 無変更）
-- ============================================================
create or replace function analytics.get_bottomup_per_bands(
  p_codes        text[],
  p_price_cutoff date
)
returns table (
  local_code    text,
  per_min       numeric,
  per_median    numeric,
  per_max       numeric,
  per_count     integer,
  sample_from   date,
  current_price numeric,
  forward_eps   numeric
)
language plpgsql
stable
security invoker
set search_path = ''
as $$
declare
  v_refreshed timestamptz;
begin
  -- 鮮度ガード: 事前計算が5日超 stale（Cron A 停止等）なら黙って古い値を返さず例外にする。
  -- Portfolio 側は RPC エラーで従来のTS経路（行転送・遅いが正しい）へフォールバックする。
  -- p_price_cutoff は互換のため受けるが参照しない（事前計算時の基準日が正）。
  select max(b.refreshed_at) into v_refreshed from analytics.bottomup_per_bands b;
  if v_refreshed is null or v_refreshed < now() - interval '5 days' then
    raise exception 'bottomup_per_bands is stale or empty (refreshed_at=%). Run analytics.refresh_bottomup_per_bands().', v_refreshed
      using errcode = 'P0001';
  end if;

  return query
  select b.local_code, b.per_min, b.per_median, b.per_max, b.per_count,
         b.sample_from, b.current_price, b.forward_eps
  from analytics.bottomup_per_bands b
  where b.local_code = any(p_codes);
end;
$$;

comment on function analytics.get_bottomup_per_bands(text[], date) is
  '構成銘柄ごとの過去trailing PERレンジ・現在値・翌期会社予想EPSを返す（00117以降は事前計算テーブル参照。計算セマンティクスは refresh_bottomup_per_bands / 00116 ヘッダ参照）。5日超staleは例外→呼び出し側がTS経路へフォールバック。';

revoke execute on function analytics.get_bottomup_per_bands(text[], date) from public, anon;
grant execute on function analytics.get_bottomup_per_bands(text[], date) to authenticated, service_role;
