-- 00118_fix_refresh_bottomup_delete_where.sql
-- 00117 の refresh_bottomup_per_bands() が本番で実行不能だった不具合の修正。
--
-- 【原因】Supabase は API 経由のセッションに pg-safeupdate を適用しており、WHERE 句の無い
-- DELETE を拒否する（エラー: 21000 "DELETE requires a WHERE clause"）。これは SECURITY DEFINER
-- 関数の内部の文にも及ぶため、00117 の「DELETE 全行 → INSERT」の全置換が PostgREST RPC 経由では
-- 一度も成功しなかった（マイグレーション適用自体は管理接続なので成功する点に注意。
-- 適用検証だけでは見つからず、初回リフレッシュの実行で発覚した）。
--
-- 【修正】全置換をやめ、「UPSERT（全現行銘柄）→ 現行銘柄以外を WHERE 付き DELETE」へ変更する。
--   - safeupdate を満たす（DELETE に実質的な WHERE が付く）
--   - TRUNCATE を使わないのも意図的: TRUNCATE は ACCESS EXCLUSIVE ロックをトランザクション
--     終了まで保持するため、夜間リフレッシュ（数十秒）の間 get_bottomup_per_bands の読み取りが
--     全部待たされる。UPSERT なら行ロックのみで読者を止めない。
--
-- 関数本体の集計SQL（CTE 群）は 00117 から一切変更していない。

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
  left join fwd   on fwd.code = c.code
  on conflict (local_code) do update set
    per_min       = excluded.per_min,
    per_median    = excluded.per_median,
    per_max       = excluded.per_max,
    per_count     = excluded.per_count,
    sample_from   = excluded.sample_from,
    current_price = excluded.current_price,
    forward_eps   = excluded.forward_eps,
    price_cutoff  = excluded.price_cutoff,
    refreshed_at  = excluded.refreshed_at;

  get diagnostics v_count = row_count;

  -- 構成銘柄から外れた行の掃除（WHERE 付き = safeupdate を満たす）
  delete from analytics.bottomup_per_bands b
  where not (b.local_code = any(v_codes));

  return v_count;
end;
$$;

comment on function analytics.refresh_bottomup_per_bands() is
  'bottomup_per_bands の日次リフレッシュ（UPSERT + 現行構成銘柄外の掃除）。対象は basket_constituents(valid_to is null) の全銘柄、遡り基準日はJST今日-2年。Cron A から PostgREST RPC で呼ぶ。00118 で pg-safeupdate 対応（WHERE無しDELETE不可）。';

revoke execute on function analytics.refresh_bottomup_per_bands() from public, anon, authenticated;
grant execute on function analytics.refresh_bottomup_per_bands() to service_role;
