-- 00108_refresh_stock_metrics_perf.sql
-- analytics.refresh_stock_metrics の px(モメンタム基準日取得)を性能退行に強い実装へ置換する。
--
-- 障害: Cron A の "Refresh factor metrics" が 2026-07-18 以降 504 Gateway Timeout で失敗
--       （RPC が Supabase ゲートウェイの ~120s 上限を超過。DB 実測でも本体クエリが 120s 超）。
-- 直接原因: 2026-07-18 の autoanalyze で統計が更新され、プランナが px の
--           「ranked_bars(materialized・約114万行) を rn 条件で5回自己結合」部分を
--           ネステッドループ（外側行ごとに114万行CTEを再走査）へ退行させた。
--           base の推定行数が 4 行に潰れ、実際の ~3700 行で数十億行走査となり時間超過。
-- 対策: px を「ranked_bars を1回だけスキャンする条件付き集約ピボット」へ書き換える。
--       自己結合が消えるため、統計に依存せず安定して ~25s で完了する
--       （本番 EXPLAIN ANALYZE で 23.4s を確認・旧実装比 >120s→23s）。
-- 意味: 00094 からの差分は px CTE 1箇所のみ。出力は完全一致
--       （同一データで old/new を突合し old_n=new_n=4636・コード差分0・値差分0 を検証済み）。
-- 依存: 00093/00094（numeric_product・split_adj）を前提に、00094 の全文をベースにしている。

create or replace function analytics.refresh_stock_metrics(p_date date default null)
returns integer
language plpgsql
security definer
set search_path = ''
set statement_timeout = '180s'
as $$
declare
  v_date date;
  v_rows integer;
begin
  v_date := coalesce(p_date, (select max(trade_date) from jquants_core.equity_bar_daily));
  if v_date is null then return 0; end if;

  with ranked_bars as (
    select local_code, trade_date, adj_close,
           row_number() over (partition by local_code order by trade_date desc) as rn
    from jquants_core.equity_bar_daily
    where trade_date <= v_date
      and trade_date > v_date - interval '420 days'  -- 1年モメンタム(rn<=253)に必要な範囲のみ
  ),
  -- [00108 差分] モメンタム基準日の取得を「5本の自己結合」から
  -- 「rn で絞った条件付き集約ピボット」へ置換（ranked_bars を1回スキャンで済ませる）。
  -- (local_code, rn) は row_number により一意なので max(...) filter は単一値を返し、
  -- 出力は 00094 の自己結合版と完全一致（本番データで old_n=new_n=4636・値/コード差分0を検証済み）。
  px as (
    select
      local_code,
      max(trade_date) filter (where rn = 1)   as as_of_date,
      max(adj_close)  filter (where rn = 1)   as close,
      max(adj_close)  filter (where rn = 22)  as c_1m,
      max(adj_close)  filter (where rn = 64)  as c_3m,
      max(adj_close)  filter (where rn = 127) as c_6m,
      max(adj_close)  filter (where rn = 253) as c_1y
    from ranked_bars
    where rn in (1, 22, 64, 127, 253)
    group by local_code
  ),
  ranked_fin as (
    -- [00094 差分 (A)] disclosed_date を追加し、同一 fiscal_year_end の複数開示に対する決定性を強化
    -- （最新開示が rn=1 になるよう disclosed_date/time の降順を tie-break に採用）
    select local_code, disclosed_date, fiscal_year_end, sales, operating_profit, net_income,
           eps, bps, equity, total_assets, equity_to_asset_ratio, cf_operating,
           dividend_annual, shares_outstanding_fy,
           row_number() over (
             partition by local_code
             order by fiscal_year_end desc, disclosed_date desc, disclosed_time desc
           ) as rn
    from jquants_core.financial_disclosure
    where period_type = 'FY' and sales is not null
  ),
  fin as (
    select f1.local_code, f1.disclosed_date, f1.fiscal_year_end, f1.sales, f1.operating_profit,
           f1.net_income, f1.eps, f1.bps, f1.equity, f1.total_assets,
           f1.equity_to_asset_ratio, f1.cf_operating, f1.dividend_annual,
           f1.shares_outstanding_fy,  -- [00094 差分 (B)] disclosed_date を fin に伝播（split_adj で使用）
           f2.sales as prev_sales, f2.operating_profit as prev_op, f2.net_income as prev_np
    from ranked_fin f1
    left join ranked_fin f2 on f2.local_code = f1.local_code and f2.rn = 2
    where f1.rn = 1
  ),
  -- [00094 差分 (C)] 銘柄ごとの累積調整係数。最新開示 disclosed_date より後〜v_date までの
  -- factor(<>1) の積。同一 trade_date に複数 session 行がある場合の二重掛けを distinct on(trade_date)
  -- で排除。イベントが無ければ空集合となり initcond='1' のため 1（＝未調整）になる。
  split_adj as (
    select fin.local_code,
           coalesce((
             select jquants_core.numeric_product(ev.adjustment_factor)
             from (
               select distinct on (b.trade_date) b.trade_date, b.adjustment_factor
               from jquants_core.equity_bar_daily b
               where b.local_code = fin.local_code
                 and b.adjustment_factor is not null and b.adjustment_factor <> 1
                 and b.trade_date > fin.disclosed_date
                 and b.trade_date <= v_date
               order by b.trade_date, b.session
             ) ev
           ), 1) as cum_factor
    from fin
  ),
  -- [00094 差分 (C)] 一株あたり値を株価基準へ換算。cum_factor<=0 は異常値ガードとして未調整(1)にフォールバック。
  -- eps/bps/dividend_annual は ×factor、shares_outstanding_fy は ÷factor（時価総額を不変に保つ）。
  fin_adj as (
    select f.local_code, f.disclosed_date, f.fiscal_year_end, f.sales, f.operating_profit,
           f.net_income, f.equity, f.total_assets, f.equity_to_asset_ratio, f.cf_operating,
           f.prev_sales, f.prev_op, f.prev_np,
           f.eps             * g.factor as eps,
           f.bps             * g.factor as bps,
           f.dividend_annual * g.factor as dividend_annual,
           f.shares_outstanding_fy / g.factor as shares_outstanding_fy
    from fin f
    join split_adj sa on sa.local_code = f.local_code
    cross join lateral (select case when sa.cum_factor > 0 then sa.cum_factor else 1 end) as g(factor)
  ),
  base as (
    select
      px.local_code, px.as_of_date, px.close,
      em.sector17_code, em.sector17_name, fin.fiscal_year_end::date as fiscal_year_end,
      (px.close * fin.shares_outstanding_fy) as market_cap,
      case when fin.eps > 0 then px.close / fin.eps end as per,
      case when fin.bps > 0 then px.close / fin.bps end as pbr,
      case when fin.sales > 0 and fin.shares_outstanding_fy > 0
           then (px.close * fin.shares_outstanding_fy) / fin.sales end as psr,
      case when px.close > 0 and fin.dividend_annual is not null
           then fin.dividend_annual / px.close * 100 end as dividend_yield,
      case when fin.equity > 0 then fin.net_income / fin.equity * 100 end as roe,
      case when fin.total_assets > 0 then fin.net_income / fin.total_assets * 100 end as roa,
      case when fin.sales > 0 then fin.operating_profit / fin.sales * 100 end as operating_margin,
      case when fin.sales > 0 then fin.net_income / fin.sales * 100 end as net_margin,
      case when fin.prev_sales > 0 then (fin.sales - fin.prev_sales) / abs(fin.prev_sales) * 100 end as sales_yoy,
      case when fin.prev_op <> 0 then (fin.operating_profit - fin.prev_op) / abs(fin.prev_op) * 100 end as op_yoy,
      case when fin.prev_np <> 0 then (fin.net_income - fin.prev_np) / abs(fin.prev_np) * 100 end as np_yoy,
      case when fin.equity_to_asset_ratio is not null then fin.equity_to_asset_ratio * 100 end as equity_ratio,
      case when fin.net_income <> 0 and fin.cf_operating is not null then fin.cf_operating / fin.net_income end as accrual_quality,
      case when px.c_1m  > 0 then (px.close - px.c_1m)  / px.c_1m  * 100 end as ret_1m,
      case when px.c_3m  > 0 then (px.close - px.c_3m)  / px.c_3m  * 100 end as ret_3m,
      case when px.c_6m  > 0 then (px.close - px.c_6m)  / px.c_6m  * 100 end as ret_6m,
      case when px.c_1y  > 0 then (px.close - px.c_1y)  / px.c_1y  * 100 end as ret_1y
    from px
    join jquants_core.equity_master em
      on em.local_code = px.local_code and em.is_current = true
    join fin_adj fin on fin.local_code = px.local_code  -- [00094 差分 (D)] fin → fin_adj（換算済み値で計算）
  ),
  -- sector17 内 percentile（低い方が良い指標は降順=低い値が高pct）と z-score
  scored as (
    select b.*,
      case when per  is not null then (1 - percent_rank() over (partition by sector17_code order by per  asc)) * 100 end as per_pct,
      case when pbr  is not null then (1 - percent_rank() over (partition by sector17_code order by pbr  asc)) * 100 end as pbr_pct,
      case when psr  is not null then (1 - percent_rank() over (partition by sector17_code order by psr  asc)) * 100 end as psr_pct,
      case when roe              is not null then percent_rank() over (partition by sector17_code order by roe asc) * 100 end as roe_pct,
      case when operating_margin is not null then percent_rank() over (partition by sector17_code order by operating_margin asc) * 100 end as opm_pct,
      case when equity_ratio     is not null then percent_rank() over (partition by sector17_code order by equity_ratio asc) * 100 end as eqr_pct,
      case when ret_3m is not null then percent_rank() over (partition by sector17_code order by ret_3m asc) * 100 end as r3_pct,
      case when ret_6m is not null then percent_rank() over (partition by sector17_code order by ret_6m asc) * 100 end as r6_pct,
      -- z-score (低い方が良い指標は符号反転)
      case when stddev_pop(per) over w <> 0 then -(per - avg(per) over w) / stddev_pop(per) over w end as z_per,
      case when stddev_pop(pbr) over w <> 0 then -(pbr - avg(pbr) over w) / stddev_pop(pbr) over w end as z_pbr,
      case when stddev_pop(roe) over w <> 0 then (roe - avg(roe) over w) / stddev_pop(roe) over w end as z_roe,
      case when stddev_pop(operating_margin) over w <> 0 then (operating_margin - avg(operating_margin) over w) / stddev_pop(operating_margin) over w end as z_opm,
      case when stddev_pop(ret_6m) over w <> 0 then (ret_6m - avg(ret_6m) over w) / stddev_pop(ret_6m) over w end as z_r6,
      count(*) over (partition by sector17_code) as sector_count
    from base b
    window w as (partition by sector17_code)
  ),
  final as (
    select s.*,
      (select avg(v) from (values (per_pct),(pbr_pct),(psr_pct)) t(v) where v is not null) as value_pct,
      (select avg(v) from (values (roe_pct),(opm_pct),(eqr_pct)) t(v) where v is not null) as quality_pct,
      (select avg(v) from (values (r3_pct),(r6_pct)) t(v) where v is not null) as momentum_pct,
      (select avg(v) from (values (z_per),(z_pbr)) t(v) where v is not null) as value_score,
      (select avg(v) from (values (z_roe),(z_opm)) t(v) where v is not null) as quality_score,
      z_r6 as momentum_score
    from scored s
  )
  insert into analytics.stock_metrics as m (
    as_of_date, local_code, sector17_code, sector17_name, fiscal_year_end,
    close, market_cap, per, pbr, psr, dividend_yield,
    roe, roa, operating_margin, net_margin, sales_yoy, op_yoy, np_yoy,
    equity_ratio, accrual_quality, ret_1m, ret_3m, ret_6m, ret_1y,
    value_pct, quality_pct, momentum_pct,
    value_score, quality_score, momentum_score, total_score, sector_count, updated_at
  )
  select
    as_of_date, local_code, sector17_code, sector17_name, fiscal_year_end,
    close, market_cap, per, pbr, psr, dividend_yield,
    roe, roa, operating_margin, net_margin, sales_yoy, op_yoy, np_yoy,
    equity_ratio, accrual_quality, ret_1m, ret_3m, ret_6m, ret_1y,
    round(value_pct::numeric, 2), round(quality_pct::numeric, 2), round(momentum_pct::numeric, 2),
    round(value_score::numeric, 3), round(quality_score::numeric, 3),
    round(momentum_score::numeric, 3),
    round((coalesce(value_score, 0) + coalesce(quality_score, 0) + coalesce(momentum_score, 0))::numeric, 3),
    sector_count, now()
  from final
  on conflict (as_of_date, local_code) do update set
    sector17_code = excluded.sector17_code, sector17_name = excluded.sector17_name,
    fiscal_year_end = excluded.fiscal_year_end, close = excluded.close,
    market_cap = excluded.market_cap, per = excluded.per, pbr = excluded.pbr,
    psr = excluded.psr, dividend_yield = excluded.dividend_yield, roe = excluded.roe,
    roa = excluded.roa, operating_margin = excluded.operating_margin,
    net_margin = excluded.net_margin, sales_yoy = excluded.sales_yoy,
    op_yoy = excluded.op_yoy, np_yoy = excluded.np_yoy, equity_ratio = excluded.equity_ratio,
    accrual_quality = excluded.accrual_quality, ret_1m = excluded.ret_1m,
    ret_3m = excluded.ret_3m, ret_6m = excluded.ret_6m, ret_1y = excluded.ret_1y,
    value_pct = excluded.value_pct, quality_pct = excluded.quality_pct,
    momentum_pct = excluded.momentum_pct, value_score = excluded.value_score,
    quality_score = excluded.quality_score, momentum_score = excluded.momentum_score,
    total_score = excluded.total_score, sector_count = excluded.sector_count,
    updated_at = now();

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

grant execute on function analytics.refresh_stock_metrics(date) to service_role;
