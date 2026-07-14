-- 00094_stock_metrics_split_adjustment.sql
-- analytics.refresh_stock_metrics を株式分割・併合に強い実装へ置き換える（00049 の全文ベース）。
-- 問題: 一株あたり財務値(eps/bps/dividend_annual/shares)は開示時点の基準のまま、株価(adj_close)は
--       分割後基準で入るため、per/pbr/psr/dividend_yield/market_cap が分割日で約 1/factor 倍に歪む。
--       （31100 の 1→5 分割で PER 2.99・利回り3.70%・時価総額1,294億円 等の誤値を確認）
-- 方針(案3): 最新開示 disclosed_date より後に発生した分割・併合の累積係数 cum_factor を求め、
--            一株あたり値に乗算・shares に除算して株価基準へ換算する（NULL化ではなく換算）。
-- 依存: 累積積アグリゲート jquants_core.numeric_product は 00093(Issue 1) で定義済みの前提。
--
-- 00049 からの差分は下記 4 箇所（各所に「-- [00094 差分]」を付記）:
--   (A) ranked_fin に disclosed_date を追加し、順序を fiscal_year_end/disclosed_date/disclosed_time で強化
--   (B) fin に disclosed_date を伝播
--   (C) split_adj CTE（銘柄ごとの累積調整係数）と fin_adj CTE（換算済み一株あたり値）を追加
--   (D) base の join 元を fin → fin_adj に差し替え（base 本体の式は不変）

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
  px as (
    select b1.local_code, b1.trade_date as as_of_date, b1.adj_close as close,
           b22.adj_close as c_1m, b64.adj_close as c_3m,
           b127.adj_close as c_6m, b253.adj_close as c_1y
    from ranked_bars b1
    left join ranked_bars b22  on b22.local_code = b1.local_code and b22.rn = 22
    left join ranked_bars b64  on b64.local_code = b1.local_code and b64.rn = 64
    left join ranked_bars b127 on b127.local_code = b1.local_code and b127.rn = 127
    left join ranked_bars b253 on b253.local_code = b1.local_code and b253.rn = 253
    where b1.rn = 1
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
