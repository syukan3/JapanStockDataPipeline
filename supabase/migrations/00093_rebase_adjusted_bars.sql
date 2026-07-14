-- 00093_rebase_adjusted_bars.sql
-- 株式分割・併合の adj_* 再基準化エンジン。
--
-- 背景: J-Quants の調整後株価は遡及型だが、パイプラインは各日を一度取り込むだけのため
-- 分割発生後に過去行の adj_* が旧基準のまま残る（forward-fill 再取込による混在もあり得る）。
-- 本関数は raw(open/high/low/close/volume) と adjustment_factor から adj_* をローカル再計算し、
-- 全履歴を最新基準に揃える（冪等・API 再依存なし）。
--
-- adj 規約（実データで確認済み・J-Quants 準拠）:
--   - adjustment_factor は権利落ち日の行に付く（当日 raw は分割後値）
--   - adj_price(t)  = raw_price(t)  × ∏{s > t} factor(s)   ※当日 factor は当日 adj に掛けない
--   - adj_volume(t) = raw_volume(t) ÷ ∏{s > t} factor(s)
--   - turnover_value は調整不要
--
-- 呼び出し: scripts/cron/rebase-adjusted-bars.ts（Cron A の equity_bars 同期後・analytics 再計算前）

-- 1) 分割・併合イベントの部分インデックス（検知と累積係数計算の両方で使用）
create index if not exists idx_equity_bar_adjustment_events
  on jquants_core.equity_bar_daily (local_code, trade_date)
  where adjustment_factor is not null and adjustment_factor <> 1;

-- 2) numeric 積アグリゲート（exp(sum(ln)) の精度劣化を避ける。00094 の財務値換算もこれを使う）
--    create aggregate に if not exists / or replace が無いため DO で冪等化。
do $$
begin
  create aggregate jquants_core.numeric_product(numeric) (
    sfunc = numeric_mul,
    stype = numeric,
    initcond = '1'
  );
exception
  when duplicate_function then null;
end;
$$;

comment on aggregate jquants_core.numeric_product(numeric) is
  'numeric の積。分割・併合の累積調整係数計算用（exp(sum(ln)) の精度劣化回避）';

-- 3) 1銘柄の全履歴 adj_* を raw から再計算する関数（更新行数を返す）
create or replace function jquants_core.rebase_adjusted_bars(p_local_code text)
returns integer
language plpgsql
security definer
set search_path = ''
set statement_timeout = '180s'
as $$
declare
  v_rows integer;
begin
  with day_factors as (
    -- 日単位の factor 系列。同一 trade_date に複数 session 行があっても積には1回だけ入れる。
    -- factor が session 間で片方 NULL の場合に備え、非NULL行を優先して1行選ぶ。
    select distinct on (trade_date)
           trade_date,
           coalesce(adjustment_factor, 1) as factor
    from jquants_core.equity_bar_daily
    where local_code = p_local_code
    order by trade_date, adjustment_factor nulls last
  ),
  cum as (
    -- cum(t) = ∏{s > t} factor(s)。当日 factor は含めない（1 preceding まで）。
    select trade_date,
           coalesce(
             jquants_core.numeric_product(factor) over (
               order by trade_date desc
               rows between unbounded preceding and 1 preceding
             ),
             1
           ) as cum_factor
    from day_factors
  )
  update jquants_core.equity_bar_daily b
  set adj_open   = (b.open  * c.cum_factor)::numeric(18,6),
      adj_high   = (b.high  * c.cum_factor)::numeric(18,6),
      adj_low    = (b.low   * c.cum_factor)::numeric(18,6),
      adj_close  = (b.close * c.cum_factor)::numeric(18,6),
      adj_volume = case when b.volume is null then null
                        else round(b.volume::numeric / c.cum_factor)::bigint end
  from cum c
  where b.local_code = p_local_code
    and b.trade_date = c.trade_date
    and c.cum_factor > 0  -- 不正 factor（0以下）は既存値を温存
    -- 同一 trade_date の全 session 行に同じ cum を適用しつつ、値が変わる行のみ UPDATE
    and (
         b.adj_open   is distinct from (b.open  * c.cum_factor)::numeric(18,6)
      or b.adj_high   is distinct from (b.high  * c.cum_factor)::numeric(18,6)
      or b.adj_low    is distinct from (b.low   * c.cum_factor)::numeric(18,6)
      or b.adj_close  is distinct from (b.close * c.cum_factor)::numeric(18,6)
      or b.adj_volume is distinct from (case when b.volume is null then null
                                             else round(b.volume::numeric / c.cum_factor)::bigint end)
    );

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

comment on function jquants_core.rebase_adjusted_bars(text) is
  '1銘柄の全履歴 adj_* を raw × 累積調整係数で再基準化（値が変わる行のみ UPDATE、更新行数を返す）';

-- 実行元は cron-a.yml / バックフィルの service_role 呼び出しのみ
revoke execute on function jquants_core.rebase_adjusted_bars(text) from public;
revoke execute on function jquants_core.rebase_adjusted_bars(text) from anon;
revoke execute on function jquants_core.rebase_adjusted_bars(text) from authenticated;
grant execute on function jquants_core.rebase_adjusted_bars(text) to service_role;
