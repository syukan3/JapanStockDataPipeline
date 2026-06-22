-- 00050_create_technical_metrics.sql
-- テクニカル指標の日次スナップショット（全銘柄の「最新値」のみ保持）。
-- analytics.stock_metrics(00048) と同居。Portfolio のスクリーナー＆銘柄カードの
-- テクニカル表示用に authenticated が参照する。
--
-- 計算: scripts/cron/refresh-technical.ts が equity_bar_daily(adj系) から日次計算して upsert。
--       Cron A 完了後（cron-a.yml の continue-on-error ステップ）に実行する想定。
-- 保持: 最新 as_of_date のみ（バッチ末尾で過去日を delete）→ 容量を一定（数MB）に保つ。
--       チャートの指標「時系列」は価格から Portfolio フロントで都度計算するため保存しない。
--
-- 原子的公開: 一括 upsert はバッチ分割されるため、書き込み途中に未確定の as_of_date 行が
--   一時的に存在する。これを公開面（authenticated の SELECT / ビュー stock_screen）に
--   見せないため、「公開済み日付マーカー」technical_publication を導入し、
--   - 基底表 technical_metrics の authenticated RLS を「公開日のみ可視」に絞り、
--   - ビュー stock_screen(00051) も同マーカーを参照する。
--   バッチは全件 upsert 成功後にマーカーを単一行 upsert で原子的に切り替える(service_role は RLS 迂回)。
--
-- 注意: analytics は既に Exposed schema（stock_metrics 稼働中）のため公開設定の変更は不要。

-- ============================================================
-- 公開済みテクニカル日付マーカー（単一行）
-- ============================================================
create table if not exists analytics.technical_publication (
  id                    boolean primary key default true check (id),
  published_as_of_date  date,
  updated_at            timestamptz not null default now()
);
insert into analytics.technical_publication (id, published_as_of_date)
  values (true, null)
  on conflict (id) do nothing;

alter table analytics.technical_publication enable row level security;
create policy "authenticated_select"
  on analytics.technical_publication for select to authenticated using (true);
create policy "service_role_all"
  on analytics.technical_publication for all to service_role using (true) with check (true);
grant select on analytics.technical_publication to authenticated;
grant all on analytics.technical_publication to service_role;

-- ============================================================
-- technical_metrics: 銘柄別テクニカル指標の日次スナップショット
-- ============================================================
create table if not exists analytics.technical_metrics (
  as_of_date      date not null,            -- 採用した株価日（最新立会日）
  local_code      text not null,
  close           numeric(14,2),            -- 採用終値(adj_close)。参照・並べ替え用

  -- 移動平均（adj_close）と乖離率(%)
  sma_25          numeric(14,4),
  sma_75          numeric(14,4),
  sma_200         numeric(14,4),
  dev_25          numeric(8,2),             -- (close - sma_25) / sma_25 * 100
  dev_75          numeric(8,2),
  dev_200         numeric(8,2),
  above_sma200    boolean,                  -- close > sma_200

  -- クロス（SMA25 × SMA75）
  cross_25_75     text check (cross_25_75 in ('golden', 'dead')),
  cross_25_75_age int,                      -- 直近クロスからの経過営業日（0=当日）

  -- オシレーター
  rsi_14          numeric(6,2),             -- Wilder
  macd            numeric(14,4),            -- EMA12 - EMA26
  macd_signal     numeric(14,4),            -- EMA9 of macd
  macd_hist       numeric(14,4),            -- macd - signal
  stoch_k         numeric(6,2),             -- slow %K (14,3)
  stoch_d         numeric(6,2),             -- %D = SMA3 of %K

  -- ボリンジャーバンド(20, 2σ; 母集団σ)
  bb_percent_b    numeric(8,4),             -- (close - lower) / (upper - lower)
  bb_bandwidth    numeric(10,4),            -- (upper - lower) / mid

  -- ボラティリティ・出来高
  atr_14          numeric(14,4),            -- Wilder
  atr_pct         numeric(8,2),             -- atr_14 / close * 100
  vol_ratio_20    numeric(10,3),            -- 直近出来高 / 20日平均出来高

  -- 一目均衡表（現値と「雲」の位置関係）
  ichimoku_state  text check (ichimoku_state in ('above', 'inside', 'below')),

  updated_at      timestamptz not null default now(),

  primary key (as_of_date, local_code)
);

comment on table analytics.technical_metrics is
  '銘柄別テクニカル指標の日次スナップショット（最新 as_of_date のみ保持）。価格(equity_bar_daily adj系)から日次計算。';

create index if not exists idx_technical_metrics_code
  on analytics.technical_metrics (local_code, as_of_date desc);
create index if not exists idx_technical_metrics_date
  on analytics.technical_metrics (as_of_date);

-- RLS: authenticated は「公開日のマーカー」が指す as_of_date のみ可視（未確定の書き込み中行を隠す）。
--      service_role(バッチ)は RLS 迂回で全行 read/write 可。
alter table analytics.technical_metrics enable row level security;

create policy "authenticated_select_published"
  on analytics.technical_metrics
  for select to authenticated
  using (
    as_of_date = (select published_as_of_date from analytics.technical_publication where id)
  );

create policy "service_role_all"
  on analytics.technical_metrics
  for all to service_role using (true) with check (true);

grant select on analytics.technical_metrics to authenticated;
grant all on analytics.technical_metrics to service_role;
