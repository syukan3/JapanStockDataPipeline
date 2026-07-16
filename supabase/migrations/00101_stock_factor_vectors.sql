-- 00101_stock_factor_vectors.sql
-- 類似銘柄検索（ファクタープロファイル近傍）の基盤。
-- docs/PLANS-supabase-pro-ai-2026-07.md §4 Issue A。
--
-- analytics.stock_screen（ファンダ×テクニカル結合ビュー、最新1行/銘柄）の13指標を
-- z-score正規化・重み付けした13次元ベクトルに変換し、pgvectorのコサイン距離で近傍探索する。
-- LLM/外部API不要の決定論計算（生成は scripts/cron/refresh-factor-vectors.ts）。
--
-- 実測（2026-07-16）: 対象約3,742銘柄。市場capがNULL、またはper/pbrが両方NULLの銘柄は
--   ベクトル表から除外（実装側で判定）。除外後は概ね3,600〜3,700行程度の見込み。

create extension if not exists vector with schema extensions;

-- ============================================================
-- stock_factor_vectors: 銘柄ごとの最新ファクターベクトル（スナップショット、最新1行のみ保持）
-- ============================================================
create table if not exists analytics.stock_factor_vectors (
  as_of_date  date not null,
  local_code  text primary key,
  embedding   extensions.vector(13) not null,
  coverage    smallint not null,  -- 13次元中、変換前の生値が非NULLだった次元数（データの薄さの目安）
  updated_at  timestamptz not null default now()
);

comment on table analytics.stock_factor_vectors is
  '銘柄別ファクターベクトル（analytics.stock_screen由来13次元、z-score正規化+重み付け）。最新 as_of_date のみ保持。類似銘柄検索(analytics.similar_stocks)の入力。';
comment on column analytics.stock_factor_vectors.embedding is
  '次元順序: [earnings_yield, log_pbr, dividend_yield, roe, log_mcap, value_pct, quality_pct, momentum_pct, dev_25, dev_200, rsi_14, atr_pct, vol_ratio_20]（scripts/cron/refresh-factor-vectors.tsのDIMENSIONS定数と厳密一致させること）';
comment on column analytics.stock_factor_vectors.coverage is
  '13次元中、winsorize/z-score変換前の生値（派生値）が非NULLだった次元数。低いほどNULL補完(=平均0)への依存が大きい。';

-- インデックス不要: 対象は約3,700行。pgvectorのexact scan（インデックス無し）で
--   コサイン距離の全件計算は数msオーダーで完了する規模であり、HNSW/IVFFlatの
--   構築・メンテナンスコスト（近似精度とのトレードオフ、再構築の運用）に見合わない。
--   数万〜数十万行規模に増えたら再検討する。

-- RLS: 00053パターン踏襲（authenticated SELECT全行 / service_role ALL / anonには付与しない）
alter table analytics.stock_factor_vectors enable row level security;

drop policy if exists "authenticated_select" on analytics.stock_factor_vectors;
create policy "authenticated_select"
  on analytics.stock_factor_vectors for select to authenticated using (true);

drop policy if exists "service_role_all" on analytics.stock_factor_vectors;
create policy "service_role_all"
  on analytics.stock_factor_vectors for all to service_role using (true) with check (true);

grant select on analytics.stock_factor_vectors to authenticated;
grant all on analytics.stock_factor_vectors to service_role;

-- ============================================================
-- similar_stocks: 指定銘柄に近い上位N件を返すRPC
-- ============================================================
-- security invoker: 呼び出しロール(authenticated/service_role)の権限で評価。
--   analytics.stock_screen は既に authenticated へ SELECT 付与済み(00051)、
--   service_role は00048のdefault privilegesにより両テーブルとも暗黙にALLを保持。
-- p_limit は 1〜50 にクランプ（無制限指定によるレスポンス肥大・濫用を防止）。
-- 対象銘柄がベクトル表に無い場合はエラーにせず0行を返す（cross joinが空になるため自然に成立）。
create or replace function analytics.similar_stocks(p_local_code text, p_limit int default 5)
returns table (
  local_code      text,
  similarity      double precision,
  as_of_date      date,
  sector17_code   text,
  sector17_name   text,
  market_cap      numeric,
  per             numeric,
  dividend_yield  numeric,
  total_score     numeric
)
language sql
stable
security invoker
set search_path = ''
as $$
  select
    v.local_code,
    1 - (v.embedding operator(extensions.<=>) q.embedding) as similarity,
    v.as_of_date,
    s.sector17_code,
    s.sector17_name,
    s.market_cap,
    s.per,
    s.dividend_yield,
    s.total_score
  from analytics.stock_factor_vectors v
  cross join (
    select embedding
    from analytics.stock_factor_vectors
    where local_code = p_local_code
  ) q
  left join analytics.stock_screen s on s.local_code = v.local_code
  where v.local_code <> p_local_code
  order by v.embedding operator(extensions.<=>) q.embedding asc
  limit greatest(1, least(coalesce(p_limit, 5), 50));
$$;

comment on function analytics.similar_stocks(text, int) is
  '指定銘柄(p_local_code)にファクタープロファイルが近い上位p_limit件をコサイン類似度降順で返す。ベクトル未生成の銘柄は0行。';

revoke execute on function analytics.similar_stocks(text, int) from public, anon;
grant execute on function analytics.similar_stocks(text, int) to authenticated, service_role;
