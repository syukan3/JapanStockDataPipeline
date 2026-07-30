-- 00115_create_basket_score_daily.sql
-- テーマバスケット合成スコアの日次PIT事前計算テーブル + スコア定義レジストリ。
-- 計画書: docs/PLANS-basket-score-perf-2026-07.md（ルートリポ）§P1
--
-- 背景:
--   スコアのPIT履歴は現在リクエストのたびに約72万行から再計算している（/baskets 一覧は
--   13バスケット分）。これをDBに事前計算し、画面と S 系評価の共通の高速データ源にする。
--
-- 設計要点:
--   - basket_score_definitions: スコア定義レジストリ（1行/バージョン）。
--     **バージョン定義の単一ソースは Portfolio 側のコード**
--     （JapanStockPortfolio/lib/baskets/score-version.ts）。この表はその
--     「記述的ミラー + 検証対象」であり、DBだけからスコアを再現する用途には使わない
--     （再現はエンジン + engine_ref のコミットSHAで行う）。
--   - basket_score_daily: 1行/日/バスケット/バージョン。軸別スコアは短キーの jsonb
--     （detail文言は保存しない = 容量節約。表示側は basket_metrics 等から再構成可能）。
--
-- v1 の実体は「履歴4軸」:
--   bottomup（目標株価アップサイド）軸はPIT再構成不能のため履歴では常に除外され、
--   valuation35 / relative20 / creditflow15 / nav5 を再正規化して合成する。
--   当日ヘッドライン（5軸・detail文言つき）とは別契約であり、同一日の composite が
--   一致しないのは仕様（現行UIの既知の差）。パリティ検証は
--   (1) DB行 ↔ 履歴エンジン出力の完全一致、(2) ヘッドラインの4軸射影 ↔ DB当日行、の2本に分離する。
--
-- 書き込み経路:
--   マイグレーション所有は本リポ（DataPipeline）、**書き込み主体は Portfolio**（service_role）。
--   スコアエンジン（lib/baskets/score-history.ts）を import できるのは Portfolio だけであり、
--   本リポへ移植すると二重実装（乖離バグ源）になるため採らない。
--   日次バッチ: JapanStockPortfolio/scripts/cron/refresh-basket-score.mts
--   （GH Actions: JapanStockPortfolio/.github/workflows/refresh-basket-score.yml, JST 20:30）
--
-- 容量: 13バスケット × 約1,760営業日 × バージョン数 ≈ 2.3万行/バージョン（軽微）。
--
-- 注意: analytics は Exposed schema のため GRANT/RLS を明示する（00105 と同方針）。

-- ============================================================
-- basket_score_definitions（スコア定義レジストリ）
-- ============================================================
create table if not exists analytics.basket_score_definitions (
  definition_version smallint primary key,
  -- 例: 'v1: 履歴4軸(valuation/relative/creditflow/nav) 35/20/15/5再正規化・閾値65/35'
  description        text not null,
  -- 軸構成の記述的ミラー。例:
  -- {"axes":[{"key":"valuation","short_key":"v","weight":35,"in_history":true},
  --          {"key":"bottomup","short_key":"b","weight":25,"in_history":false}, ...]}
  -- 軸キーの一致・重み合計100 の検証はバッチ（--register-version）と vitest が担う
  -- （SQL の CHECK では過剰なため。計画書 §P1 ③）。
  axis_config        jsonb not null check (jsonb_typeof(axis_config) = 'object'),
  -- {"undervalued":65,"overvalued":35}
  label_thresholds   jsonb not null check (jsonb_typeof(label_thresholds) = 'object'),
  -- Portfolio 側 score-version.ts の定義名 + コミットSHA（必須）。例: 'basket-score-v1@1a2b3c4'
  engine_ref         text not null,
  created_at         timestamptz not null default now()
);

comment on table analytics.basket_score_definitions is
  'バスケット合成スコアの定義レジストリ（1行/バージョン）。正はPortfolio lib/baskets/score-version.ts で、本表は記述的ミラー+検証対象。';
comment on column analytics.basket_score_definitions.axis_config is
  '軸構成の記述的ミラー。in_history=false の軸は履歴合成から除外される（v1 の bottomup）。';
comment on column analytics.basket_score_definitions.engine_ref is
  'スコアを再現できる Portfolio 側エンジンの定義名+コミットSHA。DB単体では再現しない前提の追跡子。';

-- ============================================================
-- basket_score_daily（日次PITスコア）
-- ============================================================
create table if not exists analytics.basket_score_daily (
  basket_id          text not null references analytics.basket_definitions (basket_id),
  as_of_date         date not null,
  definition_version smallint not null
                     references analytics.basket_score_definitions (definition_version),
  -- 0-100（割安ほど高い）。全軸データ不足の日は null
  composite          numeric(5,1)
                     check (composite is null or (composite >= 0 and composite <= 100)),
  label              text not null
                     check (label in ('割安', '中立', '割高', 'データ不足')),
  -- 軸別スコアは数値のみ・短キーで保持（detail文言は保存しない → 容量節約）
  -- 例: {"v":62.1,"r":55.0,"b":null,"c":41.3,"n":50.0}
  axes               jsonb not null check (jsonb_typeof(axes) = 'object'),
  updated_at         timestamptz not null default now(),

  primary key (basket_id, definition_version, as_of_date),
  -- composite が出せない日は必ず 'データ不足'（逆も同様）
  check ((label = 'データ不足') = (composite is null))
);

comment on table analytics.basket_score_daily is
  'バスケット合成スコアの日次PIT履歴（1行/日/バスケット/バージョン）。書き込みは Portfolio の日次バッチ（service_role）。';
comment on column analytics.basket_score_daily.axes is
  '軸別スコアの短キーjsonb（v/r/b/c/n）。値は数値または null のみ。履歴再構成不能な軸（v1の b）は常に null。';
comment on column analytics.basket_score_daily.composite is
  '当該バージョン定義でのPIT合成スコア。当日ヘッドライン（5軸・detail付き）とは別契約で、一致は4軸射影同士でのみ検証する。';

-- 主キー (basket_id, definition_version, as_of_date) はバスケット別の時系列読み出しを賄う。
-- S1 のクロスセクション参照（日付断面で13バスケット横断）は先頭列が違うため別インデックスを張る。
create index if not exists idx_basket_score_daily_version_date
  on analytics.basket_score_daily (definition_version, as_of_date desc);

-- ============================================================
-- RLS / GRANT（00105 と同一: authenticated SELECT + service_role ALL）
-- ============================================================
alter table analytics.basket_score_definitions enable row level security;
alter table analytics.basket_score_daily       enable row level security;

drop policy if exists "authenticated_select" on analytics.basket_score_definitions;
create policy "authenticated_select"
  on analytics.basket_score_definitions for select to authenticated using (true);
drop policy if exists "service_role_all" on analytics.basket_score_definitions;
create policy "service_role_all"
  on analytics.basket_score_definitions for all to service_role using (true) with check (true);

drop policy if exists "authenticated_select" on analytics.basket_score_daily;
create policy "authenticated_select"
  on analytics.basket_score_daily for select to authenticated using (true);
drop policy if exists "service_role_all" on analytics.basket_score_daily;
create policy "service_role_all"
  on analytics.basket_score_daily for all to service_role using (true) with check (true);

grant select on analytics.basket_score_definitions, analytics.basket_score_daily to authenticated;
grant all    on analytics.basket_score_definitions, analytics.basket_score_daily to service_role;
