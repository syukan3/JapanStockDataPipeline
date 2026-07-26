-- 00112_add_nikkeiyosoku_rating_provider.sql
-- Analyst Target Monitor の収集元に、証券会社別レーティング一覧ページ(投資の森)を追加する。
--
-- 背景: 検索API(Brave/Tavily)のスニペットだけを決定論パースする方式では、
-- 2026-07-13の実測で searched=2 / candidates=0 と候補がまったく得られなかった。
-- 一方で銘柄別レーティング一覧ページは「日付・証券会社・レーティング・目標株価」を
-- 表形式で公開しており、1銘柄1 requestで会社別の最新値を取得できる。
--
-- 予算ガードの扱いは既存providerと同じ。1銘柄1 requestのhard ceilingを
-- 日次35件・実時間30日900件に固定し、reserve/complete RPCの予約ledgerで律速する。

ALTER TABLE scouter.external_search_budget_guard
  DROP CONSTRAINT external_search_budget_guard_daily_limit_chk;
ALTER TABLE scouter.external_search_budget_guard
  DROP CONSTRAINT external_search_budget_guard_rolling_limit_chk;

ALTER TABLE scouter.external_search_budget_guard
  ADD CONSTRAINT external_search_budget_guard_daily_limit_chk
  CHECK (
    (provider = 'brave_search' AND daily_request_limit = 35)
    OR (provider = 'tavily' AND daily_request_limit = 35)
    OR (provider = 'nikkeiyosoku' AND daily_request_limit = 35)
  );
ALTER TABLE scouter.external_search_budget_guard
  ADD CONSTRAINT external_search_budget_guard_rolling_limit_chk
  CHECK (
    (provider = 'brave_search' AND rolling_30d_request_limit = 900)
    OR (provider = 'tavily' AND rolling_30d_request_limit = 900)
    OR (provider = 'nikkeiyosoku' AND rolling_30d_request_limit = 900)
  );

INSERT INTO scouter.external_search_budget_guard (
  provider,
  daily_request_limit,
  rolling_30d_request_limit
)
VALUES ('nikkeiyosoku', 35, 900)
ON CONFLICT (provider) DO NOTHING;
