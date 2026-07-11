-- 00088_add_tavily_search_provider.sql
-- Analyst Target Monitor の外部検索providerとしてTavilyを選択可能にする。
--
-- 00086で daily_request_limit / rolling_30d_request_limit を単一providerの
-- 固定値(35, 900)にCHECKしていたため、そのままでは2providerを共存できない。
-- provider名で分岐するCHECKへ差し替え、各providerが個別に持つ値を固定する。
--
-- Tavilyの実際の無料枠は本migration作成時点で未確認のため、Braveと同じ
-- 保守的な値(35/日, 900/実時間30日)を暫定値として設定する。実際のTavily契約の
-- 無料枠を確認したうえで、必要ならこのCHECKだけを対象にした別migrationで調整すること。

ALTER TABLE scouter.external_search_budget_guard
  DROP CONSTRAINT external_search_budget_guard_daily_limit_chk;
ALTER TABLE scouter.external_search_budget_guard
  DROP CONSTRAINT external_search_budget_guard_rolling_limit_chk;

ALTER TABLE scouter.external_search_budget_guard
  ADD CONSTRAINT external_search_budget_guard_daily_limit_chk
  CHECK (
    (provider = 'brave_search' AND daily_request_limit = 35)
    OR (provider = 'tavily' AND daily_request_limit = 35)
  );
ALTER TABLE scouter.external_search_budget_guard
  ADD CONSTRAINT external_search_budget_guard_rolling_limit_chk
  CHECK (
    (provider = 'brave_search' AND rolling_30d_request_limit = 900)
    OR (provider = 'tavily' AND rolling_30d_request_limit = 900)
  );

INSERT INTO scouter.external_search_budget_guard (
  provider,
  daily_request_limit,
  rolling_30d_request_limit
)
VALUES ('tavily', 35, 900)
ON CONFLICT (provider) DO NOTHING;
