-- 00088_add_tavily_search_provider.sql の手動ロールバック。
-- tavily行にexternal_search_requestsからの参照(ON DELETE RESTRICT)が残っている場合、
-- DELETEはFK違反で失敗しトランザクション全体が中断される
-- (=実運用でtavilyを使った予約履歴が既にあるならrollbackさせない、という安全側の挙動)。

BEGIN;

DELETE FROM scouter.external_search_budget_guard WHERE provider = 'tavily';

ALTER TABLE scouter.external_search_budget_guard
  DROP CONSTRAINT external_search_budget_guard_daily_limit_chk;
ALTER TABLE scouter.external_search_budget_guard
  DROP CONSTRAINT external_search_budget_guard_rolling_limit_chk;

ALTER TABLE scouter.external_search_budget_guard
  ADD CONSTRAINT external_search_budget_guard_daily_limit_chk
  CHECK (daily_request_limit = 35);
ALTER TABLE scouter.external_search_budget_guard
  ADD CONSTRAINT external_search_budget_guard_rolling_limit_chk
  CHECK (rolling_30d_request_limit = 900);

COMMIT;
