-- 00112_add_nikkeiyosoku_rating_provider.sql の手動ロールバック。
-- nikkeiyosoku行にexternal_search_requestsからの参照(ON DELETE RESTRICT)が残っている場合、
-- DELETEはFK違反で失敗しトランザクション全体が中断される
-- (=実運用でnikkeiyosokuを使った予約履歴が既にあるならrollbackさせない、という安全側の挙動)。

BEGIN;

DELETE FROM scouter.external_search_budget_guard WHERE provider = 'nikkeiyosoku';

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

COMMIT;
