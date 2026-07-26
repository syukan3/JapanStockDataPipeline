-- 00114_analyst_rating_history_rpc.sql の手動ロールバック。

BEGIN;

DROP FUNCTION IF EXISTS scouter.record_analyst_rating_history(
  uuid, uuid, date, text[], jsonb, integer
);

COMMIT;
