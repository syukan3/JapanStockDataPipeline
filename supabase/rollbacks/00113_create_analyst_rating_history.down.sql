-- 00113_create_analyst_rating_history.sql の手動ロールバック。
-- 収集履歴は再収集で復元できる（出典が直近3ヶ月ぶんを公開している範囲に限る）。

BEGIN;

DROP TABLE IF EXISTS scouter.analyst_rating_history;

COMMIT;
