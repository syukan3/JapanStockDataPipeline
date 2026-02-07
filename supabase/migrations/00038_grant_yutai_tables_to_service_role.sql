-- service_role に優待クロス取引テーブルの権限を付与
-- Scouter (service_role key) からのアクセスに必要

GRANT ALL ON jquants_core.yutai_benefit TO service_role;
GRANT ALL ON jquants_core.margin_inventory TO service_role;
GRANT ALL ON scouter.yutai_cross_screening TO service_role;
