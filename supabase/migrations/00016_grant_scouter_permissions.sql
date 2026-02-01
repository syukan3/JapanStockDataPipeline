-- scouter スキーマに service_role の権限を付与
GRANT USAGE ON SCHEMA scouter TO service_role;
GRANT ALL ON ALL TABLES IN SCHEMA scouter TO service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA scouter TO service_role;

-- 今後作成されるテーブル・シーケンスにもデフォルトで権限付与
ALTER DEFAULT PRIVILEGES IN SCHEMA scouter
  GRANT SELECT ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA scouter
  GRANT ALL ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA scouter
  GRANT ALL ON SEQUENCES TO service_role;
