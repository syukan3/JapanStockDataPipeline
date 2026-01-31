-- 00001_create_schemas.sql
-- スキーマ作成: jquants_core (データ参照用), jquants_ingest (管理用)
--
-- 注意: Supabase Dashboard > Settings > API > API Exposed Schemas に
-- jquants_core, jquants_ingest を追加する必要あり
-- （または Supabase Management API で db_schema に追加）

create schema if not exists jquants_core;
create schema if not exists jquants_ingest;

comment on schema jquants_core is 'J-Quants API から取得したコアデータを格納するスキーマ';
comment on schema jquants_ingest is 'データ取り込みジョブの管理・監視用スキーマ';

-- スキーマへのアクセス権限を付与（最小権限原則: anonには付与しない）
grant usage on schema jquants_core to authenticated, service_role;
grant usage on schema jquants_ingest to service_role;

-- 今後作成されるテーブル・シーケンスにもデフォルトで権限付与
-- jquants_core: authenticated は SELECT のみ、service_role は ALL
alter default privileges in schema jquants_core
  grant select on tables to authenticated;
alter default privileges in schema jquants_core
  grant all on tables to service_role;
alter default privileges in schema jquants_core
  grant all on sequences to service_role;

-- jquants_ingest: service_role のみ
alter default privileges in schema jquants_ingest
  grant all on tables to service_role;
alter default privileges in schema jquants_ingest
  grant all on sequences to service_role;
