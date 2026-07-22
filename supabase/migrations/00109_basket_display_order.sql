-- 00109_basket_display_order.sql
-- バスケット一覧の表示順を制御するための列を追加する。
-- 計画書: フィジカルAIバスケット(ETF 2638)追加 + バスケット一覧表示順制御（ルートリポ Issue2）
--
-- 内容:
--   analytics.basket_definitions に display_order 列を追加し、既存9バスケットへ表示順を設定する。
--   physical-ai-2638（display_order=30）は本マイグレーションでは触らない
--   （scripts/seed/basket-valuation.ts の basket_definitions upsert が投入する）。

-- ============================================================
-- analytics.basket_definitions 拡張
-- ============================================================
alter table analytics.basket_definitions
  add column if not exists display_order integer not null default 1000;

comment on column analytics.basket_definitions.display_order is
  '一覧表示順（小さいほど上）。既定1000。Portfolio側 getBasketDefinitions が display_order 昇順→basket_id 昇順で並べる。';

-- ============================================================
-- 既存バスケットの表示順
-- ============================================================
update analytics.basket_definitions set display_order = 10 where basket_id = 'nkscd-200a';
update analytics.basket_definitions set display_order = 20 where basket_id = 'topix33-banks-1615';
-- physical-ai-2638 = 30 は seed（scripts/seed/basket-valuation.ts）が投入するためここでは設定しない
update analytics.basket_definitions set display_order = 40 where basket_id = 'topix33-machinery-1624';
update analytics.basket_definitions set display_order = 50 where basket_id = 'topix33-pharma-1621';
update analytics.basket_definitions set display_order = 60 where basket_id = 'topix33-realestate-1633';
update analytics.basket_definitions set display_order = 70 where basket_id = 'topix33-retail-1630';
update analytics.basket_definitions set display_order = 80 where basket_id = 'topix33-transportequip-1622';
update analytics.basket_definitions set display_order = 90 where basket_id = 'topix33-utilities-1627';
update analytics.basket_definitions set display_order = 100 where basket_id = 'topix33-wholesale-1629';
