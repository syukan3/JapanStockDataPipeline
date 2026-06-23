-- 00053_enable_rls_stock_metrics.sql
-- Supabase Security Advisor: rls_disabled_in_public (ERROR) の解消。
--
-- analytics は Exposed schema（PostgREST 公開）で、analytics.stock_metrics(00048) には
-- authenticated への GRANT SELECT があるのに RLS が無効のままだった。RLS 無効＝ポリシー無関係に
-- 全行が公開されるため、anon key 経由の認証ユーザーに全銘柄メトリクスが筒抜けになる。
-- 同居の technical_metrics/technical_publication(00050) は既に RLS 有効。本マイグレで stock_metrics を
-- 同じ方針に揃える: 「authenticated は SELECT 可（全行）／service_role は ALL」。
--
-- 可視性ゲート不要の理由: refresh_stock_metrics()(00049) は単一の INSERT ... ON CONFLICT で
--   原子的に更新するため、technical_metrics のような「書き込み途中の未確定 as_of_date 行」が
--   公開面に漏れる懸念がない（technical は TS バッチで複数回 upsert するため publication マーカーが必要）。
-- ビューへの影響なし: stock_screen / market_sector_weights は security_invoker=on のため、
--   authenticated が参照すると基底表 RLS が効くが、本ポリシー(using true)で全行可視のため挙動は不変。

alter table analytics.stock_metrics enable row level security;

-- 冪等化: CREATE POLICY に IF NOT EXISTS が無いため drop→create（部分適用環境でも再実行可）。
drop policy if exists "authenticated_select" on analytics.stock_metrics;
create policy "authenticated_select"
  on analytics.stock_metrics for select to authenticated using (true);

drop policy if exists "service_role_all" on analytics.stock_metrics;
create policy "service_role_all"
  on analytics.stock_metrics for all to service_role using (true) with check (true);

grant select on analytics.stock_metrics to authenticated;
grant all on analytics.stock_metrics to service_role;

-- ============================================================
-- 関連 WARN の解消: SECURITY DEFINER 関数の PUBLIC 実行権限を剥奪
-- ============================================================
-- refresh_stock_metrics() は SECURITY DEFINER（テーブル所有者権限で書き込み）かつ statement_timeout=180s の
-- 重い再計算。関数は既定で PUBLIC に EXECUTE が付くため、anon/authenticated（= anon key 所持者）が
-- /rest/v1/rpc/refresh_stock_metrics を叩いて再計算を強制できる悪用/DoS 経路になっていた。
-- 実行元は cron-a.yml の service_role 呼び出しのみ（00049 で service_role に明示 grant 済み）なので、
-- PUBLIC から剥奪しても正規経路に影響はない。
revoke execute on function analytics.refresh_stock_metrics(date) from public;
revoke execute on function analytics.refresh_stock_metrics(date) from anon;
revoke execute on function analytics.refresh_stock_metrics(date) from authenticated;
