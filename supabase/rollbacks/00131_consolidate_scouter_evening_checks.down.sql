-- 00131_consolidate_scouter_evening_checks.down.sql
-- Scouter の夕方 7 本の集約（evening-checks.yml）を取り消し、7 本の個別 dispatch に戻す。
-- 切替時と同じく、旧・新どちらの run も実行中・待機中でない時間に行う（19:10〜20:45 JST を避ける）。
-- 先に Scouter 側で旧 7 本の workflow（dispatch_label input 付き）を main へ戻しておくこと。
-- 戻さずに適用すると、旧 dispatch が 404 で落ちる（reconcile_dispatches が通知する）。
-- 値は 00131 適用直前の本番（2026-09-30 に list_expected_workflows で確認）。

SELECT cron.unschedule('dispatch-evening-checks');

DELETE FROM ops.expected_workflows
 WHERE repo = 'JapanStockScouter'
   AND workflow_file = 'evening-checks.yml';

UPDATE ops.expected_workflows AS w
   SET schedule_utc = x.schedule_utc,
       deadline_jst = x.deadline_jst,
       notes = x.notes
  FROM (VALUES
    ('price-alert.yml',           '10 10 * * *',    '19:40', NULL),
    ('overheat.yml',              '40 10 * * *',    '20:40', 'メール有（保有・監視のステージ遷移時のみ・非対称トーン）。全銘柄スコアは overheat_snapshot（最新日のみ）'),
    ('earnings-alert.yml',        '0 11 * * *',     '20:30', NULL),
    ('entry-timing-signal.yml',   '10 11 * * 1-5',  '20:40', NULL),
    ('yutai-alert.yml',           '15 11 * * *',    '20:45', 'メール有・Cron E(優待更新)後'),
    ('holdings-news.yml',         '30 11 * * 1-5',  '21:00', 'メール有'),
    ('earnings-surprise.yml',     '45 11 * * *',    '21:15', NULL)
  ) AS x(workflow_file, schedule_utc, deadline_jst, notes)
 WHERE w.workflow_file = x.workflow_file
   AND w.repo = 'JapanStockScouter';

-- cron の schedule は manifest の schedule_utc と文字列完全一致させる（00125 の有効化条件）
SELECT cron.schedule('dispatch-price-alert',         '10 10 * * *',   $$ SELECT ops.dispatch_by_name('price-alert.yml') $$);
SELECT cron.schedule('dispatch-overheat',            '40 10 * * *',   $$ SELECT ops.dispatch_by_name('overheat.yml') $$);
SELECT cron.schedule('dispatch-earnings-alert',      '0 11 * * *',    $$ SELECT ops.dispatch_by_name('earnings-alert.yml') $$);
SELECT cron.schedule('dispatch-entry-timing-signal', '10 11 * * 1-5', $$ SELECT ops.dispatch_by_name('entry-timing-signal.yml') $$);
SELECT cron.schedule('dispatch-yutai-alert',         '15 11 * * *',   $$ SELECT ops.dispatch_by_name('yutai-alert.yml') $$);
SELECT cron.schedule('dispatch-holdings-news',       '30 11 * * 1-5', $$ SELECT ops.dispatch_by_name('holdings-news.yml') $$);
SELECT cron.schedule('dispatch-earnings-surprise',   '45 11 * * *',   $$ SELECT ops.dispatch_by_name('earnings-surprise.yml') $$);
