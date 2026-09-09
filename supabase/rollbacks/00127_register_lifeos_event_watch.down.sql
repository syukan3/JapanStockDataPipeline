-- 00127_register_lifeos_event_watch.down.sql
-- lifeOS EventWatch の外部トリガー登録を取り消す。
-- 取り消すと event-watch.yml は GitHub schedule 単独に戻る（= 遅延・欠測のリスクが復活）。
-- workflow 側の guard job は「外部トリガーの実行が見つからない」warning を出しつつ
-- 常に本処理を走らせる fail-open なので、この rollback 単独で検知は止まらない。

SELECT cron.unschedule('dispatch-lifeos-event-watch');

-- この migration が入れた 1 行だけを消す。repo='lifeOS' 一括だと 00126 の
-- 10 行まで巻き込んで消してしまう。
DELETE FROM ops.expected_workflows
 WHERE repo = 'lifeOS'
   AND workflow_file = 'event-watch.yml';
