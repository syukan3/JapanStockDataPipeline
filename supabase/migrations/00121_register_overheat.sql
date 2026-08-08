-- 00121_register_overheat.sql
-- 過熱警戒モニターのバッチ（JapanStockScouter overheat.yml）の登録:
--   1. job_runs の job_name CHECK制約 … 'scouter-overheat' を追加
--      （未登録のままだと index.ts の job_runs insert がCHECK違反で失敗し、
--       冪等ガード alreadySucceededToday と鮮度監視が機能しなくなる）
--   2. 外部トリガー登録 … ops.expected_workflows + pg_cron dispatch
--      （未登録のままだと GitHub schedule 単独運用となり +1.5〜5h 遅延し得る。
--       新規ワークフローの登録漏れは既知の事故パターン＝00084 で4本まとめて是正した）
--
-- テーブル本体は 00120。計画書: docs/PLANS-overheat-monitor-2026-08.md（ルートリポ）§3 Issue 3
--
-- 実行時刻について:
--   JST 19:40 = UTC 10:40（毎日）。overheat.yml の schedule と一致させること。
--   計画書 §3 の本文は「JST 18:30」と書いているが、Cron A の起動が JST 18:40 なので
--   18:30 に走らせると当日の株価・テクニカルが入る前に判定してしまい、
--   スナップショットが常に1営業日古くなる。同§の括弧書き「Cron A のテクニカル公開後」
--   という意図に合わせ、Cron A の鮮度期限 19:10 の後・macro-regime(19:45) の前に置いた。

-- ============================================================
-- 1. job_runs の job_name CHECK制約に 'scouter-overheat' を追加
--    （既存慣習に倣い DROP → 全許可名を含めて ADD し直す。最新の全リストは
--     00111 を基準にし、末尾に新規名を足す）
-- ============================================================

ALTER TABLE jquants_ingest.job_runs
  DROP CONSTRAINT IF EXISTS job_runs_job_name_check;

ALTER TABLE jquants_ingest.job_runs
  ADD CONSTRAINT job_runs_job_name_check
  CHECK (job_name IN (
    'cron_a', 'cron_b', 'cron_c',
    'scouter-high-dividend', 'cron-d-macro',
    'scouter-macro-regime', 'scouter-macro-ai',
    'cron-e-yutai', 'scouter-yutai-cross',
    'db-archival', 'scouter-signal-performance',
    'scouter-growth-signal', 'scouter-macro-ai-meta',
    'scouter-holdings-news', 'scouter-earnings-alert',
    'scouter-factor-paper',
    'scouter-price-alert',
    'scouter-earnings-surprise',
    'scouter-analyst-target-monitor',
    'scouter-yutai-alert',
    'scouter-weekly-summary',
    'weekly-margin',
    'scouter-entry-timing-signal',
    'scouter-thesis-lens',
    'scouter-overheat'
  ));

-- ============================================================
-- 2. 外部トリガー登録（pg_cron → GitHub workflow_dispatch）
--    同名cronが既にあれば他migrationの所有物を上書きしないよう fail closed にする。
-- ============================================================

-- 他の migration が同名ジョブを所有していたら上書きしない（fail closed）。
-- ただし **自分が登録したジョブなら通す**: 無条件 RAISE にすると、この migration を
-- 正常に再適用しただけで全体が失敗する（既存慣習の DO ブロックはこの点が抜けていた）。
-- 自分のジョブかどうかは command が overheat.yml 向け dispatch かどうかで見分け、
-- 該当すれば cron.schedule の同名 upsert 挙動（スケジュール・コマンドを差し替える）に任せる。
DO $$
DECLARE
  existing_command TEXT;
BEGIN
  SELECT j.command INTO existing_command
  FROM cron.job j
  WHERE j.jobname = 'dispatch-overheat';

  IF FOUND AND existing_command NOT LIKE '%ops.dispatch_by_name(''overheat.yml'')%' THEN
    RAISE EXCEPTION
      'cron job dispatch-overheat already exists with a different command: %', existing_command;
  END IF;
END;
$$;

INSERT INTO ops.expected_workflows
  (workflow_file, repo, friendly_name, schedule_utc, kind, deadline_jst, job_name, enabled, notes)
VALUES
  -- deadline は起動 19:40 + タイムアウト上限 30分 + 余裕。全銘柄×約65営業日の株価を読むため
  -- 他のScouterバッチより所要時間が長い
  ('overheat.yml', 'JapanStockScouter', 'Overheat Monitor', '40 10 * * *', 'daily', '20:40', 'scouter-overheat', true,
   'メール有（保有・監視のステージ遷移時のみ・非対称トーン）。全銘柄スコアは overheat_snapshot（最新日のみ）')
ON CONFLICT (workflow_file) DO NOTHING;

SELECT cron.schedule(
  'dispatch-overheat',
  '40 10 * * *',
  $$ SELECT ops.dispatch_by_name('overheat.yml') $$
);
