-- 00122_add_overheat_publication.sql
-- scouter.overheat_publication（公開日マーカー）の補完適用。
--
-- 経緯: 00120 は当初「テーブル4種」で本番適用された（2026-08-08夜、ユーザー実行）。
-- その直後の codex-review 指摘で 00120 のファイルに overheat_publication を追記したが、
-- 本番の移行履歴には旧版 00120 が記録済みのため、後からの再 push では 00120 が
-- スキップされマーカーテーブルだけが本番に存在しない状態になった（PostgREST 404）。
-- ローカル/新環境では 00120 が全5テーブルを作るため、本 migration の全文が
-- IF NOT EXISTS / 条件付きで冪等に流せることが必須（二重適用しても無害）。
--
-- 内容は 00120 のマーカー関連部分と同一。役割・設計理由は 00120 ヘッダー
-- 「原子的公開（公開マーカー方式）」の節を参照。

CREATE TABLE IF NOT EXISTS scouter.overheat_publication (
  -- 単一行を強制する定番: PK が boolean で TRUE しか許さない
  id                   BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
  -- overheat_snapshot として公開済みの as_of_date。NULL は「まだ1度も公開していない」
  published_as_of_date DATE,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 行が無いと読み側が毎回フォールバックに落ちるので、NULL の初期行を1本置いておく
INSERT INTO scouter.overheat_publication (id, published_as_of_date)
  VALUES (TRUE, NULL)
  ON CONFLICT (id) DO NOTHING;

-- RLS（プライベートパターン: service_role のみ。00120 と同方針）
ALTER TABLE scouter.overheat_publication ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter'
      AND tablename = 'overheat_publication'
      AND policyname = 'service_role_all'
  ) THEN
    CREATE POLICY "service_role_all" ON scouter.overheat_publication
      FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END
$$;

REVOKE ALL ON scouter.overheat_publication FROM anon, authenticated;

COMMENT ON TABLE scouter.overheat_publication IS
  '過熱スナップショットの公開日マーカー（単一行）。バッチが全銘柄 upsert 完了後にここを切り替え、読み側はこの日付で overheat_snapshot を絞る（書きかけの部分ランキングを完成品と誤認させないため）';
COMMENT ON COLUMN scouter.overheat_publication.published_as_of_date IS
  '公開済みの as_of_date。NULL は未公開（読み側は max(as_of_date) にフォールバックしてよい）';
