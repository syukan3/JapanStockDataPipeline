-- 00080_create_evaluation_failure_cases.sql
-- 失敗事例ライブラリ（Phase 4）。
-- 「confidence≥70 かつ方向逆転」だったAI評価（HD/macro）を post_mortem（LLM生成の事後診断）付きで
-- 蓄積し、類似局面（同sector・同macro_regime）の評価プロンプトへタグ一致で注入する
-- （メタ評価の集計視点を補完するケース記憶。pgvectorは使わずタグ一致検索で足りる規模）。
-- 詳細: JapanStockScouter/PLANS-llm-self-improvement.md §3 Phase 4

CREATE TABLE scouter.evaluation_failure_cases (
  id            BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  source        TEXT NOT NULL CHECK (source IN ('hd_ai', 'macro_ai')),
  eval_date     DATE NOT NULL,
  local_code    TEXT,                          -- macro_ai の事例は銘柄を持たないため NULL
  prediction    JSONB NOT NULL,                 -- {sentiment, confidence, summary}（評価時点の抜粋）
  outcome       JSONB NOT NULL,                 -- {horizon_days, realized_return, direction, direction_match}
  error_score   NUMERIC(8,4) NOT NULL,          -- confidence×外れ幅。抽出時の優先順位に使用
  context_tags  TEXT[] NOT NULL DEFAULT '{}',   -- 例: regime:caution, sector:銀行業, rsi:oversold
  post_mortem   TEXT NOT NULL,                  -- LLM生成の事後診断（300字以内。生成失敗時はinsertしない）
  model_id      TEXT NOT NULL,                  -- post_mortem を生成したモデルID
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE scouter.evaluation_failure_cases IS
  '失敗事例ライブラリ（Phase 4）。confidence≥70かつ方向逆転だったAI評価の事後診断。同sector/同regimeの評価時にタグ一致で最新事例を注入する';
COMMENT ON COLUMN scouter.evaluation_failure_cases.source IS 'hd_ai=高配当AI評価（BUY通過銘柄のみで選択バイアスあり） / macro_ai=マクロAI評価';
COMMENT ON COLUMN scouter.evaluation_failure_cases.local_code IS '対象銘柄コード。macro_ai は銘柄を持たないため NULL';
COMMENT ON COLUMN scouter.evaluation_failure_cases.prediction IS '評価時点の予測抜粋: {sentiment, confidence, summary}';
COMMENT ON COLUMN scouter.evaluation_failure_cases.outcome IS '実現結果: {horizon_days, realized_return, direction, direction_match}（direction_matchは常にfalse=失敗事例のため）';
COMMENT ON COLUMN scouter.evaluation_failure_cases.error_score IS 'confidence×|realized_return|×100。抽出候補の優先順位（降順）に使用';
COMMENT ON COLUMN scouter.evaluation_failure_cases.context_tags IS '機械的に付与するタグ（regime:xxx / sector:xxx / rsi:oversold|overbought）。LLM任せにせずコードで付与';
COMMENT ON COLUMN scouter.evaluation_failure_cases.post_mortem IS 'LLM生成の事後診断（何を見落としたか・次に何を確認すべきか。300字以内）';

-- 重複防止: (source, eval_date, local_code) の一意性。local_code は NULL を許容するため
-- 部分インデックス2本に分ける（NULLはUNIQUE制約上「区別される」ため素朴なUNIQUE(a,b,c)では
-- macro_ai の重複行を防げない。expression index(COALESCE)よりPostgREST upsertとの相性が良い
-- 通常の部分インデックスを採用）。
CREATE UNIQUE INDEX uq_evaluation_failure_cases_with_code
  ON scouter.evaluation_failure_cases (source, eval_date, local_code)
  WHERE local_code IS NOT NULL;

CREATE UNIQUE INDEX uq_evaluation_failure_cases_without_code
  ON scouter.evaluation_failure_cases (source, eval_date)
  WHERE local_code IS NULL;

-- 注入クエリ（source + 直近12ヶ月 + 新しい順）用のインデックス
CREATE INDEX idx_evaluation_failure_cases_recent
  ON scouter.evaluation_failure_cases (source, eval_date DESC);

-- タグ一致検索の絞り込み高速化（GIN。件数は年間数十〜数百件規模のため必須ではないが安価）
CREATE INDEX idx_evaluation_failure_cases_tags
  ON scouter.evaluation_failure_cases USING GIN (context_tags);

-- ============================================================
-- 権限（プライベートRLSパターン: service_role のみ。00062/00064/00070-00072 と同方針）
-- ============================================================

ALTER TABLE scouter.evaluation_failure_cases ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter' AND tablename = 'evaluation_failure_cases'
      AND policyname = 'service_role_all'
  ) THEN
    CREATE POLICY "service_role_all" ON scouter.evaluation_failure_cases
      FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END
$$;

-- 00016 の「ALTER DEFAULT PRIVILEGES IN SCHEMA scouter GRANT SELECT ON TABLES TO authenticated」
-- により、本テーブル作成時点で authenticated へ自動的に SELECT が付与される。
-- 内部運用データ（LLM生成物・銘柄コード含む）のため、authenticated/anon には公開しないよう
-- 明示的に剥奪する（Portfolio で表示する場合は service_role + cachedRef 経由で読む）。
REVOKE ALL ON scouter.evaluation_failure_cases FROM anon, authenticated;
