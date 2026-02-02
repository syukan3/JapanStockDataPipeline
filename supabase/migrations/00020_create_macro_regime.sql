-- マクロ環境判定テーブル
CREATE TABLE scouter.macro_regime (
  eval_date       DATE PRIMARY KEY,
  regime          TEXT NOT NULL CHECK (regime IN ('favorable','neutral','caution','danger')),
  score           NUMERIC(5,2) NOT NULL CHECK (score BETWEEN 0 AND 100),
  jp_score        NUMERIC(5,2) NOT NULL CHECK (jp_score BETWEEN 0 AND 100),
  us_score        NUMERIC(5,2) NOT NULL CHECK (us_score BETWEEN 0 AND 100),
  linkage_score   NUMERIC(5,2) NOT NULL CHECK (linkage_score BETWEEN 0 AND 100),
  override_reason TEXT,
  details         JSONB NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- RLS
ALTER TABLE scouter.macro_regime ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.macro_regime FORCE ROW LEVEL SECURITY;
CREATE POLICY "authenticated_select" ON scouter.macro_regime
  FOR SELECT TO authenticated USING (TRUE);
GRANT SELECT ON scouter.macro_regime TO authenticated;

-- service_role にも全権限を付与（GitHub Actions から UPSERT するため）
GRANT ALL ON scouter.macro_regime TO service_role;

-- high_dividend_screening にマクロ関連カラムを追加
ALTER TABLE scouter.high_dividend_screening
  ADD COLUMN IF NOT EXISTS macro_regime TEXT CHECK (macro_regime IN ('favorable','neutral','caution','danger')),
  ADD COLUMN IF NOT EXISTS original_recommendation TEXT CHECK (original_recommendation IN ('BUY','HOLD','PASS'));
