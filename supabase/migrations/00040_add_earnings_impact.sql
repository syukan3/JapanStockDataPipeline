-- ai_evaluations に earnings_impact カラム追加
ALTER TABLE scouter.ai_evaluations
  ADD COLUMN IF NOT EXISTS earnings_impact TEXT
  CHECK (earnings_impact IN ('safe', 'warning', 'danger'));

COMMENT ON COLUMN scouter.ai_evaluations.earnings_impact
  IS '未反映決算の影響度判定（safe/warning/danger）';

-- high_dividend_screening に earnings_downgrade フラグ追加
ALTER TABLE scouter.high_dividend_screening
  ADD COLUMN IF NOT EXISTS earnings_downgrade BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN scouter.high_dividend_screening.earnings_downgrade
  IS 'AI判定による決算影響降格（danger→HOLD降格時にtrue）';
