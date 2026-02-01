-- nakayama_score → composite_score にリネーム
ALTER TABLE scouter.high_dividend_screening
  RENAME COLUMN nakayama_score TO composite_score;

-- インデックス再作成（カラム名変更に伴い）
DROP INDEX IF EXISTS scouter.idx_hds_score;
CREATE INDEX idx_hds_score
  ON scouter.high_dividend_screening (run_date, composite_score DESC)
  WHERE excluded = FALSE;
