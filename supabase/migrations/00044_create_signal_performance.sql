-- シグナル成績評価テーブル
-- 高配当スクリーニングのBUY/HOLDシグナル検出後のパフォーマンスを追跡

CREATE TABLE IF NOT EXISTS scouter.signal_performance (
  id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

  -- シグナル識別（screening テーブルとの対応）
  signal_date           DATE NOT NULL,
  local_code            TEXT NOT NULL,
  company_name          TEXT NOT NULL,
  recommendation        TEXT NOT NULL CHECK (recommendation IN ('BUY','HOLD')),
  composite_score       NUMERIC(8,2),
  macro_regime          TEXT CHECK (macro_regime IN ('favorable','neutral','caution','danger')),
  dividend_yield        NUMERIC(6,3),

  -- エントリー価格
  entry_price           NUMERIC(10,2) NOT NULL,
  topix_at_entry        NUMERIC(18,6) NOT NULL,

  -- 21営業日後（約1ヶ月）
  eval_date_21d         DATE,
  price_21d             NUMERIC(10,2),
  capital_return_21d    NUMERIC(10,6),
  topix_return_21d      NUMERIC(10,6),
  excess_return_21d     NUMERIC(10,6),

  -- 63営業日後（約3ヶ月）
  eval_date_63d         DATE,
  price_63d             NUMERIC(10,2),
  capital_return_63d    NUMERIC(10,6),
  topix_return_63d      NUMERIC(10,6),
  excess_return_63d     NUMERIC(10,6),

  -- 126営業日後（約6ヶ月）
  eval_date_126d        DATE,
  price_126d            NUMERIC(10,2),
  capital_return_126d   NUMERIC(10,6),
  topix_return_126d     NUMERIC(10,6),
  excess_return_126d    NUMERIC(10,6),

  -- 252営業日後（約1年）
  eval_date_252d        DATE,
  price_252d            NUMERIC(10,2),
  capital_return_252d   NUMERIC(10,6),
  topix_return_252d     NUMERIC(10,6),
  excess_return_252d    NUMERIC(10,6),

  -- インカムゲイン（実績配当）
  dividend_total        NUMERIC(10,2),
  income_return         NUMERIC(10,6),
  total_return          NUMERIC(10,6),

  -- メタ
  delisted              BOOLEAN NOT NULL DEFAULT FALSE,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (signal_date, local_code)
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_sp_recommendation
  ON scouter.signal_performance (recommendation, signal_date DESC);

CREATE INDEX IF NOT EXISTS idx_sp_signal_date
  ON scouter.signal_performance (signal_date DESC);

-- updated_at トリガー（既存の set_updated_at 関数を再利用）
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_sp_updated_at'
      AND tgrelid = 'scouter.signal_performance'::regclass
  ) THEN
    CREATE TRIGGER trg_sp_updated_at
      BEFORE UPDATE ON scouter.signal_performance
      FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
  END IF;
END
$$;

-- RLS
ALTER TABLE scouter.signal_performance ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter'
      AND tablename = 'signal_performance'
      AND policyname = 'authenticated_select'
  ) THEN
    CREATE POLICY "authenticated_select" ON scouter.signal_performance
      FOR SELECT TO authenticated USING (TRUE);
  END IF;
END
$$;

-- 権限付与
GRANT SELECT ON scouter.signal_performance TO authenticated;

-- コメント
COMMENT ON TABLE scouter.signal_performance IS 'シグナル検出後のパフォーマンス追跡（BUY/HOLDシグナル）';
COMMENT ON COLUMN scouter.signal_performance.signal_date IS 'スクリーニング実行日';
COMMENT ON COLUMN scouter.signal_performance.entry_price IS 'シグナル日のadj_close（分割調整済み）';
COMMENT ON COLUMN scouter.signal_performance.topix_at_entry IS 'シグナル日のTOPIX終値';
COMMENT ON COLUMN scouter.signal_performance.capital_return_21d IS '21営業日後のキャピタルリターン';
COMMENT ON COLUMN scouter.signal_performance.excess_return_21d IS '21営業日後の対TOPIX超過リターン';
COMMENT ON COLUMN scouter.signal_performance.dividend_total IS '保有期間中の受取配当合計（円/株）';
COMMENT ON COLUMN scouter.signal_performance.income_return IS 'インカムリターン = dividend_total / entry_price';
COMMENT ON COLUMN scouter.signal_performance.total_return IS 'トータルリターン = capital_return_252d + income_return';
COMMENT ON COLUMN scouter.signal_performance.delisted IS '評価期間中に上場廃止';
