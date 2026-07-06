-- ファクターポートフォリオ・ペーパートレード（実資金は使わない紙上運用）の出力テーブル
-- PLANS-longonly-net.md（JapanStockScouter）§6 の推奨構成
-- （value+lowvol+payout, 四半期リバランス, buy0.1/hold0.4）を Scouter がローカル
-- backtest.sqlite でスコアリングし、本テーブルに upsert する。

-- ============================================================
-- 00066-1: 四半期リバランス毎の目標保有銘柄（買い/継続/売却の履歴スナップショット）
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.factor_portfolio_holdings (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  rebalance_date  DATE NOT NULL,
  local_code      TEXT NOT NULL,
  company_name    TEXT NOT NULL,
  factor_set      TEXT NOT NULL,              -- 例 'lowvol,payout,value'（ソート済みcsv）
  band_buy        NUMERIC(5,4) NOT NULL,       -- 例 0.1000
  band_hold       NUMERIC(5,4) NOT NULL,       -- 例 0.4000
  rank            INTEGER,                     -- その日の候補内順位（forced sell等でNULLもあり）
  composite_score NUMERIC(10,6),
  target_weight   NUMERIC(6,5) NOT NULL,       -- 等加重 1/N。sell行は0
  action          TEXT NOT NULL CHECK (action IN ('buy','hold','sell')),
  entry_price     NUMERIC(10,2),               -- 建玉の取得原価（holdはbuy時点から据え置き、sellも保持）
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (rebalance_date, local_code)
);

CREATE INDEX IF NOT EXISTS idx_fph_latest
  ON scouter.factor_portfolio_holdings (rebalance_date DESC);
CREATE INDEX IF NOT EXISTS idx_fph_active
  ON scouter.factor_portfolio_holdings (rebalance_date DESC, local_code) WHERE action IN ('buy','hold');

-- ============================================================
-- 00066-2: 保有銘柄の日次含み損益
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.factor_portfolio_daily_marks (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  as_of_date        DATE NOT NULL,
  local_code        TEXT NOT NULL,
  rebalance_date    DATE NOT NULL,             -- どの保有コホートに紐づくか（holdingsとのJOINキー）
  close_price       NUMERIC(10,2),
  unrealized_return NUMERIC(10,6),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (as_of_date, local_code)
);

CREATE INDEX IF NOT EXISTS idx_fpdm_latest
  ON scouter.factor_portfolio_daily_marks (as_of_date DESC);

-- ============================================================
-- updated_at トリガー（既存 scouter.set_updated_at を再利用）
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_fph_updated_at'
      AND tgrelid = 'scouter.factor_portfolio_holdings'::regclass
  ) THEN
    CREATE TRIGGER trg_fph_updated_at
      BEFORE UPDATE ON scouter.factor_portfolio_holdings
      FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_fpdm_updated_at'
      AND tgrelid = 'scouter.factor_portfolio_daily_marks'::regclass
  ) THEN
    CREATE TRIGGER trg_fpdm_updated_at
      BEFORE UPDATE ON scouter.factor_portfolio_daily_marks
      FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
  END IF;
END
$$;

-- ============================================================
-- RLS（authenticated SELECT ＋ service_role ALL、既存 scouter 方針）
-- ============================================================
ALTER TABLE scouter.factor_portfolio_holdings ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.factor_portfolio_daily_marks ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter' AND tablename = 'factor_portfolio_holdings'
      AND policyname = 'authenticated_select'
  ) THEN
    CREATE POLICY "authenticated_select" ON scouter.factor_portfolio_holdings
      FOR SELECT TO authenticated USING (TRUE);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter' AND tablename = 'factor_portfolio_daily_marks'
      AND policyname = 'authenticated_select'
  ) THEN
    CREATE POLICY "authenticated_select" ON scouter.factor_portfolio_daily_marks
      FOR SELECT TO authenticated USING (TRUE);
  END IF;
END
$$;

GRANT SELECT ON scouter.factor_portfolio_holdings TO authenticated;
GRANT SELECT ON scouter.factor_portfolio_daily_marks TO authenticated;

-- ============================================================
-- コメント
-- ============================================================
COMMENT ON TABLE scouter.factor_portfolio_holdings IS
  'ファクターポートフォリオ・ペーパートレード（実資金なし）の四半期リバランス保有スナップショット';
COMMENT ON TABLE scouter.factor_portfolio_daily_marks IS
  'ファクターポートフォリオ・ペーパートレードの日次含み損益（保有中銘柄のみ）';
COMMENT ON COLUMN scouter.factor_portfolio_holdings.action IS
  'buy=新規購入 / hold=継続保有 / sell=このリバランスで除外（entry_priceは取得原価を保持）';
COMMENT ON COLUMN scouter.factor_portfolio_daily_marks.rebalance_date IS
  'このマークがどのholdingsコホート（rebalance_date）に属するかを示すJOINキー';
