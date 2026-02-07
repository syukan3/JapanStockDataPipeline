-- 優待クロス取引用テーブル
-- yutai_benefit: 優待情報マスタ（kabuyutai.comから取得）
-- margin_inventory: 一般信用売り在庫（kabu.com CSV / kabu STATION API）
-- yutai_cross_screening: スクリーニング結果（Scouterから書き込み）

-- 1. 優待情報マスタ
CREATE TABLE jquants_core.yutai_benefit (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  local_code      TEXT NOT NULL,
  company_name    TEXT NOT NULL,
  min_shares      INTEGER NOT NULL,
  benefit_content TEXT NOT NULL,
  benefit_value   INTEGER,
  record_month    SMALLINT NOT NULL,
  record_day      TEXT DEFAULT 'end',
  category        TEXT,
  source          TEXT NOT NULL DEFAULT 'kabuyutai',
  fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (local_code, min_shares, record_month)
);

-- 2. 一般信用売り在庫
CREATE TABLE jquants_core.margin_inventory (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  local_code      TEXT NOT NULL,
  broker          TEXT NOT NULL DEFAULT 'esmart',
  inventory_date  DATE NOT NULL,
  inventory_qty   INTEGER,
  is_available    BOOLEAN NOT NULL,
  loan_type       TEXT NOT NULL DEFAULT 'general',
  loan_term       TEXT,
  premium_fee     NUMERIC(10,2),
  source          TEXT NOT NULL DEFAULT 'kabu_csv',
  fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (local_code, broker, inventory_date, loan_type)
);

-- 3. スクリーニング結果
CREATE TABLE scouter.yutai_cross_screening (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_date        DATE NOT NULL,
  local_code      TEXT NOT NULL,
  company_name    TEXT NOT NULL,

  benefit_content TEXT,
  benefit_value   INTEGER,
  min_shares      INTEGER NOT NULL,
  record_month    SMALLINT NOT NULL,

  close_price     NUMERIC(10,2),
  required_capital NUMERIC(14,2),
  lending_fee     NUMERIC(10,2),
  total_cost      NUMERIC(10,2),

  net_profit      NUMERIC(10,2),
  yield_pct       NUMERIC(6,3),
  score           NUMERIC(8,2),

  inventory_available BOOLEAN,
  inventory_qty   INTEGER,
  days_to_record  INTEGER,

  recommendation  TEXT NOT NULL CHECK (recommendation IN ('BUY','WATCH','SKIP')),
  skip_reason     TEXT,

  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_date, local_code, record_month)
);

-- インデックス
CREATE INDEX idx_yb_record_month ON jquants_core.yutai_benefit (record_month);
CREATE INDEX idx_mi_date_available ON jquants_core.margin_inventory (inventory_date, is_available) WHERE is_available = TRUE;
CREATE INDEX idx_ycs_recommendation ON scouter.yutai_cross_screening (run_date, recommendation) WHERE recommendation != 'SKIP';

-- RLS
ALTER TABLE jquants_core.yutai_benefit ENABLE ROW LEVEL SECURITY;
ALTER TABLE jquants_core.margin_inventory ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.yutai_cross_screening ENABLE ROW LEVEL SECURITY;

CREATE POLICY "authenticated_select" ON jquants_core.yutai_benefit FOR SELECT TO authenticated USING (TRUE);
CREATE POLICY "authenticated_select" ON jquants_core.margin_inventory FOR SELECT TO authenticated USING (TRUE);
CREATE POLICY "authenticated_select" ON scouter.yutai_cross_screening FOR SELECT TO authenticated USING (TRUE);

GRANT SELECT ON jquants_core.yutai_benefit TO authenticated;
GRANT SELECT ON jquants_core.margin_inventory TO authenticated;
GRANT SELECT ON scouter.yutai_cross_screening TO authenticated;

-- updated_atトリガー（既存のset_updated_at関数を再利用）
CREATE TRIGGER set_updated_at BEFORE UPDATE ON jquants_core.yutai_benefit
  FOR EACH ROW EXECUTE FUNCTION jquants_core.set_updated_at();
CREATE TRIGGER set_updated_at BEFORE UPDATE ON scouter.yutai_cross_screening
  FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
