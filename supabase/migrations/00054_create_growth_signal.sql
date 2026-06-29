-- 高勝率成長株シグナル（Kenmo改・4本柱 A/B/C）の出力テーブル
-- SPEC-growth-signal.md §9.5。RLS は authenticated SELECT ＋ service_role ALL（既存 scouter 方針）。
-- Scouter がローカル backtest.sqlite でライブ生成し、本テーブルに upsert する（フォワード/ペーパー検証）。

-- ============================================================
-- 00054-1: 日次ランキング結果
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.growth_signal_screening (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_date        DATE NOT NULL,
  local_code      TEXT NOT NULL,
  company_name    TEXT NOT NULL,
  method          TEXT NOT NULL CHECK (method IN ('A','B','C')),
  rank            INTEGER,
  composite_score NUMERIC(8,2),
  sub_scores      JSONB,                       -- {trend,earnings,liquidity,valuation,catalyst}
  close_price     NUMERIC(10,2),
  buy_zone_low    NUMERIC(10,2),
  buy_zone_high   NUMERIC(10,2),
  stop_price      NUMERIC(10,2),
  target_price    NUMERIC(10,2),
  next_earnings   DATE,
  regime_mode     TEXT,                        -- 攻め/通常/守り
  topix_regime    TEXT,                        -- 強気/普通/弱気/危険
  recommendation  TEXT NOT NULL,               -- 主力/買い/監視/見送り
  original_recommendation TEXT,                -- マクロ抑制前
  excluded        BOOLEAN NOT NULL DEFAULT FALSE,
  exclude_reason  TEXT,
  manual_check    BOOLEAN NOT NULL DEFAULT TRUE,
  comment         TEXT,
  ai_summary      TEXT,
  ai_confidence   INTEGER,                     -- 任意(Gemini)
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_date, local_code, method)        -- 同一銘柄が同日 A/B/C 複数該当を保存可
);

-- 上位提示用（excluded=FALSE のスコア順）。composite_score は nullable のため
-- インデックスとクエリ双方で NULLS LAST を明示し、上位取得に NULL が混ざらないようにする。
CREATE INDEX IF NOT EXISTS idx_gss_rank
  ON scouter.growth_signal_screening (run_date, composite_score DESC NULLS LAST) WHERE excluded = FALSE;
CREATE INDEX IF NOT EXISTS idx_gss_run_date
  ON scouter.growth_signal_screening (run_date DESC);

-- ============================================================
-- 00054-2: フォワード/ペーパー追跡（イベントドリブン専用・固定ホライズンでない）
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.growth_signal_trades (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  signal_date   DATE NOT NULL,
  local_code    TEXT NOT NULL,
  method        TEXT NOT NULL CHECK (method IN ('A','B','C')),
  entry_date    DATE,
  entry_price   NUMERIC(10,2),
  stop_price    NUMERIC(10,2),
  target_price  NUMERIC(10,2),
  exit_date     DATE,
  exit_price    NUMERIC(10,2),
  exit_reason   TEXT,
  gross_return  NUMERIC(10,6),
  net_return    NUMERIC(10,6),
  holding_days  INTEGER,
  delisted      BOOLEAN NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (signal_date, local_code, method)
);

CREATE INDEX IF NOT EXISTS idx_gst_open
  ON scouter.growth_signal_trades (signal_date DESC) WHERE exit_date IS NULL;

-- ============================================================
-- updated_at トリガー（既存 scouter.set_updated_at を再利用）
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_gss_updated_at'
      AND tgrelid = 'scouter.growth_signal_screening'::regclass
  ) THEN
    CREATE TRIGGER trg_gss_updated_at
      BEFORE UPDATE ON scouter.growth_signal_screening
      FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'trg_gst_updated_at'
      AND tgrelid = 'scouter.growth_signal_trades'::regclass
  ) THEN
    CREATE TRIGGER trg_gst_updated_at
      BEFORE UPDATE ON scouter.growth_signal_trades
      FOR EACH ROW EXECUTE FUNCTION scouter.set_updated_at();
  END IF;
END
$$;

-- ============================================================
-- RLS（authenticated SELECT ＋ service_role ALL）
-- ============================================================
ALTER TABLE scouter.growth_signal_screening ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.growth_signal_trades   ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter' AND tablename = 'growth_signal_screening'
      AND policyname = 'authenticated_select'
  ) THEN
    CREATE POLICY "authenticated_select" ON scouter.growth_signal_screening
      FOR SELECT TO authenticated USING (TRUE);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter' AND tablename = 'growth_signal_trades'
      AND policyname = 'authenticated_select'
  ) THEN
    CREATE POLICY "authenticated_select" ON scouter.growth_signal_trades
      FOR SELECT TO authenticated USING (TRUE);
  END IF;
END
$$;

GRANT SELECT ON scouter.growth_signal_screening TO authenticated;
GRANT SELECT ON scouter.growth_signal_trades   TO authenticated;

-- ============================================================
-- コメント
-- ============================================================
COMMENT ON TABLE scouter.growth_signal_screening IS '高勝率成長株シグナル 日次ランキング（手法A/B/C・フォワード検証）';
COMMENT ON TABLE scouter.growth_signal_trades IS '成長株シグナルのフォワード/ペーパー追跡（イベントドリブン・per-trade決済）';
COMMENT ON COLUMN scouter.growth_signal_screening.method IS 'A=新高値ブレイク×決算加速 / B=決算モメンタム / C=優待ランアップ';
COMMENT ON COLUMN scouter.growth_signal_screening.sub_scores IS 'スコア内訳 JSONB {trend,earnings,liquidity,valuation,catalyst}';
COMMENT ON COLUMN scouter.growth_signal_trades.exit_reason IS 'stop/target/trail/ma_exit/time/delisted';
COMMENT ON COLUMN scouter.growth_signal_trades.net_return IS 'コスト後リターン（小数・per-trade）';
