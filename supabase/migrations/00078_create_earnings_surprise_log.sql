-- 00078_create_earnings_surprise_log.sql
-- 決算サプライズトラッカー(earnings-surprise)の評価・通知ログ。
-- 実績開示(disclosure_id)ベースの重複排除に使い、評価結果と送信状態をstatusで追跡する。
-- 詳細: docs/PLANS-price-earnings-diversification-2026-07.md 機能2
--
-- statusの分類（差分抽出の要）:
--   終端（再処理しない）: sent / evaluated_no_alert / no_baseline
--   再試行対象:           pending / failed
-- no_baseline を終端にするのは、対象開示より過去のガイダンスが後から現れることは
-- 時系列上あり得ないため（J-Quants訂正データ等での再処理は該当行のdelete→再実行で手動対応）。

CREATE TABLE scouter.earnings_surprise_log (
  id                bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  local_code        text NOT NULL,
  company_name      text,
  disclosure_id     text NOT NULL,        -- jquants_core.financial_disclosure の実績開示ID（重複排除キー）
  disclosed_date    date NOT NULL,        -- 実績開示のdisclosed_date（監査・期間絞り込み用）
  disclosed_time    time,                 -- 同上disclosed_time
  fiscal_year_end   text,                 -- financial_disclosure本体と型を合わせる（'YYYY-MM-DD'文字列）
  guidance_disclosure_id text,            -- 比較に使った直近ガイダンスの開示ID（追跡用。no_baselineはNULL）
  -- 分母（予想値）がごく小さいとサプライズ%は数万%になり得るため、8桁ではなく12桁で持つ
  surprise_op_pct   numeric(12,4),        -- (実績営業利益-予想)/|予想| * 100
  surprise_np_pct   numeric(12,4),
  surprise_eps_pct  numeric(12,4),
  representative_pct numeric(12,4),       -- 絶対値最大のサプライズ%（メール見出し・閾値判定用）
  status            text NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'sent', 'evaluated_no_alert', 'no_baseline', 'failed')),
  sent_at           timestamptz,
  error_message     text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (disclosure_id)
);

COMMENT ON TABLE scouter.earnings_surprise_log IS '決算サプライズ(earnings-surprise)の評価・通知ログ。disclosure_idベースの重複排除に使用';
COMMENT ON COLUMN scouter.earnings_surprise_log.status IS '終端: sent/evaluated_no_alert/no_baseline、再試行対象: pending/failed';
COMMENT ON COLUMN scouter.earnings_surprise_log.representative_pct IS 'op/np/epsサプライズ%のうち絶対値最大のもの。±10%閾値の判定に使用';

CREATE INDEX idx_earnings_surprise_log_status ON scouter.earnings_surprise_log (status);
CREATE INDEX idx_earnings_surprise_log_local_code ON scouter.earnings_surprise_log (local_code, disclosed_date DESC);

-- ============================================================
-- 権限（保有・監視銘柄コードを含む個人情報のため、earnings_alert_log と同方針で
--       service_role のみに限定する）
-- ============================================================

ALTER TABLE scouter.earnings_surprise_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON scouter.earnings_surprise_log
  FOR ALL TO service_role USING (true) WITH CHECK (true);

REVOKE ALL ON scouter.earnings_surprise_log FROM anon, authenticated;
