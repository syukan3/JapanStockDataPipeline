-- 00064_create_earnings_alert_log.sql
-- 決算発表アラート(earnings-alert)の通知済みログ。
-- (announcement_date, local_code)ベースの重複排除に使い、送信状態をstatusで追跡する。
-- 詳細: JapanStockScouter/PLANS-event-calendar.md

CREATE TABLE scouter.earnings_alert_log (
  id                bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  announcement_date date NOT NULL,       -- jquants_core.earnings_calendar.announcement_date
  local_code        text NOT NULL,
  company_name      text,
  status            text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
  sent_at           timestamptz,
  error_message     text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (announcement_date, local_code)
);

COMMENT ON TABLE scouter.earnings_alert_log IS '決算発表アラート(earnings-alert)の通知済みログ。(announcement_date+local_code)ベースの重複排除に使用';
COMMENT ON COLUMN scouter.earnings_alert_log.status IS 'pending=DB記録済み未送信 / sent=送信成功 / failed=送信失敗(翌回に自動リトライ対象)';

CREATE INDEX idx_earnings_alert_log_status ON scouter.earnings_alert_log (status);

-- ============================================================
-- 権限（本テーブルは保有銘柄コードを含む個人情報のため、scouterスキーマの他テーブルより厳格にする）
-- ============================================================

ALTER TABLE scouter.earnings_alert_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON scouter.earnings_alert_log
  FOR ALL TO service_role USING (true) WITH CHECK (true);

-- 00016 の「ALTER DEFAULT PRIVILEGES IN SCHEMA scouter GRANT SELECT ON TABLES TO authenticated」
-- により、本テーブル作成時点で authenticated へ自動的に SELECT が付与される。
-- 保有銘柄コードを含む個人情報のため、authenticated/anon には一切公開しないよう明示的に剥奪する。
REVOKE ALL ON scouter.earnings_alert_log FROM anon, authenticated;
