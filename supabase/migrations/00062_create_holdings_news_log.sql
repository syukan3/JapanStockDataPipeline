-- 00062_create_holdings_news_log.sql
-- 保有銘柄ニュース日次チェック(holdings-news)の通知済み開示ログ。
-- 開示ID(source + disclosure_id)ベースの重複排除に使い、送信状態をstatusで追跡する。
-- 詳細: JapanStockScouter/PLANS-holdings-news.md

CREATE TABLE scouter.holdings_news_log (
  id            bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  source        text NOT NULL CHECK (source IN ('tdnet', 'jquants')),
  disclosure_id text NOT NULL,          -- yanoshin: TDnet文書ID / jquants: disclosure_id
  local_code    text NOT NULL,
  title         text NOT NULL,
  disclosed_at  timestamptz,
  status        text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
  sent_at       timestamptz,
  error_message text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source, disclosure_id)
);

COMMENT ON TABLE scouter.holdings_news_log IS '保有銘柄ニュース(holdings-news)の通知済み開示ログ。開示ID(source+disclosure_id)ベースの重複排除に使用';
COMMENT ON COLUMN scouter.holdings_news_log.source IS '情報源: tdnet(yanoshin TDnet API) / jquants(jquants_core.financial_disclosure)';
COMMENT ON COLUMN scouter.holdings_news_log.disclosure_id IS 'tdnet: TDnet文書ID(yanoshin id) / jquants: financial_disclosure.disclosure_id';
COMMENT ON COLUMN scouter.holdings_news_log.status IS 'pending=DB記録済み未送信 / sent=送信成功 / failed=送信失敗(翌営業日に自動リトライ対象)';

CREATE INDEX idx_holdings_news_log_status ON scouter.holdings_news_log (status);
CREATE INDEX idx_holdings_news_log_local_code ON scouter.holdings_news_log (local_code, disclosed_at DESC);

-- ============================================================
-- 権限（本テーブルは保有銘柄コードを含む個人情報のため、scouterスキーマの他テーブルより厳格にする）
-- ============================================================

ALTER TABLE scouter.holdings_news_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON scouter.holdings_news_log
  FOR ALL TO service_role USING (true) WITH CHECK (true);

-- 00016 の「ALTER DEFAULT PRIVILEGES IN SCHEMA scouter GRANT SELECT ON TABLES TO authenticated」
-- により、本テーブル作成時点で authenticated へ自動的に SELECT が付与される。
-- 保有銘柄コードを含む個人情報のため、authenticated/anon には一切公開しないよう明示的に剥奪する。
REVOKE ALL ON scouter.holdings_news_log FROM anon, authenticated;
