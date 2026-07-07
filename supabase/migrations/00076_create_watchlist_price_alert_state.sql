-- 00076_create_watchlist_price_alert_state.sql
-- ウォッチリスト指値アラート(price-alert)のクロス判定状態。
-- 銘柄ごとの「前回どちら側にいたか(last_side)」を保持し、終値が目標株価を
-- またいだ(side反転)ときだけ通知する。イベントログ型(earnings_alert_log等)と違い
-- 1銘柄1行の状態テーブル。詳細: docs/PLANS-price-earnings-diversification-2026-07.md 機能1
--
-- 番号が00069から飛んでいるのは、並行作業(LLM自己改善)が00070〜00075を予約済みのため。

CREATE TABLE scouter.watchlist_price_alert_state (
  local_code       text PRIMARY KEY,
  target_price     numeric(12,2) NOT NULL,     -- 前回判定時点のtarget_price（変更検知用）
  -- watchlist_items.created_at のコピー（世代識別）。ウォッチリスト削除→再追加で行が
  -- 作り直されると created_at が変わることを利用し、同一目標株価でも世代交代を検知して
  -- baseline からやり直す（古い last_side との比較による誤アラート防止）
  watchlist_item_created_at timestamptz NOT NULL,
  last_side        text NOT NULL CHECK (last_side IN ('above', 'below')),
  last_close       numeric(12,2),
  last_checked_date date NOT NULL,
  last_alert_sent_at timestamptz,
  status           text NOT NULL DEFAULT 'baseline' CHECK (status IN ('baseline', 'sent', 'failed')),
  error_message    text,
  updated_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE scouter.watchlist_price_alert_state IS 'ウォッチリスト指値アラートのクロス判定状態(1銘柄1行)。side反転時のみ通知';
COMMENT ON COLUMN scouter.watchlist_price_alert_state.last_side IS 'above=終値>=目標株価 / below=終値<目標株価（前回判定時点）';
COMMENT ON COLUMN scouter.watchlist_price_alert_state.watchlist_item_created_at IS 'watchlist_items.created_atのコピー。削除→再追加(世代交代)の検知に使い、交代時はbaselineからやり直す';
COMMENT ON COLUMN scouter.watchlist_price_alert_state.status IS 'baseline=初期記録(通知なし) / sent=直近クロスを通知済み / failed=クロス検知したが送信失敗(翌回自動リトライ)';

-- ============================================================
-- 権限（監視銘柄コードを含む個人情報のため、earnings_alert_log/holdings_news_log と同方針で
--       service_role のみに限定する）
-- ============================================================

ALTER TABLE scouter.watchlist_price_alert_state ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON scouter.watchlist_price_alert_state
  FOR ALL TO service_role USING (true) WITH CHECK (true);

-- 00016 の ALTER DEFAULT PRIVILEGES により authenticated へ自動付与される SELECT を剥奪
REVOKE ALL ON scouter.watchlist_price_alert_state FROM anon, authenticated;
