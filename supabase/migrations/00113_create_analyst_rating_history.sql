-- 証券会社レーティング履歴: 公開レーティング一覧ページから収集した会社別の
-- 目標株価・格付けを、そのまま時系列で閲覧できるようにする。
-- 計画書: docs/PLANS-analyst-target-monitor-2026-07.md（ルートリポ）§11
--
-- 設計の要点:
--   * portfolio.analyst_target_* は「人が出典を確認して確定させる追記履歴」で、
--     確認クリックを毎回必要とする。こちらは確認を要求しない**閲覧用の収集履歴**で、
--     判定にも中央値にも使わない。両者は別物として併存させる。
--   * 収集対象は公開情報そのものでユーザー固有ではないため user_id を持たず、
--     scouter スキーマに置く（Portfolio は service_role + cachedRef で読む）。
--   * PKは event_fingerprint（code/firm/価格/格付/公表日のsha256）。同じイベントを
--     何度収集しても1行に収束し、last_seen_at だけが動く。
--   * 保持期間は収集側（Scouter）が直近3ヶ月に絞る。ここでは制約しない。

CREATE TABLE IF NOT EXISTS scouter.analyst_rating_history (
  -- 収集元の同一イベント判定キー。candidates 側と同じ材料・同じ順序で作る
  event_fingerprint     TEXT PRIMARY KEY
    CHECK (event_fingerprint ~ '^[0-9a-f]{64}$'),
  local_code            TEXT NOT NULL CHECK (local_code ~ '^\d{5}$'),
  firm_key              TEXT NOT NULL
    CHECK (firm_key = lower(btrim(firm_key)) AND firm_key ~ '^[a-z0-9][a-z0-9._-]{0,99}$'),
  firm_name             TEXT NOT NULL
    CHECK (char_length(btrim(firm_name)) BETWEEN 1 AND 200),
  published_on          DATE NOT NULL,
  -- 一覧ページの「レーティング」列の表記をそのまま持つ（"買い継続" "Equal継続" 等）
  rating                TEXT CHECK (rating IS NULL OR char_length(btrim(rating)) BETWEEN 1 AND 200),
  previous_target_price NUMERIC(12,2) CHECK (previous_target_price IS NULL OR previous_target_price > 0),
  target_price          NUMERIC(12,2) NOT NULL CHECK (target_price > 0),
  source_url            TEXT NOT NULL
    CHECK (char_length(source_url) BETWEEN 9 AND 2048 AND source_url ~ '^https://'),
  source_domain         TEXT NOT NULL
    CHECK (source_domain = lower(source_domain) AND source_domain ~ '^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$'),
  first_seen_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 画面は銘柄ごとに公表日の新しい順で読む
CREATE INDEX IF NOT EXISTS idx_analyst_rating_history_code_published
  ON scouter.analyst_rating_history (local_code, published_on DESC);
-- 「会社ごとの最新1件」を出すため
CREATE INDEX IF NOT EXISTS idx_analyst_rating_history_code_firm_published
  ON scouter.analyst_rating_history (local_code, firm_key, published_on DESC);

-- ============================================================
-- RLS（プライベートパターン: service_role のみ。00110 と同方針）
-- 00016 のデフォルト権限が authenticated へ自動SELECTを付与するため REVOKE が必須。
-- ============================================================
ALTER TABLE scouter.analyst_rating_history ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'scouter'
      AND tablename = 'analyst_rating_history'
      AND policyname = 'service_role_all'
  ) THEN
    EXECUTE 'CREATE POLICY "service_role_all" ON scouter.analyst_rating_history FOR ALL TO service_role USING (true) WITH CHECK (true)';
  END IF;
END
$$;

REVOKE ALL ON scouter.analyst_rating_history FROM anon, authenticated;

COMMENT ON TABLE scouter.analyst_rating_history IS
  '公開レーティング一覧から収集した証券会社別の目標株価・格付け履歴。閲覧専用で、人の確認を経ないため中央値や判定には使わない';
COMMENT ON COLUMN scouter.analyst_rating_history.event_fingerprint IS
  'code/firm/目標株価/格付/公表日のsha256。portfolio.analyst_target_candidates と同じ材料で作る';
COMMENT ON COLUMN scouter.analyst_rating_history.last_seen_at IS
  '同一イベントを最後に収集できた時刻。出典から消えた行を見分けるために持つ';
