-- 00120_create_overheat_tables.sql
-- 過熱警戒モニター（バンドワゴン警報）の記録テーブル4種 + 公開日マーカー1種。
-- 計画書: docs/PLANS-overheat-monitor-2026-08.md（ルートリポ）§2
--
-- 目的:
--   「今この銘柄は物語のどの章にいるか」（early / riding / climax / unwind）を日次で採点し、
--   クライマックス圏での**新規参入**に警告を出すための素材を保存する。
--   警告は非対称で「飛び乗るな（保有継続は可）」であり、売り推奨ではない。
--   本 migration は記録のためのスキーマだけを持ち、ステージ判定・閾値・ヒステリシスは
--   一切持たない（すべて Scouter 側のコードが正本。閾値は Issue 1 の較正で確定する）。
--
-- 書き込み経路:
--   JapanStockScouter `src/strategies/overheat/`（service_role・毎日 JST 19:40、
--   Cron A（18:40起動）のテクニカル公開後）。DataPipeline 側からは書き込まない（scouter スキーマの通例）。
--   データ源は既存DBのみ: jquants_core.equity_bar_daily / analytics.technical_metrics /
--   jquants_core.weekly_margin_interest。
--
-- 保持ポリシー（**SQLでは実装しない。すべてバッチ側の prune が担う**）:
--   * overheat_snapshot     … 最新 as_of_date のみ（実行末尾で古い as_of_date の行を delete）
--   * overheat_history      … 無期限。ただし**保有＋ウォッチ銘柄のみ**書くので増え続けない
--   * overheat_stage_event  … 1年
--   * overheat_buzz         … 30日
--   * overheat_publication  … 単一行なので保持の概念なし（上書きのみ）
--   保持を SQL 側（トリガ・pg_cron）に持たせないのは、書き込み主体の Scouter が
--   1トランザクションで「upsert → prune」を完結でき、DB側の削除機構と二重管理にならないため。
--
-- 原子的公開（**公開日マーカー方式**）:
--   Portfolio は **service_role で読む＝RLS を迂回する**ため、technical_metrics(00050) の
--   「公開日マーカー + authenticated RLS で未確定行を隠す」のうち *RLS で隠す部分* は効かない。
--   だがマーカーそのものは有効で、読み側が「マーカーが指す日付」で絞れば未確定行は見えない。
--
--   当初案の「読み側が max(as_of_date) で絞る」は**不十分**だった（Issue 3 のレビュー指摘）:
--   バッチは全銘柄を500行チャンクに割って upsert するため、最初のチャンクを書いた瞬間に
--   max(as_of_date) は新しい日付になる。読み側はそれを「その日の完成した結果」として受け取り、
--   数百銘柄しか無い部分ランキングを全体だと誤認する。ランキングは母集団が全てなので、
--   値が正しくても母集団が欠けていれば結論が変わる。
--
--   そこで scouter.overheat_publication（単一行マーカー）を置き、
--     書き手: 全チャンク upsert 完了 → published_as_of_date を単一行 upsert で切替 → prune
--     読み手: published_as_of_date を先に読み、その日付で overheat_snapshot を .eq() 絞り
--   とする。マーカー切替は1行の upsert なので原子的で、切り替わった時点では全銘柄が
--   新しい as_of_date になっている。
--
--   残る窓（設計上の割り切り）: overheat_snapshot は PK が local_code のみなので、
--   チャンク書き込み中は旧日付の行が順次上書きされて減っていく。この間マーカーは旧日付を
--   指したままなので、読み手には「旧日付の、母集団が減っていくランキング」が見える。
--   これを消すには単一トランザクションが要るが PostgREST では張れない。それでも
--   「書きかけの新しい日を完成品と誤認する」より軽い（旧日付だと分かる・数十秒で解消）。
--   マーカーが NULL / 行なしの環境では、読み側は max(as_of_date) にフォールバックしてよい。
--
-- local_code の形式について:
--   JPX は 2024年以降、4桁目に英字を含むコードを払い出している（例: キオクシア 285A → '285A0'）。
--   2026-08-07 時点の全上場銘柄 4,445 のうち 434 が英字入りで、`^\d{5}$` で縛ると
--   これらが丸ごと投入不能になる（過熱モニターが対象にしたい新規上場銘柄そのもの）。
--   そのため 00119 が確立した `^[0-9]{3}[0-9A-Z][0-9]$` を本 migration でも使う。
--   全 4,445 銘柄がこのパターンに収まることを実データで確認済み。

-- ============================================================
-- overheat_publication（公開済み日付マーカー・単一行）
-- analytics.technical_publication(00050) と同じ形。違いは「RLS で未確定行を隠す」用途では
-- なく、**読み側が絞り込みに使う日付そのもの**を提供する点（scouter は service_role 読み）。
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.overheat_publication (
  -- 単一行を強制する定番: PK が boolean で TRUE しか許さない
  id                   BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
  -- overheat_snapshot として公開済みの as_of_date。NULL は「まだ1度も公開していない」
  published_as_of_date DATE,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 行が無いと読み側が毎回フォールバックに落ちるので、NULL の初期行を1本置いておく
INSERT INTO scouter.overheat_publication (id, published_as_of_date)
  VALUES (TRUE, NULL)
  ON CONFLICT (id) DO NOTHING;

-- ============================================================
-- overheat_snapshot（全銘柄・最新日のみ）
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.overheat_snapshot (
  -- 全上場普通株を1行ずつ。日次で upsert し、対象から外れた銘柄（上場廃止・データ欠落）は
  -- 古い as_of_date のまま残るのでバッチ末尾の prune で消す
  local_code          TEXT PRIMARY KEY CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$'),
  as_of_date          DATE NOT NULL,

  -- 3軸スコア（0-100。クロスセクション percentile の加重平均）
  price_heat          NUMERIC(5,1) CHECK (price_heat  IS NULL OR (price_heat  BETWEEN 0 AND 100)),
  volume_heat         NUMERIC(5,1) CHECK (volume_heat IS NULL OR (volume_heat BETWEEN 0 AND 100)),
  -- 信用軸は weekly_margin_interest（データ1年のみ・較正検証なし）由来の補助情報。
  -- 同表に無い銘柄は NULL のままにする（0 で埋めない＝「過熱していない」と誤読させない）
  credit_heat         NUMERIC(5,1) CHECK (credit_heat IS NULL OR (credit_heat BETWEEN 0 AND 100)),
  -- price 0.4 + volume 0.4 + credit 0.2（credit が NULL の銘柄は price/volume を 0.5/0.5）
  total_heat          NUMERIC(5,1) CHECK (total_heat  IS NULL OR (total_heat  BETWEEN 0 AND 100)),

  stage               TEXT NOT NULL
                        CHECK (stage IN ('none','early','riding','climax','unwind')),
  -- 現ステージへ突入した日。ヒステリシスで維持している間は動かさない
  stage_since         DATE CHECK (stage_since IS NULL OR stage_since <= as_of_date),

  -- 売買代金の対自身60日中央値倍率。ランキングの並べ替えキー
  trading_value_ratio NUMERIC(8,2) CHECK (trading_value_ratio IS NULL OR trading_value_ratio >= 0),
  -- 60日売買代金中央値 >= 1億円。false は「買えない銘柄」としてUIで折りたたむ
  liquidity_ok        BOOLEAN NOT NULL,

  -- 内訳（dev_25 / rsi_14 / vol_ratio_20 / 信用系 / percentile 生値）。
  -- 「なぜこのスコアになったか」を後から検証するための監査用スナップショット
  components          JSONB CHECK (components IS NULL OR jsonb_typeof(components) = 'object'),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- overheat_history（保有＋ウォッチ銘柄のみ・無期限）
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.overheat_history (
  local_code   TEXT NOT NULL CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$'),
  as_of_date   DATE NOT NULL,
  price_heat   NUMERIC(5,1) CHECK (price_heat  IS NULL OR (price_heat  BETWEEN 0 AND 100)),
  volume_heat  NUMERIC(5,1) CHECK (volume_heat IS NULL OR (volume_heat BETWEEN 0 AND 100)),
  credit_heat  NUMERIC(5,1) CHECK (credit_heat IS NULL OR (credit_heat BETWEEN 0 AND 100)),
  total_heat   NUMERIC(5,1) CHECK (total_heat  IS NULL OR (total_heat  BETWEEN 0 AND 100)),
  -- snapshot と同じ語彙に固定する（片方だけ増えるとUIとメールで解釈がずれる）
  stage        TEXT NOT NULL
                 CHECK (stage IN ('none','early','riding','climax','unwind')),
  components   JSONB CHECK (components IS NULL OR jsonb_typeof(components) = 'object'),

  PRIMARY KEY (local_code, as_of_date)
);

-- ============================================================
-- overheat_stage_event（ステージ遷移イベント。メール dedup 兼監査）
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.overheat_stage_event (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  local_code  TEXT NOT NULL CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$'),
  as_of_date  DATE NOT NULL,
  stage_from  TEXT NOT NULL CHECK (stage_from IN ('none','early','riding','climax','unwind')),
  stage_to    TEXT NOT NULL CHECK (stage_to   IN ('none','early','riding','climax','unwind')),
  -- メール送信済みなら時刻。NULL は「遷移は起きたが通知対象外/未送信」
  notified_at TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- 同じ日の同じ遷移先を二重記録しない（= 再実行しても同じメールを送らない）
  UNIQUE (local_code, as_of_date, stage_to),
  -- 遷移イベントなので from = to はあり得ない
  CONSTRAINT overheat_stage_event_is_transition CHECK (stage_from <> stage_to)
);

-- ============================================================
-- overheat_buzz（LLM「なぜ盛り上がってるか」1行要約）
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.overheat_buzz (
  local_code TEXT NOT NULL CHECK (local_code ~ '^[0-9]{3}[0-9A-Z][0-9]$'),
  as_of_date DATE NOT NULL,
  -- 1行要約。上限500字は暴走出力（web検索結果の丸貼り）を弾くための安全弁であり、
  -- 表示上の行数制御ではない。バッチ側で切り詰めてから insert すること
  summary    TEXT NOT NULL CHECK (char_length(btrim(summary)) BETWEEN 1 AND 500),
  -- 生成に使ったモデルID（例: 'openai/gpt-5.6-luna'）。後からモデル差を追えるように持つ
  model      TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (local_code, as_of_date)
);

-- ============================================================
-- インデックス（計画書 §2 末尾の3本）
-- ============================================================

-- climax / unwind 銘柄の抽出（メール・週次サマリーの「climax 銘柄数」）
CREATE INDEX IF NOT EXISTS idx_overheat_snapshot_stage
  ON scouter.overheat_snapshot (stage);

-- ランキング本体（流動性フィルタ + 売買代金倍率の降順）。
-- NULLS LAST で作るのは、UIが欲しい並びが「倍率の高い順・不明は末尾」だから。
-- **ランキングクエリは必ず `ORDER BY trading_value_ratio DESC NULLS LAST` と書くこと**。
-- 既定の `DESC`（= NULLS FIRST）だとこのインデックスと順序が食い違い、ソートに落ちる
-- （ORDER BY の NULLS 指定とインデックス定義の不一致で全走査になった cron-a の事例と同型）。
CREATE INDEX IF NOT EXISTS idx_overheat_snapshot_ranking
  ON scouter.overheat_snapshot (liquidity_ok, trading_value_ratio DESC NULLS LAST);

-- 銘柄別の履歴ミニチャート（新しい順）。主キー (local_code, as_of_date) の後方スキャンでも
-- 賄えるが、`DESC NULLS LAST` 付きで書かれてもソートに落ちないよう明示的に張る
CREATE INDEX IF NOT EXISTS idx_overheat_history_code_date
  ON scouter.overheat_history (local_code, as_of_date DESC);

-- ============================================================
-- RLS（プライベートパターン: service_role のみ。00110/00113 と同方針）
-- Portfolio は service_role クライアント + cachedRef で読む（scouter は 2026-06-21 に
-- authenticated 権限を剥奪済み）。00016 のデフォルト権限が authenticated へ自動SELECTを
-- 付与するため REVOKE が必須。
-- ============================================================
ALTER TABLE scouter.overheat_publication  ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.overheat_snapshot    ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.overheat_history     ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.overheat_stage_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.overheat_buzz        ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'overheat_publication','overheat_snapshot','overheat_history','overheat_stage_event','overheat_buzz'
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'scouter' AND tablename = t AND policyname = 'service_role_all'
    ) THEN
      EXECUTE format(
        'CREATE POLICY "service_role_all" ON scouter.%I FOR ALL TO service_role USING (true) WITH CHECK (true)',
        t
      );
    END IF;
  END LOOP;
END
$$;

REVOKE ALL ON scouter.overheat_publication  FROM anon, authenticated;
REVOKE ALL ON scouter.overheat_snapshot    FROM anon, authenticated;
REVOKE ALL ON scouter.overheat_history     FROM anon, authenticated;
REVOKE ALL ON scouter.overheat_stage_event FROM anon, authenticated;
REVOKE ALL ON scouter.overheat_buzz        FROM anon, authenticated;

-- ============================================================
-- コメント
-- ============================================================
COMMENT ON TABLE scouter.overheat_publication IS
  '過熱スナップショットの公開日マーカー（単一行）。バッチが全銘柄 upsert 完了後にここを切り替え、読み側はこの日付で overheat_snapshot を絞る（書きかけの部分ランキングを完成品と誤認させないため）';
COMMENT ON COLUMN scouter.overheat_publication.published_as_of_date IS
  '公開済みの as_of_date。NULL は未公開（読み側は max(as_of_date) にフォールバックしてよい）';

COMMENT ON TABLE scouter.overheat_snapshot IS
  '過熱警戒モニターの全銘柄スナップショット（1銘柄1行・最新 as_of_date のみ保持）。旧日の残骸はバッチ末尾の prune で削除する';
COMMENT ON COLUMN scouter.overheat_snapshot.credit_heat IS
  '信用軸(0-100)。weekly_margin_interest 由来の補助情報で較正検証なし。同表に無い銘柄はNULL（0で埋めない）';
COMMENT ON COLUMN scouter.overheat_snapshot.total_heat IS
  '総合(0-100)。price0.4+volume0.4+credit0.2。credit_heat がNULLの銘柄は price/volume を 0.5/0.5 で再正規化';
COMMENT ON COLUMN scouter.overheat_snapshot.stage IS
  'early=初動 / riding=バンドワゴン走行中 / climax=新規に飛び乗るな圏 / unwind=振り落とし / none=該当なし。climax は「売れ」ではない（警告は非対称）';
COMMENT ON COLUMN scouter.overheat_snapshot.stage_since IS
  '現ステージへ突入した日。ヒステリシスで維持している間は動かさない（ばたつき防止）';
COMMENT ON COLUMN scouter.overheat_snapshot.trading_value_ratio IS
  '売買代金の対自身60日中央値倍率。ランキングの並べ替えキー。ORDER BY は DESC NULLS LAST で書く（インデックス定義と一致させるため）';
COMMENT ON COLUMN scouter.overheat_snapshot.liquidity_ok IS
  '60日売買代金中央値 >= 1億円。false はランキング本体から外して折りたたむ（買えない銘柄を上位に出さない）';
COMMENT ON COLUMN scouter.overheat_snapshot.components IS
  'スコア内訳の監査用スナップショット（dev_25 / rsi_14 / vol_ratio_20 / 信用系 / percentile 生値）';

COMMENT ON TABLE scouter.overheat_history IS
  '過熱スコアの履歴。**保有＋ウォッチ銘柄のみ**（全銘柄を残すと容量が持たない）。無期限保持で銘柄詳細の履歴ミニチャートに使う';

COMMENT ON TABLE scouter.overheat_stage_event IS
  'ステージ遷移イベント。メールの重複送信を防ぐ dedup ログ兼監査証跡。保持1年（バッチ側 prune）';
COMMENT ON COLUMN scouter.overheat_stage_event.notified_at IS
  'メール送信済みなら時刻。NULLは通知対象外（保有/ウォッチ外）または未送信。再実行時の二重送信防止はUNIQUE制約とこの列で判定する';

COMMENT ON TABLE scouter.overheat_buzz IS
  'LLMによる「なぜ盛り上がってるか」1行要約（ランキング上位10銘柄のみ）。保持30日（バッチ側 prune）';
COMMENT ON COLUMN scouter.overheat_buzz.summary IS
  '1行要約。500字上限は暴走出力を弾く安全弁で、表示上の制御ではない（バッチ側で切り詰めてから insert する）';
COMMENT ON COLUMN scouter.overheat_buzz.model IS
  '生成に使ったモデルID（例: openai/gpt-5.6-luna）。モデル切替前後の質の差を後から追えるように持つ';
