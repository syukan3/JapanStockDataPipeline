-- 論者レンズ（Thesis Lens）: 著名人の見立てを検証可能な命題として保存し、PITで採点する
-- 計画書: docs/PLANS-thesis-lens-2026-07.md（ルートリポ）
--
-- 設計の要点:
--   * 人物の発言そのもの（原文）は保存しない。著作物であり、また「なりきり生成」を封じるため。
--     保存するのは自分の言葉に抽象化した命題（claim）と、それを測る観測軸（observables）だけ。
--   * falsifier（反証条件）を持てない命題は is_measurable=false として採点対象外にする
--     （例: 水ショック→資源戦争。観測軸を保有していない）。
--   * 命題は合成スコアにまとめない。thesis_observations は命題ごとに1行で、
--     レンズ単位の総合スコア列を意図的に持たない（順風と逆風の併存自体が情報）。
--   * measure（price/earnings/both）を必須にする。価格と実績利益は逆の答えを出すことがある
--     （実測: 200A は1年で価格+170%・実績利益-2.4%）。
--   * observations.is_forward で後ろ向き検証と前向きトラックレコードを分離する。
--     lens の pit_anchor（見立てを聞いた日）より前の採点は予測力の証拠にならない。
--
-- 判定への影響: なし。Portfolio 側は注釈としてのみ表示し、stock-advisor の decision を
-- 一切変更しない（PLANS-stock-advisor.md 不変条件 4/7）。

-- ============================================================
-- thesis_lenses（論者）
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.thesis_lenses (
  lens_id         TEXT PRIMARY KEY,
  display_name    TEXT NOT NULL,
  -- 論者の既知の偏り。permabear は水準では常時ONになるため採点で補正する
  bias            TEXT,
  bias_correction TEXT,
  -- 出典。原文は保存せず、参照先のパスと日付だけを持つ
  source_ref      TEXT,
  source_date     DATE,
  -- 前向きトラックレコードの起点（この見立てを聞いた日）
  pit_anchor      DATE NOT NULL,
  notes           TEXT,
  display_order   INTEGER NOT NULL DEFAULT 1000,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- thesis_definitions（命題）
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.thesis_definitions (
  thesis_id     TEXT PRIMARY KEY,
  lens_id       TEXT NOT NULL REFERENCES scouter.thesis_lenses (lens_id) ON DELETE CASCADE,
  claim         TEXT NOT NULL,
  kind          TEXT NOT NULL CHECK (kind IN ('level','propagation')),
  horizon       TEXT NOT NULL CHECK (horizon IN ('months','1-3y','structural')),
  implication   TEXT,
  -- 反証条件。is_measurable=true なら NOT NULL 相当（下のCHECKで強制）
  falsifier     TEXT,
  -- 観測軸の配列。要素は {ref, kind, region, role} 形式:
  --   ref    = 'basket:nkscd-200a' | 'macro:T10YIE' | 'market:pct_above_sma25'
  --   region = 'jp' | 'us' | 'global'（命題の対象地域と一致しない系列は role='secondary' のみ許す）
  --   role   = 'primary' | 'secondary'
  observables   JSONB NOT NULL DEFAULT '[]'::jsonb,
  -- propagation 命題のみ。[{stage, name, test, observables}]
  stages        JSONB,
  measure       TEXT CHECK (measure IN ('price','earnings','both')),
  conviction    TEXT NOT NULL CHECK (conviction IN ('high','medium','low')),
  -- 採点の閾値・窓幅など決定論パラメータ（採点コードが読む）
  scoring       JSONB NOT NULL DEFAULT '{}'::jsonb,
  is_measurable BOOLEAN NOT NULL DEFAULT TRUE,
  note          TEXT,
  display_order INTEGER NOT NULL DEFAULT 1000,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- 採点対象なら falsifier / measure / 観測軸が揃っていることを DB で強制する。
  -- 「反証条件を書けない命題を自動採点にかけない」という設計上の核をここで担保する。
  -- 空文字の falsifier や ref を持たない観測軸は「揃っている」と見なさない
  -- （NULL でなければ通る緩い制約だと、実質書かれていない反証条件で採点対象になれてしまう）。
  CONSTRAINT thesis_measurable_requires_falsifier CHECK (
    NOT is_measurable
    OR (
      falsifier IS NOT NULL
      AND btrim(falsifier) <> ''
      AND measure IS NOT NULL
      AND jsonb_typeof(observables) = 'array'
      AND jsonb_array_length(observables) > 0
      -- 全要素が「空白以外を含む文字列」の ref を持つこと
      -- （数値や true、空白のみの文字列を ref として通さない）
      AND jsonb_array_length(
            jsonb_path_query_array(
              observables,
              '$[*] ? (@.ref.type() == "string" && @.ref like_regex "\\S")'
            )
          ) = jsonb_array_length(observables)
    )
  ),
  -- observables は常に配列（is_measurable=false でも型は崩さない）
  CONSTRAINT thesis_observables_is_array CHECK (jsonb_typeof(observables) = 'array'),
  -- propagation 命題は stages を必ず持つ。空配列や stage 番号なしは不可
  CONSTRAINT thesis_propagation_requires_stages CHECK (
    kind <> 'propagation'
    OR (
      stages IS NOT NULL
      AND jsonb_typeof(stages) = 'array'
      AND jsonb_array_length(stages) > 0
      -- 全要素が 0 以上の整数の stage 番号を持つこと（負数・小数を通さない）
      AND jsonb_array_length(
            jsonb_path_query_array(
              stages,
              '$[*] ? (@.stage.type() == "number" && @.stage >= 0 && @.stage == @.stage.floor())'
            )
          ) = jsonb_array_length(stages)
    )
  )
);

CREATE INDEX IF NOT EXISTS idx_thesis_definitions_lens
  ON scouter.thesis_definitions (lens_id, display_order);

-- ============================================================
-- thesis_observations（採点履歴・PIT）
-- ============================================================
CREATE TABLE IF NOT EXISTS scouter.thesis_observations (
  thesis_id       TEXT NOT NULL REFERENCES scouter.thesis_definitions (thesis_id) ON DELETE CASCADE,
  eval_date       DATE NOT NULL,
  -- lens_id は thesis 経由で辿れるが、レンズ単位の取得を1クエリで済ませるため非正規化して持つ
  lens_id         TEXT NOT NULL REFERENCES scouter.thesis_lenses (lens_id) ON DELETE CASCADE,
  verdict         TEXT NOT NULL
                    CHECK (verdict IN ('tailwind','neutral','headwind','not_fired','unmeasurable')),
  -- measure='both' の命題は価格と利益で別の答えになり得るため両方を残す
  price_verdict   TEXT CHECK (price_verdict IN ('tailwind','neutral','headwind','not_fired')),
  earnings_verdict TEXT CHECK (earnings_verdict IN ('tailwind','neutral','headwind','not_fired')),
  -- propagation 命題の到達段階
  stage           INTEGER CHECK (stage IS NULL OR stage >= 0),
  falsifier_state TEXT NOT NULL DEFAULT 'not_evaluable'
                    CHECK (falsifier_state IN ('intact','approaching','triggered','not_evaluable')),
  -- 採点に使った観測値の実測スナップショット（監査用。何を見てこの verdict にしたか）
  observations    JSONB NOT NULL DEFAULT '{}'::jsonb,
  -- eval_date >= lens.pit_anchor。後ろ向き検証を前向き実績と混ぜないためのフラグ
  is_forward      BOOLEAN NOT NULL,
  scorer_version  TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (thesis_id, eval_date)
);

CREATE INDEX IF NOT EXISTS idx_thesis_observations_lens_date
  ON scouter.thesis_observations (lens_id, eval_date DESC);
CREATE INDEX IF NOT EXISTS idx_thesis_observations_forward
  ON scouter.thesis_observations (lens_id, eval_date DESC) WHERE is_forward;

-- ============================================================
-- RLS（プライベートパターン: service_role のみ。00080/00081/00082 と同方針）
-- Portfolio は service_role クライアント + cachedRef で読む（scouter は 2026-06-21 に
-- authenticated 権限を剥奪済み）。00016 のデフォルト権限が authenticated へ自動SELECTを
-- 付与するため REVOKE が必須。
-- ============================================================
ALTER TABLE scouter.thesis_lenses      ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.thesis_definitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE scouter.thesis_observations ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['thesis_lenses','thesis_definitions','thesis_observations'] LOOP
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

REVOKE ALL ON scouter.thesis_lenses      FROM anon, authenticated;
REVOKE ALL ON scouter.thesis_definitions FROM anon, authenticated;
REVOKE ALL ON scouter.thesis_observations FROM anon, authenticated;

-- ============================================================
-- コメント
-- ============================================================
COMMENT ON TABLE scouter.thesis_lenses IS
  '論者レンズ。著名人の見立てを検証可能な命題群として保存する単位。原文は保存せず出典参照のみ持つ';
COMMENT ON COLUMN scouter.thesis_lenses.bias IS
  '論者の既知の偏り（例: permabear-inflation）。水準では警戒シグナルが常時ONになるため採点で補正する';
COMMENT ON COLUMN scouter.thesis_lenses.pit_anchor IS
  '前向きトラックレコードの起点日（見立てを聞いた日）。これより前の採点は予測力の証拠にならない';
COMMENT ON COLUMN scouter.thesis_lenses.source_ref IS
  '出典の参照先パスのみ（著作物の原文・PDFは保存しない）';

COMMENT ON TABLE scouter.thesis_definitions IS
  '命題。1論者にN件。falsifier を持てない命題は is_measurable=false で採点対象外にする';
COMMENT ON COLUMN scouter.thesis_definitions.falsifier IS
  '反証条件。何が観測されたら「間違いだった」と判断するか。is_measurable=true では必須（CHECK制約）';
COMMENT ON COLUMN scouter.thesis_definitions.observables IS
  '観測軸の配列。{ref,kind,region,role}。ref は basket:/macro:/market: プレフィクス。命題の対象地域と一致しない系列は role=secondary のみ（日本の波及を米国BEIで判定した誤りの再発防止）';
COMMENT ON COLUMN scouter.thesis_definitions.measure IS
  'price/earnings/both。価格と実績利益は逆の答えを出すことがあるため必須（200A: 1年で価格+170%・実績利益-2.4%）';
COMMENT ON COLUMN scouter.thesis_definitions.stages IS
  'propagation 命題の段階定義。[{stage,name,test,observables}]。波及は水準ではなく到達段階で測る';
COMMENT ON COLUMN scouter.thesis_definitions.scoring IS
  '採点の決定論パラメータ（窓幅・閾値）。UIやコードに閾値を複製せずここを正本にする';

COMMENT ON TABLE scouter.thesis_observations IS
  '命題ごとのPIT採点履歴。レンズ単位の合成スコア列を意図的に持たない（順風と逆風の併存自体が情報）';
COMMENT ON COLUMN scouter.thesis_observations.is_forward IS
  'eval_date >= lens.pit_anchor。後ろ向き検証と前向きトラックレコードを混ぜないためのフラグ';
COMMENT ON COLUMN scouter.thesis_observations.observations IS
  '採点に使った実測値のスナップショット（監査用）。何を見てこの verdict にしたかを後から検証できる';
COMMENT ON COLUMN scouter.thesis_observations.falsifier_state IS
  'intact=反証されていない / approaching=反証条件に接近 / triggered=反証条件に到達 / not_evaluable=判定不能';
