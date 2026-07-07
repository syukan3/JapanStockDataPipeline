-- 00071_add_supplement_status.sql
-- prompt_supplement の効果ゲート＋ロールバック（Phase 2）のための状態管理・帰属記録カラム。
-- - メタ評価2表に supplement_status（active/retired/rolled_back）を追加し、
--   「現在日次プロンプトへ注入すべき改善指示」を状態で管理する（従来は最新行を無条件注入）。
--   active の一意性は部分 UNIQUE インデックスで DB レベルでも強制する。
-- - 日次評価2表に prompt_supplement_id を追加し、どの評価がどの改善指示の下で
--   行われたかの帰属（コホート）を記録する。効果ゲートはこのコホート別の実測較正
--   （Brier・的中率）で改善/悪化を判定する。
-- - status 遷移（旧 active の退役 + 新規メタ評価行の upsert）は途中失敗で active が
--   0件/2件にならないよう、1トランザクションの DB 関数（fn_transition_*_supplement）で行う。
-- 詳細: JapanStockScouter/PLANS-llm-self-improvement.md §3 Phase 2

-- ============================================================
-- 00071-1: メタ評価テーブルに supplement_status を追加
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'scouter' AND table_name = 'macro_ai_meta_evaluations'
      AND column_name = 'supplement_status'
  ) THEN
    ALTER TABLE scouter.macro_ai_meta_evaluations
      ADD COLUMN supplement_status TEXT NOT NULL DEFAULT 'active'
      CHECK (supplement_status IN ('active', 'retired', 'rolled_back'));

    -- 既存行バックフィル: 最新行のみ active、それ以外は retired
    UPDATE scouter.macro_ai_meta_evaluations
    SET supplement_status = 'retired'
    WHERE id <> (
      SELECT id FROM scouter.macro_ai_meta_evaluations
      ORDER BY eval_date DESC, id DESC LIMIT 1
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'scouter' AND table_name = 'hd_ai_meta_evaluations'
      AND column_name = 'supplement_status'
  ) THEN
    ALTER TABLE scouter.hd_ai_meta_evaluations
      ADD COLUMN supplement_status TEXT NOT NULL DEFAULT 'active'
      CHECK (supplement_status IN ('active', 'retired', 'rolled_back'));

    -- 既存行バックフィル: 最新行のみ active、それ以外は retired
    UPDATE scouter.hd_ai_meta_evaluations
    SET supplement_status = 'retired'
    WHERE id <> (
      SELECT id FROM scouter.hd_ai_meta_evaluations
      ORDER BY eval_date DESC, id DESC LIMIT 1
    );
  END IF;
END
$$;

COMMENT ON COLUMN scouter.macro_ai_meta_evaluations.supplement_status IS
  'prompt_supplement の適用状態。active=日次プロンプトへ注入中（部分UNIQUEで常に1件以下） / retired=通常退役（後続supplementに交代 or 判定保留で未適用保存） / rolled_back=効果ゲートで実測悪化と判定され巻き戻し';
COMMENT ON COLUMN scouter.hd_ai_meta_evaluations.supplement_status IS
  'prompt_supplement の適用状態。active=日次プロンプトへ注入中（部分UNIQUEで常に1件以下） / retired=通常退役（後続supplementに交代 or 判定保留で未適用保存） / rolled_back=効果ゲートで実測悪化と判定され巻き戻し';

-- ============================================================
-- 00071-2: active 一意の部分 UNIQUE インデックス
-- （作成前に重複 active を決定的にクリーンアップ: 最新 eval_date（同日なら最大id）以外を retired 化）
-- ============================================================
UPDATE scouter.macro_ai_meta_evaluations
SET supplement_status = 'retired'
WHERE supplement_status = 'active'
  AND id <> (
    SELECT id FROM scouter.macro_ai_meta_evaluations
    WHERE supplement_status = 'active'
    ORDER BY eval_date DESC, id DESC LIMIT 1
  );

UPDATE scouter.hd_ai_meta_evaluations
SET supplement_status = 'retired'
WHERE supplement_status = 'active'
  AND id <> (
    SELECT id FROM scouter.hd_ai_meta_evaluations
    WHERE supplement_status = 'active'
    ORDER BY eval_date DESC, id DESC LIMIT 1
  );

CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_ai_meta_single_active
  ON scouter.macro_ai_meta_evaluations ((TRUE))
  WHERE supplement_status = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS uq_hd_ai_meta_single_active
  ON scouter.hd_ai_meta_evaluations ((TRUE))
  WHERE supplement_status = 'active';

-- ============================================================
-- 00071-3: 日次評価テーブルに prompt_supplement_id（帰属記録）を追加
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'scouter' AND table_name = 'macro_ai_evaluations'
      AND column_name = 'prompt_supplement_id'
  ) THEN
    ALTER TABLE scouter.macro_ai_evaluations
      ADD COLUMN prompt_supplement_id BIGINT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'scouter' AND table_name = 'ai_evaluations'
      AND column_name = 'prompt_supplement_id'
  ) THEN
    ALTER TABLE scouter.ai_evaluations
      ADD COLUMN prompt_supplement_id BIGINT;
  END IF;
END
$$;

COMMENT ON COLUMN scouter.macro_ai_evaluations.prompt_supplement_id IS
  'この評価に適用した prompt_supplement の元メタ評価行 id（scouter.macro_ai_meta_evaluations.id）。効果ゲートのコホート判定に使う帰属記録。supplement未適用（メタ評価前・取得失敗時）は NULL';
COMMENT ON COLUMN scouter.ai_evaluations.prompt_supplement_id IS
  'この評価に適用した prompt_supplement の元メタ評価行 id（scouter.hd_ai_meta_evaluations.id）。効果ゲートのコホート判定に使う帰属記録。前回評価の再利用行（skipped=true）と supplement 未適用時は NULL';

-- コホート集計（prompt_supplement_id 別の較正計算）用インデックス
CREATE INDEX IF NOT EXISTS idx_macro_ai_evals_supplement
  ON scouter.macro_ai_evaluations (prompt_supplement_id)
  WHERE prompt_supplement_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ai_evals_supplement
  ON scouter.ai_evaluations (prompt_supplement_id)
  WHERE prompt_supplement_id IS NOT NULL;

-- ============================================================
-- 00071-4: status 遷移 + 新規メタ評価行 upsert の原子的トランザクション関数
-- 旧 active を retired/rolled_back へ更新した直後の upsert 失敗で active 0件になる
-- 非アトミック性を防ぐ。テーブル名はハードコード（動的SQLなし＝SQLインジェクション不可）。
-- SECURITY INVOKER（既定）: 呼び出しは service_role のみ（下で EXECUTE を REVOKE）。
-- ============================================================

CREATE OR REPLACE FUNCTION scouter.fn_transition_macro_supplement(
  p_prev_id BIGINT,       -- 退役させる旧 active 行の id（不要時 NULL）
  p_prev_status TEXT,     -- 'retired' | 'rolled_back'（p_prev_id NULL 時は無視）
  p_new_row JSONB         -- 新規メタ評価行（eval_date キーで upsert）
) RETURNS VOID
LANGUAGE plpgsql
SET search_path = ''
AS $$
DECLARE
  r scouter.macro_ai_meta_evaluations;
BEGIN
  IF p_prev_id IS NOT NULL THEN
    IF p_prev_status NOT IN ('retired', 'rolled_back') THEN
      RAISE EXCEPTION 'invalid p_prev_status: %', p_prev_status;
    END IF;
    UPDATE scouter.macro_ai_meta_evaluations
    SET supplement_status = p_prev_status
    WHERE id = p_prev_id;
  END IF;

  r := jsonb_populate_record(NULL::scouter.macro_ai_meta_evaluations, p_new_row);
  IF r.supplement_status NOT IN ('active', 'retired') THEN
    RAISE EXCEPTION 'invalid supplement_status for new row: %', r.supplement_status;
  END IF;

  INSERT INTO scouter.macro_ai_meta_evaluations (
    eval_date, period_start, period_end, eval_count,
    repetition_score, specificity_score, divergence_score, contrarian_score,
    delta_score, recommendation_score, overall_score,
    repetition_analysis, specificity_analysis, divergence_analysis,
    contrarian_analysis, delta_analysis, recommendation_analysis,
    prompt_supplement, previous_overall_score, score_delta, improvement_notes,
    model_id, prompt_version, supplement_status
  ) VALUES (
    r.eval_date, r.period_start, r.period_end, r.eval_count,
    r.repetition_score, r.specificity_score, r.divergence_score, r.contrarian_score,
    r.delta_score, r.recommendation_score, r.overall_score,
    r.repetition_analysis, r.specificity_analysis, r.divergence_analysis,
    r.contrarian_analysis, r.delta_analysis, r.recommendation_analysis,
    r.prompt_supplement, r.previous_overall_score, r.score_delta, r.improvement_notes,
    r.model_id, r.prompt_version, r.supplement_status
  )
  ON CONFLICT (eval_date) DO UPDATE SET
    period_start = EXCLUDED.period_start,
    period_end = EXCLUDED.period_end,
    eval_count = EXCLUDED.eval_count,
    repetition_score = EXCLUDED.repetition_score,
    specificity_score = EXCLUDED.specificity_score,
    divergence_score = EXCLUDED.divergence_score,
    contrarian_score = EXCLUDED.contrarian_score,
    delta_score = EXCLUDED.delta_score,
    recommendation_score = EXCLUDED.recommendation_score,
    overall_score = EXCLUDED.overall_score,
    repetition_analysis = EXCLUDED.repetition_analysis,
    specificity_analysis = EXCLUDED.specificity_analysis,
    divergence_analysis = EXCLUDED.divergence_analysis,
    contrarian_analysis = EXCLUDED.contrarian_analysis,
    delta_analysis = EXCLUDED.delta_analysis,
    recommendation_analysis = EXCLUDED.recommendation_analysis,
    prompt_supplement = EXCLUDED.prompt_supplement,
    previous_overall_score = EXCLUDED.previous_overall_score,
    score_delta = EXCLUDED.score_delta,
    improvement_notes = EXCLUDED.improvement_notes,
    model_id = EXCLUDED.model_id,
    prompt_version = EXCLUDED.prompt_version,
    supplement_status = EXCLUDED.supplement_status;
END
$$;

CREATE OR REPLACE FUNCTION scouter.fn_transition_hd_supplement(
  p_prev_id BIGINT,       -- 退役させる旧 active 行の id（不要時 NULL）
  p_prev_status TEXT,     -- 'retired' | 'rolled_back'（p_prev_id NULL 時は無視）
  p_new_row JSONB         -- 新規メタ評価行（eval_date キーで upsert）
) RETURNS VOID
LANGUAGE plpgsql
SET search_path = ''
AS $$
DECLARE
  r scouter.hd_ai_meta_evaluations;
BEGIN
  IF p_prev_id IS NOT NULL THEN
    IF p_prev_status NOT IN ('retired', 'rolled_back') THEN
      RAISE EXCEPTION 'invalid p_prev_status: %', p_prev_status;
    END IF;
    UPDATE scouter.hd_ai_meta_evaluations
    SET supplement_status = p_prev_status
    WHERE id = p_prev_id;
  END IF;

  r := jsonb_populate_record(NULL::scouter.hd_ai_meta_evaluations, p_new_row);
  IF r.supplement_status NOT IN ('active', 'retired') THEN
    RAISE EXCEPTION 'invalid supplement_status for new row: %', r.supplement_status;
  END IF;

  INSERT INTO scouter.hd_ai_meta_evaluations (
    eval_date, period_start, period_end, signal_count, matched_count,
    sentiment_accuracy_score, confidence_calibration_score, risk_detection_score,
    sector_quality_score, recommendation_precision_score, overall_score,
    sentiment_accuracy_analysis, confidence_calibration_analysis, risk_detection_analysis,
    sector_quality_analysis, recommendation_precision_analysis,
    prompt_supplement, previous_overall_score, score_delta, improvement_notes,
    model_id, prompt_version, supplement_status
  ) VALUES (
    r.eval_date, r.period_start, r.period_end, r.signal_count, r.matched_count,
    r.sentiment_accuracy_score, r.confidence_calibration_score, r.risk_detection_score,
    r.sector_quality_score, r.recommendation_precision_score, r.overall_score,
    r.sentiment_accuracy_analysis, r.confidence_calibration_analysis, r.risk_detection_analysis,
    r.sector_quality_analysis, r.recommendation_precision_analysis,
    r.prompt_supplement, r.previous_overall_score, r.score_delta, r.improvement_notes,
    r.model_id, r.prompt_version, r.supplement_status
  )
  ON CONFLICT (eval_date) DO UPDATE SET
    period_start = EXCLUDED.period_start,
    period_end = EXCLUDED.period_end,
    signal_count = EXCLUDED.signal_count,
    matched_count = EXCLUDED.matched_count,
    sentiment_accuracy_score = EXCLUDED.sentiment_accuracy_score,
    confidence_calibration_score = EXCLUDED.confidence_calibration_score,
    risk_detection_score = EXCLUDED.risk_detection_score,
    sector_quality_score = EXCLUDED.sector_quality_score,
    recommendation_precision_score = EXCLUDED.recommendation_precision_score,
    overall_score = EXCLUDED.overall_score,
    sentiment_accuracy_analysis = EXCLUDED.sentiment_accuracy_analysis,
    confidence_calibration_analysis = EXCLUDED.confidence_calibration_analysis,
    risk_detection_analysis = EXCLUDED.risk_detection_analysis,
    sector_quality_analysis = EXCLUDED.sector_quality_analysis,
    recommendation_precision_analysis = EXCLUDED.recommendation_precision_analysis,
    prompt_supplement = EXCLUDED.prompt_supplement,
    previous_overall_score = EXCLUDED.previous_overall_score,
    score_delta = EXCLUDED.score_delta,
    improvement_notes = EXCLUDED.improvement_notes,
    model_id = EXCLUDED.model_id,
    prompt_version = EXCLUDED.prompt_version,
    supplement_status = EXCLUDED.supplement_status;
END
$$;

COMMENT ON FUNCTION scouter.fn_transition_macro_supplement(BIGINT, TEXT, JSONB) IS
  'マクロAIメタ評価の supplement status 遷移（旧active退役 + 新規行upsert）を1トランザクションで行う。active一意は uq_macro_ai_meta_single_active が強制';
COMMENT ON FUNCTION scouter.fn_transition_hd_supplement(BIGINT, TEXT, JSONB) IS
  '高配当AIメタ評価の supplement status 遷移（旧active退役 + 新規行upsert）を1トランザクションで行う。active一意は uq_hd_ai_meta_single_active が強制';

-- EXECUTE は service_role のみ（00053 の refresh_stock_metrics と同方針）
REVOKE ALL ON FUNCTION scouter.fn_transition_macro_supplement(BIGINT, TEXT, JSONB) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION scouter.fn_transition_hd_supplement(BIGINT, TEXT, JSONB) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION scouter.fn_transition_macro_supplement(BIGINT, TEXT, JSONB) TO service_role;
GRANT EXECUTE ON FUNCTION scouter.fn_transition_hd_supplement(BIGINT, TEXT, JSONB) TO service_role;
