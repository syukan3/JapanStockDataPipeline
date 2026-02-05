-- Search Grounding の参照元URLを配列で保存するカラムを追加

-- 高配当AI評価
ALTER TABLE scouter.ai_evaluations
ADD COLUMN IF NOT EXISTS grounding_sources TEXT[] DEFAULT '{}';

COMMENT ON COLUMN scouter.ai_evaluations.grounding_sources IS 'Gemini Search Grounding の参照元URL一覧';

-- マクロAI評価
ALTER TABLE scouter.macro_ai_evaluations
ADD COLUMN IF NOT EXISTS grounding_sources TEXT[] DEFAULT '{}';

COMMENT ON COLUMN scouter.macro_ai_evaluations.grounding_sources IS 'Gemini Search Grounding の参照元URL一覧';
