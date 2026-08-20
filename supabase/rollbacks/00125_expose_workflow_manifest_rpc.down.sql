-- 00125 のロールバック。
-- RPCを落とすだけで、ops.expected_workflows の中身（enabled の現在値）は変更しない。
-- 戻したあとは manifest の切替が Supabase SQL エディタでの手作業に戻る。

DROP FUNCTION IF EXISTS jquants_ingest.set_expected_workflow_enabled(text, boolean);
DROP FUNCTION IF EXISTS jquants_ingest.list_expected_workflows(text);
