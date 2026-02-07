# JapanStockDataPipeline

日本株データパイプライン

## Skills

このプロジェクトでは以下のスキルを使用します：

### Database
- [Supabase Postgres Best Practices](.claude/skills/postgres-best-practices/SKILL.md)
  - SQLクエリ作成、スキーマ設計、インデックス最適化、RLS設定
- [Supabase Data Check](.claude/skills/supabase-data-check/SKILL.md)
  - データ存在確認、最新データ日チェック、Cron A結果確認
- [Supabase DB Capacity](.claude/skills/supabase-db-capacity/SKILL.md)
  - DB容量確認、テーブル別サイズ、Free Plan使用率チェック

### Ops
- [Cron Troubleshoot](.claude/skills/cron-troubleshoot/SKILL.md)
  - Cron障害調査、エラーログ解析、データ欠損特定、バックフィル
- [GH Actions Verify](.claude/skills/gh-actions-verify/SKILL.md)
  - GitHub Actionsワークフローの最短テスト実行・動作確認

### Frontend
- [Vercel React Best Practices](.claude/skills/react-best-practices/SKILL.md)
  - React/Next.js パフォーマンス最適化、57ルール（8カテゴリ）
- [Web Design Guidelines](.claude/skills/web-design-guidelines/SKILL.md)
  - UI/UXレビュー、アクセシビリティチェック

### Review
- [Codex Review](.claude/skills/codex-review/SKILL.md)
  - Codex CLIを使った反復レビューゲート

## GitHub CLI (gh)

`gh` コマンド使用時は `.envrc` の `GH_TOKEN` を環境変数にセットして認証する。毎回聞かずに自動で行うこと。

```bash
export GH_TOKEN="$(grep GH_TOKEN .envrc | cut -d'"' -f2)"
```

## Review Gate (codex-review)

主要なマイルストーン（仕様書/計画の更新後、大規模な実装ステップ完了後（5ファイル以上/公開API/インフラ・設定変更）、commit/PR/release前）では、codex-reviewスキルを実行し、レビュー→修正→再レビューのサイクルを問題がなくなるまで繰り返すこと。
