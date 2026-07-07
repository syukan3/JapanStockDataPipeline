# JapanStockDataPipeline

日本株データパイプライン（Public リポジトリ）。J-Quants / FRED / e-Stat / スクレイピングからデータを自動収集し、Supabase に格納。**共有DBのスキーマ所有者**（portfolio スキーマ以外の全マイグレーションを管理）。

プロジェクト全体像は `../docs/PROJECT-OVERVIEW.md` を参照。

## 機能マップ

| 機能 | 実行 (JST) | 入口 |
|---|---|---|
| Cron A: 株価/TOPIX/財務/銘柄マスタ + analytics再計算 | 毎日 18:40 | `.github/workflows/cron-a.yml` → `scripts/cron/cron-a-direct.ts` |
| Cron B: 決算発表カレンダー | 毎日 19:30 | `.github/workflows/cron-b.yml` → API `/api/cron/jquants/b` |
| Cron C: 投資部門別売買 + 整合性チェック | 毎日 12:10 | `.github/workflows/cron-c.yml` |
| Cron D: マクロ指標（FRED/e-Stat） | 毎日 07:00 | `.github/workflows/cron-d.yml` → `scripts/cron/cron-d-direct.ts` |
| Cron E: 優待 + 信用残高 | 毎日 16:30 | `.github/workflows/cron-e.yml` → `scripts/cron/cron-e-direct.ts` |
| テクニカル指標再計算 | Cron A 内 | `scripts/cron/refresh-technical.ts` |
| 市場指標（日経PER/騰落レシオ等9種） | Cron A 内 | `scripts/cron/refresh-market-indicators.ts` |
| DBアーカイブ（450MB超過時） | 日曜 03:00 | `.github/workflows/db-archival.yml` |
| pg_cron 外部トリガー（GH Actions遅延対策） | — | マイグレ 00057/00058、監視 00060/00061（ops スキーマ） |

## ディレクトリ地図

- `src/lib/jquants/` `fred/` `estat/` `yutai/` `market/` — データソース別クライアント
- `src/lib/analytics/` — technical.ts / market-breadth.ts（派生指標の計算ロジック）
- `src/lib/cron/` — 実行基盤（job-run, job-lock, catch-up, forward-fill, heartbeat）
- `scripts/seed/` — バックフィル（`npm run seed:all` ほか個別）
- `supabase/migrations/` — 全68本+。スキーマ仕様の一次情報はここ（番号順・最新優先で読む）

## 所有スキーマ

`jquants_core`（生データ）/ `jquants_ingest`（ジョブ管理・監視VIEW）/ `analytics`（stock_metrics・technical_metrics・market_indicators・stock_screen・market_sector_weights）/ `scouter`（テーブル定義のみ。書き込みは Scouter）/ `ops`（外部トリガー）

## 設計上の約束

- 全同期は冪等（UPSERT）。対象日はカレンダー**前方フィル**方式（job_runs 依存にしない）
- Vercel 10秒制限があるため重い処理は GH Actions ランナーで tsx 直接実行
- J-Quants は 60req/min（トークンバケット）。Pooler は Advisory Lock 不可 → TTL付きテーブルロック
- Cron A の equity_bars 失敗時はゲートで factor/technical がスキップされる → 復旧後に手動再計算

## コマンド

```bash
npm run lint && npm run typecheck && npm run test:run   # 品質ゲート（他: dev / build）
npm run seed:all                     # J-Quants 基礎データの一括バックフィル（個別: seed:bars 等。market は seed:market）
npm run cron:direct -- equity_bars   # Cron A の個別データセットをローカル実行（dataset 引数必須）
npm run cron:market -- --dry-run     # 市場指標再計算（--full / --only-breadth / --skip-breadth あり）
```

注意: Cron の実行主体は GH Actions + pg_cron 外部トリガーのみ。時刻の正は `.github/workflows/`（legacy な Vercel Cron 定義は二重発火のため 2026-07-07 に vercel.json ごと削除済み）。

## Skills

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
