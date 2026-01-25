# J-Quants API V2 データ同期基盤 タスクリスト

## 概要

実装計画: [000001_jquants-sync-implementation.md](../plan/000001_jquants-sync-implementation.md)

## タスク一覧

### Phase 1: プロジェクト初期設定

- [x] package.json 作成
- [x] tsconfig.json 作成
- [x] next.config.ts 作成
- [x] .env.local.example 作成
- [ ] src/app/layout.tsx 作成
- [ ] src/app/page.tsx 作成
- [ ] .github/workflows/cron-a.yml - 日次確定データスケジューラ（JST 09:20）
- [ ] .github/workflows/cron-b.yml - 決算発表予定スケジューラ（JST 19:20）
- [ ] .github/workflows/cron-c.yml - 投資部門別スケジューラ（JST 12:10）
- [ ] .github/workflows/backup.yml - 週次バックアップ

### Phase 2: Supabase マイグレーション

- [ ] 00001_create_schemas.sql - スキーマ作成 (jquants_core, jquants_ingest)
- [ ] 00002_create_ingest_tables.sql - ジョブ管理テーブル
  - [ ] job_locks
  - [ ] job_runs
  - [ ] job_run_items
  - [ ] job_heartbeat
- [ ] 00003_create_core_tables.sql - データテーブル
  - [ ] trading_calendar
  - [ ] equity_master_snapshot
  - [ ] equity_bar_daily
  - [ ] topix_bar_daily
  - [ ] financial_disclosure
  - [ ] earnings_calendar
  - [ ] investor_type_trading
- [ ] 00004_enable_rls.sql - RLS設定
- [ ] 00005_create_monitoring_views.sql - 監視用ビュー

### Phase 3: 基盤ライブラリ

- [ ] src/lib/supabase/client.ts - ブラウザ用クライアント
- [ ] src/lib/supabase/admin.ts - Service Role クライアント
- [ ] src/lib/jquants/types.ts - 型定義
- [ ] src/lib/jquants/rate-limiter.ts - トークンバケット (60 req/min)
- [ ] src/lib/jquants/client.ts - API クライアント
- [ ] src/lib/utils/date.ts - JST日付ユーティリティ
- [ ] src/lib/utils/retry.ts - 指数バックオフ
- [ ] src/lib/utils/batch.ts - バッチ処理
- [ ] src/lib/utils/logger.ts - 構造化ロギング
- [ ] src/lib/cron/auth.ts - CRON_SECRET 検証
- [ ] src/lib/cron/job-lock.ts - テーブルベースロック
- [ ] src/lib/cron/job-run.ts - 実行ログ管理
- [ ] src/lib/cron/heartbeat.ts - 死活監視
- [ ] src/lib/cron/catch-up.ts - キャッチアップ
- [ ] src/lib/cron/business-day.ts - 営業日判定
- [ ] src/lib/notification/email.ts - Resend クライアント
- [ ] src/lib/notification/templates.ts - メールテンプレート

### Phase 4: J-Quants エンドポイント

- [ ] src/lib/jquants/endpoints/trading-calendar.ts
- [ ] src/lib/jquants/endpoints/equity-master.ts
- [ ] src/lib/jquants/endpoints/equity-bars-daily.ts
- [ ] src/lib/jquants/endpoints/index-topix.ts
- [ ] src/lib/jquants/endpoints/fins-summary.ts
- [ ] src/lib/jquants/endpoints/earnings-calendar.ts
- [ ] src/lib/jquants/endpoints/investor-types.ts

### Phase 5: Cron ハンドラー

- [ ] src/lib/cron/handlers/cron-a.ts - 日次確定データ
  - 前営業日の株価、財務、TOPIX、銘柄マスタ、カレンダー
  - キャッチアップロジック (最大5営業日)
- [ ] src/lib/cron/handlers/cron-b.ts - 決算発表予定
  - 翌営業日の決算発表予定
- [ ] src/lib/cron/handlers/cron-c.ts - 投資部門別
  - スライディングウィンドウ60日
  - 整合性チェック

### Phase 6: Route Handler

- [ ] src/app/api/cron/jquants/a/route.ts
- [ ] src/app/api/cron/jquants/b/route.ts
- [ ] src/app/api/cron/jquants/c/route.ts

### Phase 7: 運用ドキュメント

- [ ] docs/operations/README.md - 運用概要
- [ ] docs/operations/env-variables.md - 環境変数一覧
- [ ] docs/operations/cron-schedule.md - Cron時刻対応表
- [ ] docs/operations/manual-resync.md - 再実行手順
- [ ] docs/operations/troubleshooting.md - 障害時追跡方法

## 依存関係

```
Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 → Phase 7
```

## 技術スタック

| 技術 | バージョン |
|------|-----------|
| Next.js | 15.1.4 |
| TypeScript | 5.7+ |
| @supabase/supabase-js | 2.47+ |
| @supabase/ssr | 0.5+ |
| zod | 3.24+ |
| vitest | 3.0+ |
| resend | latest |
