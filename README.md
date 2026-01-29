# JapanStockAnalyzer

J-Quants API V2 から日本株データを取得し、Supabase に自動同期するデータ基盤プロジェクト。

## 技術スタック

- **フレームワーク**: Next.js 16 (Turbopack) / React 19 / TypeScript 5.7
- **データベース**: Supabase (PostgreSQL)
- **スケジューラ**: GitHub Actions (Cron)
- **デプロイ**: Vercel (Hobby)
- **メール通知**: Resend
- **エラー監視**: Sentry
- **テスト**: Vitest

## セットアップ

```bash
npm install
cp .env.local.example .env.local
# .env.local に環境変数を設定
npm run dev
```

環境変数の詳細は [docs/operations/env-variables.md](docs/operations/env-variables.md) を参照。

## スクリプト

| コマンド | 説明 |
|---|---|
| `npm run dev` | 開発サーバー起動 |
| `npm run build` | プロダクションビルド |
| `npm test` | テスト (watch) |
| `npm run test:run` | テスト (単発) |
| `npm run test:coverage` | カバレッジレポート |
| `npm run typecheck` | 型チェック |
| `npm run lint` | Lint |
| `npm run seed:all` | 初期データ一括投入 |

## Cron スケジュール

| ジョブ | 時刻 (JST) | 内容 |
|---|---|---|
| A | 18:40 | 日次確定データ (カレンダー, 株価, TOPIX, 銘柄マスタ, 財務) |
| B | 19:20 | 決算発表カレンダー |
| C | 12:10 | 投資部門別売買 + 整合性チェック |

## ディレクトリ構成

```
src/
├── app/api/cron/jquants/   # Cron API Routes (a, b, c)
├── lib/
│   ├── jquants/            # J-Quants API クライアント
│   ├── supabase/           # Supabase クライアント
│   ├── cron/               # ジョブ実行管理 (ロック, ハートビート)
│   ├── notification/       # メール通知
│   └── utils/              # ユーティリティ
└── tests/                  # ユニットテスト
supabase/migrations/        # DBマイグレーション
scripts/seed/               # 初期データ投入CLI
docs/                       # 運用ドキュメント
```

## ドキュメント

- [運用ガイド](docs/operations/README.md)
- [アーキテクチャ図](docs/architecture/architecture-mermaid.md)
- [Cronスケジュール詳細](docs/operations/cron-schedule.md)
- [トラブルシューティング](docs/operations/troubleshooting.md)
- [手動再同期手順](docs/operations/manual-resync.md)
