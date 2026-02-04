# 環境変数一覧

## 必須環境変数

### Supabase

| 変数名 | 説明 | 例 |
|--------|------|-----|
| `NEXT_PUBLIC_SUPABASE_URL` | Supabase プロジェクト URL | `https://xxxxx.supabase.co` |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | Supabase 匿名キー | `eyJhbGci...` |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase サービスロールキー | `eyJhbGci...` |

### J-Quants API

| 変数名 | 説明 | 例 |
|--------|------|-----|
| `JQUANTS_API_KEY` | J-Quants API V2 キー | `your-api-key` |

> **取得方法**: [J-Quants ダッシュボード](https://application.jpx-jquants.com/) からAPIキーを発行

### FRED API (Cron D)

| 変数名 | 説明 | 例 |
|--------|------|-----|
| `FRED_API_KEY` | FRED API キー | `your-fred-api-key` |

> **取得方法**: [FRED API Keys](https://fred.stlouisfed.org/docs/api/api_key.html) からAPIキーを発行（無料）

### e-Stat API (Cron D)

| 変数名 | 説明 | 例 |
|--------|------|-----|
| `ESTAT_API_KEY` | e-Stat API アプリケーション ID | `your-estat-app-id` |

> **取得方法**: [e-Stat API](https://www.e-stat.go.jp/api/) でユーザー登録後、アプリケーション ID を発行（無料）

### Cron 認証

| 変数名 | 説明 | 例 |
|--------|------|-----|
| `CRON_SECRET` | Cron エンドポイント認証用シークレット | `your-random-secret` |

> **生成方法**: `openssl rand -base64 32` などで十分な長さのランダム文字列を生成

### 通知

| 変数名 | 説明 | 例 |
|--------|------|-----|
| `RESEND_API_KEY` | Resend API キー | `re_xxxxx` |
| `ALERT_EMAIL_TO` | 障害通知の送信先メールアドレス | `admin@example.com` |

## オプション環境変数

| 変数名 | 説明 | デフォルト |
|--------|------|-----------|
| `SYNC_MAX_CATCHUP_DAYS` | 1回の実行で処理する最大キャッチアップ日数 | `5` |
| `SYNC_LOOKBACK_DAYS` | キャッチアップで遡る最大日数 | `30` |
| `INVESTOR_TYPES_WINDOW_DAYS` | 投資部門別のスライディングウィンドウ日数 | `60` |

## 設定場所

### Vercel

1. Vercel ダッシュボードでプロジェクトを開く
2. **Settings** → **Environment Variables**
3. 各環境変数を追加（Production, Preview, Development を選択）

### GitHub Actions

1. リポジトリの **Settings** → **Secrets and variables** → **Actions**
2. **Repository secrets** に以下を追加:
   - `VERCEL_URL`: デプロイ先の Vercel URL（例: `https://your-app.vercel.app`）
   - `CRON_SECRET`: Cron 認証用シークレット
   - `FRED_API_KEY`: FRED API キー（Cron D 用）
   - `ESTAT_API_KEY`: e-Stat API アプリケーション ID（Cron D 用）
   - `NEXT_PUBLIC_SUPABASE_URL`: Supabase プロジェクト URL（Cron D 用）
   - `SUPABASE_SERVICE_ROLE_KEY`: Supabase サービスロールキー（Cron D 用）

### ローカル開発

`.env.local` ファイルを作成:

```bash
cp .env.local.example .env.local
```

各値を設定:

```env
# Supabase
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# J-Quants API
JQUANTS_API_KEY=your-jquants-api-key

# Cron Security
CRON_SECRET=your-cron-secret

# Resend (Email Notification)
RESEND_API_KEY=your-resend-api-key
ALERT_EMAIL_TO=your-email@example.com

# Optional
SYNC_MAX_CATCHUP_DAYS=5
INVESTOR_TYPES_WINDOW_DAYS=60

# FRED API (Cron D)
FRED_API_KEY=your-fred-api-key

# e-Stat API (Cron D)
ESTAT_API_KEY=your-estat-app-id
```

## セキュリティ注意事項

- `.env.local` は **絶対に Git にコミットしない**（`.gitignore` に含まれています）
- `SUPABASE_SERVICE_ROLE_KEY` は RLS をバイパスするため、サーバーサイドのみで使用
- `CRON_SECRET` は十分な長さ（32文字以上推奨）のランダム文字列を使用
- 本番環境では環境変数の値を定期的にローテーション
