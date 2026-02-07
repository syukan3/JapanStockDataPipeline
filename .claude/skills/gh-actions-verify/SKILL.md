---
name: gh-actions-verify
description: GitHub Actions ワークフローの最短テスト実行・動作確認。「GH Actionsで動作確認」「ワークフロー実行して」「CI回して」で使用する。
---

# GitHub Actions ワークフロー最短テスト実行

新規・修正したワークフローを最短で実行して動作確認するための手順。

---

## 前提: GH_TOKEN 認証

全コマンドの前に必ず実行（CLAUDE.md に記載の方法）:

```bash
export GH_TOKEN="$(grep GH_TOKEN /Users/m-sakae/Source/JapanStock/JapanStockDataPipeline/.envrc | cut -d'"' -f2)"
```

**重要**: マルチリポジトリ環境では `gh` コマンドを実行するディレクトリで対象リポジトリが決まる。必ず対象リポジトリの `cd` を含めること。

```bash
# NG: デフォルトディレクトリで実行 → 意図しないリポジトリを参照
gh workflow run ...

# OK: 明示的に cd してから実行
cd /Users/m-sakae/Source/JapanStock/JapanStockScouter && gh workflow run ...
```

---

## Step 1: プリフライトチェック（2分）

新規ワークフローを `workflow_dispatch` でトリガーするには **main ブランチにマージ済み** が必須。

```bash
# 1a. ワークフローファイルが main にあるか確認
cd /path/to/repo && git log --oneline -1 -- .github/workflows/{name}.yml

# 1b. ワークフローが GitHub に認識されているか確認
gh workflow list

# 1c. GitHub Secrets の確認
gh secret list
```

### Secrets チェックリスト

| Secret | 必要なワークフロー | 設定方法 |
|---|---|---|
| `NEXT_PUBLIC_SUPABASE_URL` | Supabase 接続が必要なもの全て | `gh secret set NEXT_PUBLIC_SUPABASE_URL < <(echo "値")` |
| `SUPABASE_SERVICE_ROLE_KEY` | 同上 | `gh secret set SUPABASE_SERVICE_ROLE_KEY < <(echo "値")` |
| `RESEND_API_KEY` | メール送信を含むもの | `gh secret set RESEND_API_KEY < <(echo "値")` |
| `ALERT_EMAIL_TO` | メール送信を含むもの | `gh secret set ALERT_EMAIL_TO < <(echo "値")` |
| `CRON_SECRET` | Cron A〜D（Vercel route 認証） | `gh secret set CRON_SECRET < <(echo "値")` |

**値の取得元**: `/Users/m-sakae/Source/JapanStock/JapanStockDataPipeline/.env.local`

### DB マイグレーション確認

新テーブルを使うワークフローの場合:

```bash
cd /Users/m-sakae/Source/JapanStock/JapanStockDataPipeline
SUPABASE_ACCESS_TOKEN=... supabase db push --dry-run
```

`Would push these migrations:` にファイルが出たら適用が必要:

```bash
SUPABASE_ACCESS_TOKEN=... supabase db push
```

---

## Step 2: トリガー（10秒）

```bash
# 基本形
gh workflow run {workflow}.yml --ref main

# inputs がある場合
gh workflow run cron-e.yml --ref main -f source=kabuyutai

# フィーチャーブランチで試す場合（main にも同名ワークフローが必要）
gh workflow run {workflow}.yml --ref feat/branch-name
```

---

## Step 3: 監視（成功/失敗まで）

```bash
# Run ID を取得
gh run list --workflow={workflow}.yml -L 1

# リアルタイム監視（完了まで待機）
gh run watch {RUN_ID} --exit-status

# タイムアウトが長い場合はバックグラウンドで
gh run watch {RUN_ID} --exit-status &
```

---

## Step 4: 失敗時の診断（30秒）

```bash
# 失敗ステップのログのみ
gh run view {RUN_ID} --log-failed 2>&1 | tail -50

# 全ログからアプリケーション出力を抽出
gh run view {RUN_ID} --log 2>&1 | grep -E '"message"' | head -30

# 特定パターンで検索
gh run view {RUN_ID} --log 2>&1 | grep -iE "error|fail|denied|constraint|timeout" | head -20
```

---

## Step 5: 修正 → 再実行サイクル

```bash
# 1. ローカルで修正・テスト
npx vitest run src/tests/...

# 2. コミット & プッシュ
git add ... && git commit -m "fix: ..." && git push origin main

# 3. 即座に再トリガー
gh workflow run {workflow}.yml --ref main

# 4. 結果確認（1コマンドで）
sleep 5 && gh run list --workflow={workflow}.yml -L 1
```

---

## よくあるエラーと対処法

| エラー | 原因 | 対処 |
|---|---|---|
| `HTTP 404: workflow xxx not found on the default branch` | main 未マージ or 別リポジトリのディレクトリで実行 | `cd` でリポジトリを切り替え、main にマージ |
| `exceeded the maximum execution time of Xm` | `timeout-minutes` 不足 | ワークフローの `timeout-minutes` を引き上げ |
| `permission denied for table xxx` | `service_role` への GRANT 不足 | `GRANT ALL ON schema.table TO service_role;` |
| `violates check constraint "job_runs_job_name_check"` | job_name が CHECK 制約に未登録 | `ALTER TABLE ... DROP/ADD CONSTRAINT` で追加 |
| `duplicate key value violates unique constraint` | 同日再実行で一意制約に衝突 | insert → 23505 エラー時に update へフォールバック |
| `there is no unique or exclusion constraint matching the ON CONFLICT` | 部分インデックスに対して upsert | `onConflict` 不可。insert + error handling で対応 |
| `Could not find the table 'schema.table' in the schema cache` | テーブル未作成 or GRANT 未付与 | `supabase db push` でマイグレーション適用 |
| Secret が `***` でなく空 | GitHub Secrets 未設定 | `gh secret set` で設定 |

---

## 実績タイムライン（参考）

新規ワークフロー2本を最短で動作確認した実績:

| 時刻 | アクション | 結果 |
|---|---|---|
| +0m | main マージ & push | - |
| +1m | workflow_dispatch トリガー | - |
| +2m | 失敗: Secrets 未設定 | `gh secret set` で追加 |
| +3m | 再トリガー | - |
| +4m | 失敗: テーブル GRANT 不足 | マイグレーション追加・適用 |
| +5m | 再トリガー | 成功（Scouter） |
| +5m | 失敗: URL 構造ミス（DataPipeline） | パーサー修正 |
| +8m | 再トリガー | 失敗: タイムアウト |
| +9m | timeout-minutes 引き上げ + レート制限調整 | - |
| +15m | 再トリガー | 成功（DataPipeline） |
| +16m | Scouter 再トリガー（データ投入後） | 成功 |

**教訓**: 1回で成功することは稀。Secrets → GRANT → コード → タイムアウトの順に問題が顕在化する。プリフライトチェック（Step 1）を徹底すれば大幅に短縮できる。

---

## マルチリポジトリ環境での注意

このプロジェクトは3リポジトリ（DataPipeline / Portfolio / Scouter）が同一ディレクトリ配下にある。

- `gh` コマンドは `.git/config` の `origin` URL で対象リポジトリを判定
- **全ての `gh` コマンドに `cd /path/to/repo &&` を付ける**
- ブランチ操作（`git checkout -b`）も同様にディレクトリに注意
- Bash ツールの「Working directory persists between commands」は**シェル間では持続しない**
