#!/usr/bin/env bash
# J-Quants の ON/OFF スイッチ（00132 の ops.data_source_switches）を確認・切替する。
# 設計正本: ../docs/PLANS-jquants-off-switch-2026-10.md §3.2（JapanStock ルートリポ）
#
# 使い方（DataPipeline のリポジトリ直下で）:
#   bash scripts/ops/jquants-toggle.sh status
#   bash scripts/ops/jquants-toggle.sh off "J-Quants 解約のため"
#   bash scripts/ops/jquants-toggle.sh on  "Standard を再契約したため"
#
# off: 最後の公式取引日（equity_bar_daily の最大日）を記録して OFF にする。
#      以後、Cron A は生成カレンダー＋Yahoo 価格＋TOPIX 推計、Cron C は JPX 週次Excel で動き、
#      J-Quants 依存のワークフロー（requires_jquants）は dispatch も未達検知も止まる。
# on : まず J-Quants API の疎通を確認する（有料プランでしか取れない TOPIX 日足が返るか）。
#      失敗したら ON にしない（fail closed）。成功したら ON にし、埋め戻し手順を表示する。
#
# 読み書きは service_role 専用の橋渡しRPC（jquants_ingest.get/set_data_source_mode）。
# .env.local の NEXT_PUBLIC_SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY（on は JQUANTS_API_KEY も）を使う。
# PORTFOLIO_URL と INTERNAL_REVALIDATE_SECRET が環境変数にあれば、切替後に Portfolio の
# 参照キャッシュを失効させる（無ければ最大1時間で Portfolio の表示が切り替わる）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

usage() { sed -n '2,22p' "$0" >&2; exit 1; }

ACTION="${1:-}"
REASON="${2:-}"
case "$ACTION" in
  status) ;;
  on|off) ;;
  *) usage ;;
esac

env_get() { grep "^$1=" .env.local 2>/dev/null | head -1 | cut -d'=' -f2- | sed -e 's/^"//' -e 's/"$//'; }

SERVICE_KEY="$(env_get SUPABASE_SERVICE_ROLE_KEY)"
SUPA_URL="$(env_get NEXT_PUBLIC_SUPABASE_URL)"
: "${SERVICE_KEY:?SUPABASE_SERVICE_ROLE_KEY を .env.local から読めませんでした}"
: "${SUPA_URL:?NEXT_PUBLIC_SUPABASE_URL を .env.local から読めませんでした}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

json_arg() { python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"; }

rpc() {
  curl -sS -o "$TMP/body" -w '%{http_code}' \
    "$SUPA_URL/rest/v1/rpc/$1" \
    -H "apikey: $SERVICE_KEY" \
    -H "Authorization: Bearer $SERVICE_KEY" \
    -H "Content-Profile: jquants_ingest" \
    -H "Content-Type: application/json" \
    --data "$2" \
    --max-time 60 || echo 000
}

show_status() {
  local code
  code="$(rpc get_data_source_mode '{"p_provider":"jquants"}')"
  if [ "$code" != "200" ]; then
    echo "モードを取得できません (HTTP $code): $(cat "$TMP/body")" >&2
    echo "00132 が未適用なら先に適用してください。" >&2
    exit 1
  fi
  python3 - "$TMP/body" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1]))
if not rows:
    print("ops.data_source_switches に jquants 行がありません（00132 の適用を確認）")
    sys.exit(1)
r = rows[0]
print(f"  J-Quants      : {'ON' if r['enabled'] else 'OFF'}")
print(f"  切替日時      : {r.get('changed_at')}")
print(f"  切替者・理由  : {r.get('changed_by')} / {r.get('reason')}")
print(f"  最後の公式取引日: {r.get('last_official_trade_date') or '—'}")
skipped = r.get('skipped_workflows') or []
print(f"  停止中のワークフロー: {', '.join(skipped) if skipped else 'なし'}")
PY
}

revalidate_portfolio() {
  if [ -n "${PORTFOLIO_URL:-}" ] && [ -n "${INTERNAL_REVALIDATE_SECRET:-}" ]; then
    local code
    code="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$PORTFOLIO_URL/api/internal/revalidate-reference" \
      -H "Authorization: Bearer $INTERNAL_REVALIDATE_SECRET" --max-time 30 || echo 000)"
    echo "Portfolio の参照キャッシュ失効: HTTP $code"
  else
    echo "※ PORTFOLIO_URL / INTERNAL_REVALIDATE_SECRET が無いので Portfolio のキャッシュは失効させていません（最大1時間で表示が切り替わります）。"
  fi
}

echo "=== 現在の状態 ==="
show_status
[ "$ACTION" = "status" ] && exit 0

TARGET=true
[ "$ACTION" = "off" ] && TARGET=false

if [ -z "$REASON" ]; then
  read -r -p "切替の理由（監査ログに残します）: " REASON
fi
[ -n "$REASON" ] || { echo "理由が空です。中断しました。" >&2; exit 1; }

if [ "$TARGET" = "true" ]; then
  echo
  echo "=== J-Quants API の疎通確認（有料プランでのみ取れる TOPIX 日足） ==="
  API_KEY="$(env_get JQUANTS_API_KEY)"
  : "${API_KEY:?JQUANTS_API_KEY を .env.local から読めませんでした（再契約後の新しいキーを入れてください）}"
  FROM="$(python3 -c 'import datetime; print((datetime.date.today()-datetime.timedelta(days=14)).isoformat())')"
  TO="$(python3 -c 'import datetime; print(datetime.date.today().isoformat())')"
  CODE="$(curl -sS -o "$TMP/jq" -w '%{http_code}' \
    "https://api.jquants.com/v2/indices/bars/daily/topix?from=$FROM&to=$TO" \
    -H "x-api-key: $API_KEY" --max-time 30 || echo 000)"
  ROWS="$(python3 -c 'import json,sys
try:
  print(len(json.load(open(sys.argv[1])).get("data") or []))
except Exception:
  print(0)' "$TMP/jq")"
  echo "HTTP $CODE / 直近2週間の TOPIX: $ROWS 行"
  if [ "$CODE" != "200" ] || [ "$ROWS" = "0" ]; then
    echo "J-Quants の有料プランで使えるキーになっていません。ON にしません（fail closed）。" >&2
    echo "応答: $(head -c 300 "$TMP/jq")" >&2
    exit 1
  fi
fi

echo
read -r -p "J-Quants を $( [ "$TARGET" = "true" ] && echo ON || echo OFF ) にします。よろしいですか？ [y/N]: " CONFIRM
if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
  echo "中断しました。"
  exit 1
fi

CODE="$(rpc set_data_source_mode "{\"p_provider\":\"jquants\",\"p_enabled\":$TARGET,\"p_reason\":$(json_arg "$REASON"),\"p_changed_by\":$(json_arg "${USER:-unknown}@jquants-toggle")}")"
if [ "$CODE" != "200" ]; then
  echo "切替に失敗しました (HTTP $CODE): $(cat "$TMP/body")" >&2
  exit 1
fi

echo
echo "=== 反映後の状態 ==="
show_status
echo
revalidate_portfolio

echo
if [ "$TARGET" = "false" ]; then
  cat <<'MSG'
✅ OFF にしました。
  - 次の Cron A（18:40）から代替経路で動きます。翌日に GitHub Actions が緑か、鮮度アラートが無いかを確認してください。
  - J-Quants の解約はこの確認のあとに J-Quants のサイトで行ってください。
MSG
else
  cat <<'MSG'
✅ ON にしました。続けて、OFF 期間の代替データを公式データへ置き換えてください（期間の長さにかかわらず必須）:
     npx tsx scripts/ops/jquants-restore.ts --dry-run   # 何を消して何を取り直すかの確認
     npx tsx scripts/ops/jquants-restore.ts             # 実行
MSG
fi
