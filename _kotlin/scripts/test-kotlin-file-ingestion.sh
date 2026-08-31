#!/usr/bin/env bash
set -euo pipefail

web_url="${WEB_BASE_URL:-http://localhost:${WEB_PORT:-3000}}"
compose_project="${COMPOSE_PROJECT_NAME:-onyx-foss-kotlin}"
opensearch_index="${OPENSEARCH_INDEX:-onyx-kotlin-chunks}"
compose=(docker compose -p "$compose_project")
curl_args=(--fail-with-body --silent --show-error --connect-timeout 5 --max-time 30)
work_dir="$(mktemp -d)"
connector_id=""
credential_id=""
asset_id=""

cleanup() {
  asset_cleanup_safe=true
  if [[ -n "$connector_id" && -n "$credential_id" ]]; then
    asset_cleanup_safe=false
    if ! curl "${curl_args[@]}" -X POST "$web_url/api/manage/admin/deletion-attempt" \
      -H 'Content-Type: application/json' \
      -d "{\"connector_id\":$connector_id,\"credential_id\":$credential_id}" >/dev/null; then
      if curl "${curl_args[@]}" -X DELETE "$web_url/api/manage/admin/connector/$connector_id" >/dev/null; then
        asset_cleanup_safe=true
      fi
    else
      asset_cleanup_safe=true
    fi
  elif [[ -n "$connector_id" ]]; then
    asset_cleanup_safe=false
    if curl "${curl_args[@]}" -X DELETE "$web_url/api/manage/admin/connector/$connector_id" >/dev/null; then
      asset_cleanup_safe=true
    fi
  fi
  if [[ -n "$credential_id" ]]; then
    curl "${curl_args[@]}" -X DELETE "$web_url/api/manage/credential/$credential_id" >/dev/null || true
  fi
  if [[ "$asset_cleanup_safe" == "true" && "$asset_id" =~ ^[A-Za-z0-9-]+$ ]]; then
    "${compose[@]}" exec -T api rm -f -- "/var/lib/onyx/files/$asset_id" || true
    "${compose[@]}" exec -T postgres psql -U onyx -d onyx -c \
      "DELETE FROM file_assets WHERE id = '$asset_id'" >/dev/null || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

ready=false
for _ in {1..60}; do
  if curl "${curl_args[@]}" "$web_url/api/manage/connector" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
[[ "$ready" == "true" ]] || {
  printf 'Web API did not become ready: %s\n' "$web_url" >&2
  exit 1
}

printf 'Kotlin File ingestion verification %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$work_dir/document.txt"

upload="$(curl "${curl_args[@]}" "$web_url/api/manage/admin/connector/file/upload" \
  -F "files=@$work_dir/document.txt;type=text/plain")"
asset_id="$(jq -er '.file_paths[0]' <<<"$upload")"
file_name="$(jq -er '.file_names[0]' <<<"$upload")"
[[ "$asset_id" =~ ^[A-Za-z0-9-]+$ ]]
source_document_id="FILE_CONNECTOR__$asset_id"

connector_payload="$(jq -cn \
  --arg asset "$asset_id" \
  --arg name "$file_name" \
  '{name:"Task 10 File connector",source:"file",connector_specific_config:{file_locations:[$asset],file_names:[$name]}}')"
connector_id="$(curl "${curl_args[@]}" "$web_url/api/manage/admin/connector" \
  -H 'Content-Type: application/json' -d "$connector_payload" | jq -er '.id')"

credential_id="$(curl "${curl_args[@]}" "$web_url/api/manage/credential" \
  -H 'Content-Type: application/json' \
  -d '{"source":"file","name":"Task 10 File credential","credential_json":{}}' | jq -er '.id')"

pair_id="$(curl "${curl_args[@]}" -X PUT "$web_url/api/manage/connector/$connector_id/credential/$credential_id" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Task 10 File pair"}' | jq -er '.data')"
[[ "$pair_id" =~ ^[0-9]+$ ]]

deadline=$((SECONDS + 180))
status="not_started"
while (( SECONDS < deadline )); do
  status="$(curl "${curl_args[@]}" "$web_url/api/manage/admin/cc-pair/$pair_id/index-attempts?page_num=0&page_size=1" \
    | jq -er '.items[0].status')"
  case "$status" in
    success|completed_with_errors|failed|canceled) break ;;
  esac
  sleep 1
done
[[ "$status" == "success" ]] || {
  printf 'Index attempt ended with status: %s\n' "$status" >&2
  exit 1
}

postgres_count="$("${compose[@]}" exec -T postgres psql -U onyx -d onyx -Atc \
  "SELECT COUNT(*) FROM indexed_documents WHERE cc_pair_id = $pair_id AND source_document_id = '$source_document_id'")"
[[ "$postgres_count" == "1" ]] || {
  printf 'Expected one PostgreSQL document, found: %s\n' "$postgres_count" >&2
  exit 1
}

search_payload="$(jq -cn --argjson pair "$pair_id" --arg document "$source_document_id" \
  '{query:{bool:{filter:[{term:{cc_pair_id:$pair}},{term:{"source_document_id.keyword":$document}}]}}}')"
opensearch_count="$("${compose[@]}" exec -T opensearch curl -fsS \
  -H 'Content-Type: application/json' \
  "http://localhost:9200/$opensearch_index/_search" \
  -d "$search_payload" | jq -er '.hits.total.value')"
(( opensearch_count > 0 )) || {
  printf 'Expected OpenSearch chunks, found: %s\n' "$opensearch_count" >&2
  exit 1
}

printf 'PASS pair=%s document=%s postgres=%s opensearch=%s\n' \
  "$pair_id" "$source_document_id" "$postgres_count" "$opensearch_count"
