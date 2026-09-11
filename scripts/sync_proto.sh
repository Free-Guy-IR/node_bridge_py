#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=${PROTO_REPO:-Free-Guy-IR/node}
SRC_PATH=${PROTO_PATH:-common/service.proto}
REF=${1:-main}
DEST=PasarGuardNodeBridge/common/service.proto

valid_repo() {
  local r=$1
  [[ $r =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || return 1
  if [[ $r =~ (^|/)\.\.?(/|$) ]]; then return 1; fi
  return 0
}

valid_path() {
  local p=$1
  [ -n "$p" ] || return 1
  case "$p" in /*|*/|*//*|*'\\'*|*'%'*|*'?'*|*'#'*|*' '*) return 1;; esac
  if [[ $p =~ (^|/)\.\.?(/|$) ]]; then return 1; fi
  [[ $p =~ ^[A-Za-z0-9_./-]+$ ]] || return 1
  return 0
}

valid_repo "$REPO" || { echo "PROTO_REPO is malformed" >&2; exit 1; }
valid_path "$SRC_PATH" || { echo "PROTO_PATH is malformed" >&2; exit 1; }

resolve_commit() {
  local ref=$1 url="https://github.com/$REPO.git" out head_oid tag_oid ltag_oid
  if [[ $ref =~ ^[0-9a-f]{40}$ ]]; then
    printf '%s' "$ref"
    return
  fi
  out=$(git ls-remote "$url" "refs/heads/$ref" "refs/tags/$ref" "refs/tags/$ref^{}")
  head_oid=$(printf '%s\n' "$out" | awk '$2 ~ /^refs\/heads\// {print $1; exit}')
  tag_oid=$(printf '%s\n' "$out" | awk '$2 ~ /\^\{\}$/ {print $1; exit}')
  ltag_oid=$(printf '%s\n' "$out" | awk '$2 ~ /^refs\/tags\// {print $1; exit}')
  printf '%s' "${head_oid:-${tag_oid:-${ltag_oid:-}}}"
}

COMMIT=$(resolve_commit "$REF")
[[ $COMMIT =~ ^[0-9a-f]{40}$ ]] || { echo "cannot resolve $REF in $REPO to a commit sha" >&2; exit 1; }

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
curl -fsSL --max-time 30 --retry 3 --retry-delay 2 "https://raw.githubusercontent.com/$REPO/$COMMIT/$SRC_PATH" -o "$TMP"
mv "$TMP" "$DEST"
trap - EXIT
printf 'repo=%s\npath=%s\ncommit=%s\n' "$REPO" "$SRC_PATH" "$COMMIT" > proto.lock
make generate_grpc_code
echo "synced $DEST from $REPO@$COMMIT"
git status --short "$DEST" proto.lock PasarGuardNodeBridge/common
