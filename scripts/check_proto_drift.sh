#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
LOCAL=PasarGuardNodeBridge/common/service.proto

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

read_lock() { sed -n "s/^$1=//p" proto.lock; }
REPO=$(read_lock repo)
SRC_PATH=$(read_lock path)
PINNED=$(read_lock commit)
[ -n "$REPO" ] && [ -n "$SRC_PATH" ] && [ -n "$PINNED" ] || { echo "proto.lock is incomplete" >&2; exit 1; }
valid_repo "$REPO" || { echo "proto.lock repo is malformed" >&2; exit 1; }
valid_path "$SRC_PATH" || { echo "proto.lock path is malformed" >&2; exit 1; }
[[ $PINNED =~ ^[0-9a-f]{40}$ ]] || { echo "proto.lock commit is not a 40-hex sha" >&2; exit 1; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
fetch() { curl -fsSL --max-time 30 --retry 3 --retry-delay 2 "https://raw.githubusercontent.com/$REPO/$1/$SRC_PATH" -o "$2"; }

fetch "$PINNED" "$WORK/pinned.proto"
if ! diff -u "$WORK/pinned.proto" "$LOCAL" > "$WORK/integrity.diff"; then
  echo "DRIFT: $LOCAL differs from $REPO@$PINNED:$SRC_PATH; run scripts/sync_proto.sh" >&2
  cat "$WORK/integrity.diff" >&2
  exit 1
fi

mkdir -p "$WORK/gen/PasarGuardNodeBridge/common"
cp "$LOCAL" "$WORK/gen/PasarGuardNodeBridge/common/service.proto"
( cd "$WORK/gen" && uv run --frozen --project "$ROOT" -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpclib_python_out=. PasarGuardNodeBridge/common/service.proto )
for f in service_pb2.py service_pb2.pyi service_grpc.py; do
  if ! diff -u "$WORK/gen/PasarGuardNodeBridge/common/$f" "PasarGuardNodeBridge/common/$f" > "$WORK/$f.diff"; then
    echo "STALE STUB: PasarGuardNodeBridge/common/$f does not match protoc output; run make generate_grpc_code" >&2
    cat "$WORK/$f.diff" >&2
    exit 1
  fi
done
echo "integrity ok: $LOCAL matches $REPO@$PINNED and every stub matches protoc output"

if [ "${PROTO_FRESHNESS:-0}" = "1" ]; then
  LATEST=$(git ls-remote "https://github.com/$REPO.git" refs/heads/main | cut -f1)
  [[ $LATEST =~ ^[0-9a-f]{40}$ ]] || { echo "cannot resolve $REPO main" >&2; exit 1; }
  fetch "$LATEST" "$WORK/latest.proto"
  if ! diff -u "$LOCAL" "$WORK/latest.proto" > "$WORK/freshness.diff"; then
    echo "STALE: $REPO main ($LATEST) changed $SRC_PATH since pinned $PINNED; run scripts/sync_proto.sh" >&2
    cat "$WORK/freshness.diff" >&2
    exit 1
  fi
  echo "freshness ok: $LOCAL matches $REPO main ($LATEST)"
fi
