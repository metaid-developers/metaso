#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy-prod-indexer.sh <image-tag>

Deploy ghcr.io/metaso-labs/man-indexer:<image-tag> to the current show.now
production host. The script only recreates the indexer service; it never restarts
server or man-mongodb.

Environment overrides:
  METASO_PROD_SSH=root@47.238.89.188
  METASO_IMAGE=ghcr.io/metaso-labs/man-indexer
  METASO_COMPOSE_FILE=/root/metaso/docker-compose.yml
  METASO_COMPOSE_BIN=docker-compose
  METASO_EXPECTED_DIGEST=sha256:...
  METASO_MAX_CURSOR_LAG=20
  METASO_SKIP_API_CHECKS=1
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
  usage >&2
  exit 64
fi

if [[ ! "$TAG" =~ ^[0-9A-Za-z][0-9A-Za-z._-]*$ ]]; then
  echo "Invalid image tag: $TAG" >&2
  exit 64
fi

PROD_SSH="${METASO_PROD_SSH:-root@47.238.89.188}"
IMAGE="${METASO_IMAGE:-ghcr.io/metaso-labs/man-indexer}"
COMPOSE_FILE="${METASO_COMPOSE_FILE:-/root/metaso/docker-compose.yml}"
COMPOSE_BIN="${METASO_COMPOSE_BIN:-docker-compose}"
EXPECTED_DIGEST="${METASO_EXPECTED_DIGEST:-}"
MAX_CURSOR_LAG="${METASO_MAX_CURSOR_LAG:-20}"
SKIP_API_CHECKS="${METASO_SKIP_API_CHECKS:-0}"

if [[ ! "$IMAGE" =~ ^[0-9A-Za-z./:_-]+$ ]]; then
  echo "Invalid image name: $IMAGE" >&2
  exit 64
fi

if [[ ! "$COMPOSE_FILE" =~ ^/[0-9A-Za-z./_-]+$ ]]; then
  echo "METASO_COMPOSE_FILE must be an absolute path without shell metacharacters" >&2
  exit 64
fi

if [[ ! "$COMPOSE_BIN" =~ ^[0-9A-Za-z./_-]+$ ]]; then
  echo "METASO_COMPOSE_BIN contains unsupported characters" >&2
  exit 64
fi

if [[ ! "$MAX_CURSOR_LAG" =~ ^[0-9]+$ ]]; then
  echo "METASO_MAX_CURSOR_LAG must be a non-negative integer" >&2
  exit 64
fi

if [[ -n "$EXPECTED_DIGEST" && ! "$EXPECTED_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "METASO_EXPECTED_DIGEST must look like sha256:<64 hex chars>" >&2
  exit 64
fi

API_NEWEST="https://www.show.now/man/social/buzz/newest?size=10&lastId="
API_RECOMMENDED="https://www.show.now/man/social/buzz/recommended?size=10&lastId=&userAddress=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ"
API_OPCAT_PIN="https://www.show.now/man/api/pin/c0a57f10c8e8d990d974d541bd7d86fc5f3eff05fb08f602c5b3c0e5b4f80fddi0"

SSH_OPTS=(
  -o StrictHostKeyChecking=no
  -o ConnectTimeout=10
  -o ServerAliveInterval=10
  -o ServerAliveCountMax=3
)

log() {
  printf '\n==> %s\n' "$*"
}

api_check() {
  local label="$1"
  local url="$2"
  local output

  output="$(curl -sS -o /dev/null -w "${label} http=%{http_code} time=%{time_total}" "$url")"
  echo "$output"
  if [[ "$output" != *"http=200"* ]]; then
    echo "API check failed: $label" >&2
    return 1
  fi
}

api_checks() {
  local phase="$1"

  if [[ "$SKIP_API_CHECKS" == "1" ]]; then
    echo "Skipping API checks because METASO_SKIP_API_CHECKS=1"
    return 0
  fi

  api_check "$phase newest" "$API_NEWEST"
  api_check "$phase recommended" "$API_RECOMMENDED"
  api_check "$phase opcat_pin" "$API_OPCAT_PIN"
}

if command -v docker >/dev/null 2>&1; then
  log "Inspecting image locally"
  docker buildx imagetools inspect "${IMAGE}:${TAG}" >/dev/null
else
  echo "docker is not available locally; remote docker pull will be the image existence check."
fi

log "Pre-deploy external API baseline"
api_checks "pre"

log "Remote precheck and deploy"
ssh "${SSH_OPTS[@]}" "$PROD_SSH" \
  "bash -s -- '$TAG' '$IMAGE' '$COMPOSE_FILE' '$COMPOSE_BIN' '$EXPECTED_DIGEST' '$MAX_CURSOR_LAG'" <<'REMOTE'
set -euo pipefail

TAG="$1"
IMAGE="$2"
COMPOSE_FILE="$3"
COMPOSE_BIN="$4"
EXPECTED_DIGEST="$5"
MAX_CURSOR_LAG="$6"

MONGO_URI='mongodb://test:test@127.0.0.1:27017/man-indexer?authSource=admin'
MONGO_KEYS_REGEX='^(btcChainSyncHeight|btc_del_mempool_height|mvcChainSyncHeight|mvc_del_mempool_height|opcatChainSyncHeight|opcat_del_mempool_height)$'

log_remote() {
  printf '\n[remote] %s\n' "$*"
}

mongo_cursor_eval() {
  docker exec man-mongodb mongosh --quiet "$MONGO_URI" --eval "$1"
}

check_cursor_lag() {
  mongo_cursor_eval "
const maxLag = Number(${MAX_CURSOR_LAG});
const keys = [
  'btcChainSyncHeight',
  'btc_del_mempool_height',
  'mvcChainSyncHeight',
  'mvc_del_mempool_height',
  'opcatChainSyncHeight',
  'opcat_del_mempool_height',
];
const values = {};
db.sync_lastid_log.find({key:{\$in:keys}}).forEach(x => values[x.key] = Number(x.lastnumber));
for (const chain of ['btc', 'mvc', 'opcat']) {
  const sync = values[chain + 'ChainSyncHeight'];
  const del = values[chain + '_del_mempool_height'];
  if (!Number.isFinite(sync) || !Number.isFinite(del)) {
    throw new Error('missing sync cursor for ' + chain);
  }
  const lag = sync - del;
  print(chain + ' sync=' + sync + ' del=' + del + ' lag=' + lag);
  if (lag > maxLag) {
    throw new Error(chain + ' mempool cleanup cursor lag ' + lag + ' exceeds max ' + maxLag);
  }
}
"
}

print_cursors() {
  mongo_cursor_eval "db.sync_lastid_log.find({key:/$(printf '%s' "$MONGO_KEYS_REGEX")/}).sort({key:1}).forEach(x=>printjson(x))"
}

log_remote "precheck container"
command -v "$COMPOSE_BIN" >/dev/null
command -v python3 >/dev/null
docker inspect man-indexer --format 'image={{.Config.Image}} started={{.State.StartedAt}} status={{.State.Status}} restart={{.RestartCount}} oom={{.State.OOMKilled}} exit={{.State.ExitCode}}'
docker stats --no-stream man-indexer --format 'mem={{.MemUsage}} cpu={{.CPUPerc}}'
df -h / | tail -1
grep -n "$IMAGE" "$COMPOSE_FILE"

log_remote "precheck cursors"
print_cursors
check_cursor_lag

log_remote "precheck recent errors"
docker logs --since 10m man-indexer 2>&1 | grep -Ei 'panic|fatal|error|failed|exception|out of memory|oom|killed' | tail -80 || true

log_remote "backup"
TS="$(date +%Y%m%d-%H%M%S)"
BK="/root/deploy-backups/man-indexer-$TS"
mkdir -p "$BK"

cp -a "$COMPOSE_FILE" "$BK/docker-compose.yml"
cp -a /root/metaso/.env "$BK/env"
cp -a /metaso/manconfig.toml "$BK/manconfig.toml"
"$COMPOSE_BIN" -f "$COMPOSE_FILE" config > "$BK/docker-compose.rendered.yml" 2> "$BK/docker-compose.config.stderr"
docker inspect man-indexer > "$BK/man-indexer.inspect.before.json"
docker logs --tail 1000 man-indexer > "$BK/man-indexer.logs.before.txt" 2>&1
docker ps --format '{{.ID}} {{.Image}} {{.Names}} {{.Ports}}' > "$BK/docker-ps.before.txt"
print_cursors > "$BK/sync_lastid_log.before.json"

CURRENT_IMAGE="$(docker inspect man-indexer --format '{{.Config.Image}}')"
ROLLBACK_IMAGE="$IMAGE:rollback-$TS"
echo "$CURRENT_IMAGE" > "$BK/current-indexer-image.txt"
docker image inspect "$CURRENT_IMAGE" > "$BK/current-image.inspect.json"
docker image tag "$CURRENT_IMAGE" "$ROLLBACK_IMAGE"

echo "backup_dir=$BK"
echo "rollback_image=$ROLLBACK_IMAGE"

log_remote "pull image"
for attempt in 1 2 3; do
  echo "pull_attempt=$attempt"
  if docker pull "$IMAGE:$TAG"; then
    break
  fi
  if [[ "$attempt" == "3" ]]; then
    echo "docker pull failed after $attempt attempts" >&2
    exit 1
  fi
  sleep 5
done

PULLED_ID="$(docker image inspect "$IMAGE:$TAG" --format '{{.Id}}')"
echo "pulled_image=$PULLED_ID"
if [[ -n "$EXPECTED_DIGEST" ]]; then
  docker image inspect "$IMAGE:$TAG" --format '{{range .RepoDigests}}{{println .}}{{end}}' | grep -F "@$EXPECTED_DIGEST" >/dev/null
  echo "expected_digest=$EXPECTED_DIGEST"
fi

log_remote "update compose"
cp -a "$COMPOSE_FILE" "$COMPOSE_FILE.pre-indexer-$TAG"
python3 - "$COMPOSE_FILE" "$IMAGE" "$TAG" <<'PY'
import pathlib
import sys

compose_file = pathlib.Path(sys.argv[1])
image = sys.argv[2]
tag = sys.argv[3]
lines = compose_file.read_text().splitlines(keepends=True)
changed = 0
next_lines = []
for line in lines:
    if "image:" in line and image in line:
        prefix = line.split("image:", 1)[0]
        next_lines.append(f"{prefix}image:  {image}:{tag}\n")
        changed += 1
    else:
        next_lines.append(line)
if changed != 1:
    raise SystemExit(f"expected exactly one {image} image line, changed {changed}")
compose_file.write_text("".join(next_lines))
PY
grep -n "$IMAGE" "$COMPOSE_FILE"

log_remote "recreate indexer only"
"$COMPOSE_BIN" -f "$COMPOSE_FILE" up -d --no-deps --force-recreate indexer

log_remote "postcheck container"
docker inspect man-indexer --format 'image={{.Config.Image}} started={{.State.StartedAt}} status={{.State.Status}} restart={{.RestartCount}} oom={{.State.OOMKilled}} exit={{.State.ExitCode}}'
docker stats --no-stream man-indexer --format 'mem={{.MemUsage}} cpu={{.CPUPerc}}'

log_remote "postcheck cursors"
print_cursors
check_cursor_lag

log_remote "postcheck recent errors"
docker logs --since 3m man-indexer 2>&1 | grep -Ei 'panic|fatal|error|failed|exception|out of memory|oom|killed' | tail -80 || true

cat <<EOF

rollback_compose:
  cp -a $BK/docker-compose.yml $COMPOSE_FILE
  $COMPOSE_BIN -f $COMPOSE_FILE up -d --no-deps --force-recreate indexer
rollback_image:
  python3 - "$COMPOSE_FILE" "$IMAGE" "rollback-$TS" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
image = sys.argv[2]
tag = sys.argv[3]
lines = []
changed = 0
for line in p.read_text().splitlines(keepends=True):
    if "image:" in line and image in line:
        lines.append(line.split("image:", 1)[0] + f"image:  {image}:{tag}\n")
        changed += 1
    else:
        lines.append(line)
if changed != 1:
    raise SystemExit(f"expected exactly one image line, changed {changed}")
p.write_text("".join(lines))
PY
  $COMPOSE_BIN -f $COMPOSE_FILE up -d --no-deps --force-recreate indexer
EOF
REMOTE

log "Post-deploy external API checks"
api_checks "post"

log "60-second external API recheck"
sleep 60
api_checks "recheck"

log "Deployed ${IMAGE}:${TAG}"
