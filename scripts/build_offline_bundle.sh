#!/usr/bin/env bash
#
# Build a self-contained OFFLINE (air-gapped) install bundle for phronexus-core,
# covering multiple Python versions in one wheelhouse.
#
#   scripts/build_offline_bundle.sh [extras] [out_dir]
#
#   extras   comma list (default: aerospike,msk,s3tables,api,client,oidc,otel
#            — i.e. Aerospike + AWS-managed MSK + S3 Tables)
#   out_dir  output dir (default: dist)
#
#   env:
#     PHRONEXUS_PYVERS    space list of Python minors (default "3.11 3.12 3.13 3.14")
#     PHRONEXUS_PLATFORM  cross-build platform tag (e.g. manylinux2014_x86_64).
#                         Unset = the host's native platform (recommended: run
#                         this inside the target's base image, e.g. python:3.11-slim).
#
# The phronexus-core wheel is pure-Python (works on every version); each Python
# minor needs its OWN copy of the COMPILED dependencies (pydantic-core, aerospike,
# confluent-kafka, pyarrow, …). Where upstream ships no wheel for a (Python, extra)
# combo, it is skipped and recorded in MANIFEST.txt — so the bundle stays valid
# for the combos that ARE covered. Read the coverage matrix before shipping.
set -euo pipefail

EXTRAS_CSV="${1:-aerospike,msk,s3tables,api,client,oidc,otel}"
OUTDIR="${2:-dist}"
PYVERS="${PHRONEXUS_PYVERS:-3.11 3.12 3.13 3.14}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PLAT_ARG=""; PLAT_LABEL="native ($(python -c 'import platform;print(platform.system().lower()+"-"+platform.machine().lower())'))"
if [ -n "${PHRONEXUS_PLATFORM:-}" ]; then PLAT_ARG="--platform ${PHRONEXUS_PLATFORM}"; PLAT_LABEL="${PHRONEXUS_PLATFORM}"; fi

NAME="phronexus-offline"
DEST="$OUTDIR/$NAME"; WH="$DEST/wheelhouse"
echo "==> offline bundle | extras=[$EXTRAS_CSV] | python=[$PYVERS] | platform=$PLAT_LABEL"
rm -rf "$DEST"; mkdir -p "$WH"

# 1) the pure-Python phronexus wheel (one copy, valid on every Python version)
( cd "$ROOT" && python -m pip wheel . --no-deps -w "$WH" >/dev/null )
WHEEL="$(ls "$WH"/phronexus_core-*.whl)"

IFS=',' read -ra EXTRAS <<< "$EXTRAS_CSV"
COVER=""
dl() { python -m pip download "$1" --only-binary=:all: --python-version "$2" $PLAT_ARG -d "$WH" >/dev/null 2>&1; }

# 2) per Python version: core deps, then each extra (resilient — a missing extra
#    on one version doesn't abort the others)
for pv in $PYVERS; do
  if dl "$WHEEL" "$pv"; then COVER+=$'\n'"$pv core OK"; else COVER+=$'\n'"$pv core MISSING"; echo "   ! py$pv: core deps unavailable"; fi
  for ex in "${EXTRAS[@]}"; do
    if dl "${WHEEL}[$ex]" "$pv"; then COVER+=$'\n'"$pv $ex OK"
    else COVER+=$'\n'"$pv $ex MISSING"; echo "   ! py$pv: extra '$ex' has no wheels upstream — skipped"; fi
  done
done

# 3) installer for the air-gapped host
cat > "$DEST/install.sh" <<'EOS'
#!/usr/bin/env bash
# Offline install (no network). Usage:  [PYTHON=python3.12] ./install.sh [extras]
#   e.g.  ./install.sh aerospike,msk,s3tables,api,client,oidc,otel
# Pick the interpreter matching this bundle's Python version (see MANIFEST.txt).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
PY="${PYTHON:-}"
[ -z "$PY" ] && for c in python3.12 python3 python; do command -v "$c" >/dev/null 2>&1 && { PY="$c"; break; }; done
spec="phronexus-core"; [ "${1:-}" ] && spec="phronexus-core[$1]"
echo "using $("$PY" --version 2>&1) ($PY)"
"$PY" -m pip install --no-index --find-links "$here/wheelhouse" "$spec"
echo "OK: installed $spec offline."
EOS
chmod +x "$DEST/install.sh"

# 4) manifest with the coverage matrix (only OK combos are installable)
{
  echo "phronexus-core — offline install bundle (multi-Python)"
  echo "built:    $(date -u +%FT%TZ)"
  echo "platform: $PLAT_LABEL"
  echo "python:   $PYVERS"
  echo "install:  ./install.sh [extras]     e.g.  ./install.sh aerospike,api"
  echo
  echo "coverage — install a (python, extras) combo only where every extra is OK:"
  printf '%s\n' "$COVER" | awk 'NF{printf "  py%-6s %-10s %s\n",$1,$2,$3}'
  echo
  echo "wheels: $(ls -1 "$WH" | wc -l | tr -d ' ')"
} > "$DEST/MANIFEST.txt"

tar -C "$OUTDIR" -czf "$DEST.tar.gz" "$NAME"
echo "==> $DEST.tar.gz ($(du -h "$DEST.tar.gz" | cut -f1))"
sed -n '/^coverage/,/^wheels/p' "$DEST/MANIFEST.txt"
