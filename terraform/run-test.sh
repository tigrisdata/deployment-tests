#!/usr/bin/env bash
set -euo pipefail

# run-test.sh
# Run a command on all OCI VMs and fetch a file from each—concurrently.

usage() {
  cat <<'EOF'
Usage:
  run-test.sh --cmd "<remote command>" --remote-file <path/on/vm> --out-dir <local/dir> [--user opc] [--key ~/.ssh/id_rsa] [--bastion bastion_user@bastion_host] [--use-private-ips] [--max-parallel N]

Notes:
  - VM IPs are read from Terraform outputs:
      public_ips (default) or private_ips (--use-private-ips)
  - Requires: terraform, jq, ssh, scp
  - All SSH/SCP run concurrently. Errors are reported per host.

Examples:
  run-test.sh \
    --cmd 'deployment-tests/t3-validator -bucket tigris-consistency-test-bucket -global-endpoint "https://oracle.storage.dev" -regional-endpoints "https://iad.storage.dev,https://ord.storage.dev,https://sjc.storage.dev" > results.log' \
    --remote-file "/home/opc/results.log" \
    --out-dir ./downloads

  # With bastion / jump host:
  run-test.sh \
    --cmd "sudo systemctl status docker --no-pager" \
    --remote-file /tmp/result.txt \
    --out-dir ./dl --bastion admin@1.2.3.4

  # Use private IPs (e.g., when VMs have no public IP and you have a bastion):
  run-test.sh --use-private-ips --bastion admin@bastion.example \
    --cmd "hostname" --remote-file /etc/os-release --out-dir ./dl
EOF
  exit 1
}

# Defaults
USER_NAME="opc"
SSH_KEY="${HOME}/.ssh/id_rsa"
USE_PRIVATE_IPS="false"
BASTION=""
REMOTE_CMD=""
REMOTE_FILE=""
OUT_DIR=""
MAX_PARALLEL=0   # 0 = unlimited (all at once)

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cmd)           shift; REMOTE_CMD="${1:-}";;
    --remote-file)   shift; REMOTE_FILE="${1:-}";;
    --out-dir)       shift; OUT_DIR="${1:-}";;
    --user)          shift; USER_NAME="${1:-}";;
    --key)           shift; SSH_KEY="${1:-}";;
    --bastion)       shift; BASTION="${1:-}";;
    --use-private-ips) USE_PRIVATE_IPS="true";;
    --max-parallel)    shift; MAX_PARALLEL="${1:-0}";;
    -h|--help)       usage;;
    *) echo "Unknown arg: $1" >&2; usage;;
  esac
  shift || true
done

[[ -z "${REMOTE_CMD}"  ]] && echo "Missing --cmd" >&2 && usage
[[ -z "${REMOTE_FILE}" ]] && echo "Missing --remote-file" >&2 && usage
[[ -z "${OUT_DIR}"     ]] && echo "Missing --out-dir" >&2 && usage
if ! [[ "${MAX_PARALLEL}" =~ ^[0-9]+$ ]]; then
  echo "--max-parallel must be a non-negative integer" >&2; exit 2
fi

# Tools check
for t in terraform jq ssh scp; do
  command -v "$t" >/dev/null 2>&1 || { echo "Missing required tool: $t" >&2; exit 2; }
done

mkdir -p "${OUT_DIR}"

# Pull IPs from terraform outputs
OUTPUT_NAME="public_ips"
[[ "${USE_PRIVATE_IPS}" == "true" ]] && OUTPUT_NAME="private_ips"

# terraform output -json returns e.g. ["203.0.113.10","203.0.113.11",""]
IPS_JSON="$(terraform output -json "${OUTPUT_NAME}")"
# Filter empty strings and nulls
mapfile -t HOSTS < <(echo "${IPS_JSON}" | jq -r '.[] | select(. != null and . != "")')

if [[ ${#HOSTS[@]} -eq 0 ]]; then
  echo "No ${OUTPUT_NAME} found (or they're empty)." >&2
  exit 3
fi

# SSH options
SSH_OPTS=(
  -i "${SSH_KEY}"
  -o BatchMode=yes
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o ConnectTimeout=8
  -o ServerAliveInterval=30
  -o ServerAliveCountMax=3
)
SCP_OPTS=("${SSH_OPTS[@]}")

if [[ -n "${BASTION}" ]]; then
  SSH_OPTS+=( -o "ProxyJump=${BASTION}" )
  SCP_OPTS+=( -o "ProxyJump=${BASTION}" )
fi

# Run everything concurrently
pids=()
declare -A host_status

echo "Found ${#HOSTS[@]} hosts from '${OUTPUT_NAME}'. Max parallel: ${MAX_PARALLEL:-0} (0=unlimited)."

if ! [[ "${MAX_PARALLEL}" -eq 0 ]] || ! [[ "${MAX_PARALLEL}" -ge ${#HOSTS[@]} ]]; then
  # Limit, redefine HOSTS list
  HOSTS=("${HOSTS[@]:MAX_PARALLEL}")
fi

for host in "${HOSTS[@]}"; do
  (
    remote="${USER_NAME}@${host}"
    tag="$(echo "${host}" | tr ':/' '_')"

    echo "[${host}] Running command: ${REMOTE_CMD}"
    # Run command; capture stdout/stderr locally for debugging
    # Use bash -lc to support shell features on remote
    if ! ssh "${SSH_OPTS[@]}" "${remote}" "bash -lc -- $(printf '%q' "${REMOTE_CMD}")" >"${OUT_DIR}/${tag}.cmd.out" 2>"${OUT_DIR}/${tag}.cmd.err"; then
      echo "[${host}] ERROR: command failed (see ${OUT_DIR}/${tag}.cmd.err)"
      exit 10
    fi

    # Fetch file
    # Destination file will be named <host>__<basename>
    base="$(basename "${REMOTE_FILE}")"
    dest="${OUT_DIR}/${tag}__${base}"

    echo "[${host}] Downloading: ${REMOTE_FILE} -> ${dest}"
    if ! scp "${SCP_OPTS[@]}" "${remote}:${REMOTE_FILE}" "${dest}" >"${OUT_DIR}/${tag}.scp.out" 2>"${OUT_DIR}/${tag}.scp.err"; then
      echo "[${host}] ERROR: scp failed (see ${OUT_DIR}/${tag}.scp.err)"
      exit 11
    fi

    echo "[${host}] DONE"
  ) &
  pids+=($!)
done

# Wait and summarize
fail=0
for pid in "${pids[@]}"; do
  if ! wait "${pid}"; then
    fail=$((fail+1))
  fi
done

if [[ ${fail} -gt 0 ]]; then
  echo "Completed with ${fail} failures. Check logs in ${OUT_DIR}/ for details." >&2
  exit 1
fi

echo "All hosts completed successfully. Files in: ${OUT_DIR}/"
