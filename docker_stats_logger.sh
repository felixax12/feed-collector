#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${1:-feed_clickhouse}"
INTERVAL="${INTERVAL_S:-10}"
DURATION_MIN="${DURATION_MIN:-30}"
OUTFILE="${OUTFILE:-logs/docker_stats.log}"

mkdir -p "$(dirname "$OUTFILE")"

iterations=$((DURATION_MIN * 60 / INTERVAL))

for ((i=0; i<iterations; i++)); do
  date +"%Y-%m-%d %H:%M:%S" >> "$OUTFILE"
  docker stats --no-stream "$CONTAINER_NAME" >> "$OUTFILE"
  echo "" >> "$OUTFILE"
  sleep "$INTERVAL"
done
