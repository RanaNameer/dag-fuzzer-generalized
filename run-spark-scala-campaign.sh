#!/usr/bin/env bash
# Runs the classic spark-scala DAGger target as a series of short-lived JVM
# "slices" instead of one long-lived process.
#
# Why: SparkCodeExecutor compiles and evals every generated program's Scala
# source via scala.tools.reflect.ToolBox, inside the same JVM as the fuzzer
# itself. Repeated dynamic compilation in one long-lived JVM is a known
# Metaspace/classloader leak in the Scala reflection Toolbox (the compiler's
# internal global symbol tables retain references that keep old classloaders
# from ever becoming collectable) - eventually Metaspace is exhausted and
# executions start failing. Restarting the JVM is the only way to reclaim it,
# since spark.catalog.clearCache() only clears Spark's own data cache, not
# JVM class metadata.
#
# Each slice gets its own output subdirectory, so nothing needs to change in
# the fuzzer itself (MainFuzzer already wipes+creates its own --out-dir on
# startup - pointing every slice at a fresh, unique path makes that a no-op,
# and no slice can ever overwrite another slice's findings by filename
# collision).
#
# Usage:
#   ./run-spark-scala-campaign.sh <total-minutes> <slice-minutes> [campaign-name]
#
# Example: run for 3 hours total, restarting the JVM every 15 minutes:
#   ./run-spark-scala-campaign.sh 180 15

set -euo pipefail

TOTAL_MINUTES="${1:?Usage: $0 <total-minutes> <slice-minutes> [campaign-name]}"
SLICE_MINUTES="${2:?Usage: $0 <total-minutes> <slice-minutes> [campaign-name]}"
CAMPAIGN_NAME="${3:-campaign-$(date +%Y%m%d-%H%M%S)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

JAR="target/scala-2.13/DAGFuzzerBetter-assembly-0.1.0-SNAPSHOT.jar"
if [ ! -f "$JAR" ]; then
  echo "Jar not found at $JAR - run 'sbt assembly' first." >&2
  exit 1
fi
if ! compgen -G "lib/*.jar" > /dev/null; then
  echo "No jars found under lib/ - spark-sql is a 'provided' dependency and must be" >&2
  echo "supplied at runtime (e.g. a symlink to a Spark distribution's jars/ dir)." >&2
  exit 1
fi

BASE_OUT_DIR="target/dagfuzz-out/spark-scala/${CAMPAIGN_NAME}"
LOG_DIR="${BASE_OUT_DIR}/slice-logs"
mkdir -p "$LOG_DIR"

TOTAL_SECONDS=$((TOTAL_MINUTES * 60))
SLICE_SECONDS=$((SLICE_MINUTES * 60))
START_TIME=$(date +%s)
SLICE_NUM=0

echo "=== Campaign '$CAMPAIGN_NAME' starting: ${TOTAL_MINUTES}min total, ${SLICE_MINUTES}min per JVM slice ==="
echo "Output: $BASE_OUT_DIR"
echo

while true; do
  NOW=$(date +%s)
  ELAPSED=$((NOW - START_TIME))
  REMAINING=$((TOTAL_SECONDS - ELAPSED))

  if [ "$REMAINING" -le 0 ]; then
    echo "=== Overall time budget exhausted after $SLICE_NUM slice(s). Stopping. ==="
    break
  fi

  THIS_SLICE_SECONDS=$SLICE_SECONDS
  if [ "$REMAINING" -lt "$SLICE_SECONDS" ]; then
    THIS_SLICE_SECONDS=$REMAINING
  fi

  SLICE_OUT_DIR="${BASE_OUT_DIR}/slice-${SLICE_NUM}"
  SLICE_SEED="slice-${SLICE_NUM}-$(date +%s%N)"
  SLICE_LOG="${LOG_DIR}/slice-${SLICE_NUM}.log"

  echo "--- Slice $SLICE_NUM: up to ${THIS_SLICE_SECONDS}s, out-dir=$SLICE_OUT_DIR ---"

  java -cp "$JAR:lib/*" fuzzer.MainFuzzer spark-scala \
    --exit-after-n-successes false \
    --time-limit-sec "$THIS_SLICE_SECONDS" \
    --seed "$SLICE_SEED" \
    --out-dir "$SLICE_OUT_DIR" \
    > "$SLICE_LOG" 2>&1 \
    || echo "  (slice $SLICE_NUM exited non-zero - see $SLICE_LOG - continuing to next slice)"

  tail -5 "$SLICE_LOG" | sed 's/^/  | /'

  SLICE_NUM=$((SLICE_NUM + 1))
done

echo
echo "=== Campaign summary across $SLICE_NUM slice(s) ==="
declare -A TOTALS
for slice_dir in "$BASE_OUT_DIR"/slice-*/; do
  [ -d "$slice_dir" ] || continue
  for rtype_dir in "$slice_dir"*/; do
    [ -d "$rtype_dir" ] || continue
    rtype="$(basename "$rtype_dir")"
    [ "$rtype" = "live-stats" ] && continue
    count=$(find "$rtype_dir" -maxdepth 1 -name "*.scala" | wc -l)
    TOTALS["$rtype"]=$(( ${TOTALS["$rtype"]:-0} + count ))
  done
done
for rtype in "${!TOTALS[@]}"; do
  echo "  $rtype: ${TOTALS[$rtype]}"
done
echo
echo "Per-slice logs: $LOG_DIR"
echo "All generated programs: $BASE_OUT_DIR/slice-*/<ResultType>/*.scala"
