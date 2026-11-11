#!/bin/sh
# Runs the two load variants from SPEC §7.4 against the normal `api` service (TEST_MODE off) and
# writes everything to results/load.txt, with docker stats snapshots taken during each run.
#
#   sh loadtest/run.sh            # 60 s per variant
#   DURATION=10 sh loadtest/run.sh
#
# Scaling experiment (the variables must be exported for the whole script, not just for `up`):
#   export WEB_CONCURRENCY=4 STREAM_SECRET=some-shared-secret DB_POOL_MAX=10
#   docker compose up -d api && OUT=results/load_workers4.txt sh loadtest/run.sh
set -u
cd "$(dirname "$0")/.."
DURATION="${DURATION:-60}"
OUT="${OUT:-results/load.txt}"
# Container ids are looked up, not hard-coded: their names depend on the directory name.
API_C=$(docker compose ps -q api); DB_C=$(docker compose ps -q db)
[ -n "$API_C" ] && [ -n "$DB_C" ] || { echo "api/db are not running: docker compose up -d" >&2; exit 1; }

# Total CPU time a container has used so far, in microseconds (cgroup v2 counter).
cpu_usec() { docker exec "$1" cat /sys/fs/cgroup/cpu.stat | awk '/^usage_usec/ {print $2}'; }
mem_peak_mib() { docker exec "$1" cat /sys/fs/cgroup/memory.peak 2>/dev/null | awk '{printf "%.0f", $1/1048576}'; }

snapshot() {
  echo "--- docker stats, $1"
  docker stats --no-stream --format '{{.Name}}\tcpu {{.CPUPerc}}\tmem {{.MemUsage}}' \
    | grep -E -- '-(api-1|db-1|tests-run)' | sort
}

{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "host: $(nproc) cpus, $(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo) RAM; load generator, api and db all on this one machine"
  echo "api: $(docker exec "$API_C" sh -c 'echo "uvicorn workers=$WEB_CONCURRENCY, db pool max per worker=$DB_POOL_MAX"')"
  echo
  for BATCH in 10 1; do
    snapshot "idle, before batch=$BATCH"
    API0=$(cpu_usec "$API_C"); DB0=$(cpu_usec "$DB_C"); T0=$(date +%s.%N)
    docker compose run --rm -T tests python loadtest/load.py --batch "$BATCH" --duration "$DURATION" 2>&1 \
      | grep -v 'Container' > "results/.load_$BATCH.tmp" &
    PID=$!
    sleep $((DURATION / 3 + 4)); snapshot "batch=$BATCH, about 1/3 into the run"
    sleep $((DURATION / 3));     snapshot "batch=$BATCH, about 2/3 into the run"
    wait "$PID"
    API1=$(cpu_usec "$API_C"); DB1=$(cpu_usec "$DB_C"); T1=$(date +%s.%N)
    cat "results/.load_$BATCH.tmp"; rm -f "results/.load_$BATCH.tmp"
    awk -v a0="$API0" -v a1="$API1" -v d0="$DB0" -v d1="$DB1" -v t0="$T0" -v t1="$T1" 'BEGIN {
      w = t1 - t0
      printf "cpu over the whole run (cgroup counters, %.0f s incl. setup and drain; 100 %% = one core): api %.1f %%, db %.1f %%\n", w, (a1-a0)/w/10000, (d1-d0)/w/10000 }'
    echo "peak memory since container start: api $(mem_peak_mib "$API_C") MiB, db $(mem_peak_mib "$DB_C") MiB"
    # Note: the list ends with a 0 that is this probe's own shell, and with several workers it
    # starts with the idle supervisor process.
    echo "cpu seconds per api process since container start (shows how evenly workers shared the load): $(docker exec "$API_C" sh -c 'for p in /proc/[0-9]*; do grep -q uvicorn $p/cmdline 2>/dev/null || grep -q multiprocessing $p/cmdline 2>/dev/null || continue; awk "{printf \"%.0f \", (\$14+\$15)/100}" $p/stat; done')"
    echo
  done
  # Printed again at the end: `docker compose run` recreates the api container if the settings in
  # the environment differ from the ones it was started with, which would silently change what
  # was measured. (That happened once: a "4 worker" run that was really 1 worker.)
  echo "api at end of run: $(docker exec "$API_C" sh -c 'echo "uvicorn workers=$WEB_CONCURRENCY, db pool max per worker=$DB_POOL_MAX, up since $(date -u -d @$(stat -c %Y /proc/1) +%H:%M:%SZ)"')"
} 2>&1 | tee "$OUT"
