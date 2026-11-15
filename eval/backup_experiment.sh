#!/usr/bin/env bash
# Backup-task experiment: {no straggler, straggler} x {-B off, -B on}, TRIALS each,
# on /input/big with -m 16 -r 4. Writes eval/results/backup.csv.
# Needs the cluster's images built and /input/big uploaded:
#   python3 upload_data -f <big csv dir> -d /input/big
# Env: BOSS_URL (default http://localhost:5000), PYTHON (default python3), TRIALS (3).
set -euo pipefail
cd "$(dirname "$0")/.."

export BOSS_URL=${BOSS_URL:-http://localhost:5000}
PY=${PYTHON:-python3}
TRIALS=${TRIALS:-3}
OUT=eval/results/backup.csv
RUN=$(date +%Y%m%d-%H%M%S)

mkdir -p eval/results
echo "config,trial,seconds,backups_launched,backups_won,wasted_seconds" > "$OUT"

for straggler in no yes; do
  if [ "$straggler" = yes ]; then
    files=(-f docker-compose.yml -f docker-compose.straggler.yml)
  else
    files=(-f docker-compose.yml)
  fi
  docker compose "${files[@]}" up -d >/dev/null
  until curl -sf "$BOSS_URL/workers" >/dev/null; do sleep 1; done
  sleep 3                                   # let recreated workers reconnect
  # untimed warm-up: the first job on fresh containers is much slower (cold start)
  "$PY" ./submit -i /input/big -o "/output/bexp-$RUN-warmup-s$straggler" \
    -j examples/avg_temp.py -m 16 -r 4 >/dev/null

  for backup in off on; do
    config="straggler-$straggler+backup-$backup"
    flag=(); [ "$backup" = on ] && flag=(-B)
    for trial in $(seq 1 "$TRIALS"); do
      echo "== $config trial $trial"
      out="/output/bexp-$RUN-s$straggler-b$backup-$trial"
      job=$("$PY" ./submit -i /input/big -o "$out" -j examples/avg_temp.py -m 16 -r 4 \
              "${flag[@]+"${flag[@]}"}" | tee /dev/stderr | awk '/^submitted job/{print $3}')
      curl -s "$BOSS_URL/job/$job" | "$PY" -c '
import json, sys
j = json.load(sys.stdin)
print(",".join(str(v) for v in [sys.argv[1], sys.argv[2], j["elapsed"],
      j["backups_launched"], j["backups_won"], j["wasted_seconds"]]))' \
        "$config" "$trial" | tee -a "$OUT"
      echo "   events: shared/jobs/$job/events.jsonl"
    done
  done
done

docker compose up -d >/dev/null             # back to 4 equal workers
echo "wrote $OUT"
