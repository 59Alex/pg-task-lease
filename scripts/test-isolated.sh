#!/usr/bin/env bash
# A fresh PostgreSQL for every invocation. Only the container created here is stopped.
set -euo pipefail
cd "$(dirname "$0")/.."
container_id=
cleanup() {
  if [[ -n "$container_id" ]]; then docker stop "$container_id" >/dev/null; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
container_id=$(docker run --rm -d --label io.github.59alex.fixture=pg-task-lease \
  --tmpfs /var/lib/postgresql/data:rw,size=268435456 \
  -e POSTGRES_DB=pg_task_lease_test -e POSTGRES_USER=lease_test \
  -e POSTGRES_PASSWORD=lease_test_only -p 127.0.0.1::5432 postgres:17-alpine)
ready=false
for attempt in {1..60}; do
  if docker exec "$container_id" pg_isready -U lease_test -d pg_task_lease_test >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  docker logs "$container_id"
  echo 'Disposable PostgreSQL did not become ready' >&2
  exit 1
fi
port=$(docker port "$container_id" 5432/tcp)
export PG_TASK_TEST_URL="r2dbc:postgresql://127.0.0.1:${port##*:}/pg_task_lease_test"
export PG_TASK_TEST_USER=lease_test PG_TASK_TEST_PASSWORD=lease_test_only
./gradlew build --rerun-tasks "$@"
