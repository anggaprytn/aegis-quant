#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$REPO_ROOT"

echo "Rebuilding scheduled research runner image..."
docker compose -f infra/docker-compose.yml --env-file .env --profile research-scheduler build scheduled-research-runner

echo "Recreating scheduled research runner container..."
docker compose -f infra/docker-compose.yml --env-file .env --profile research-scheduler up -d --no-deps --force-recreate scheduled-research-runner

echo "Scheduled research runner status:"
docker ps --filter "name=aegis-quant-scheduled-research-runner"

echo "Recent scheduled research runner logs:"
docker logs aegis-quant-scheduled-research-runner --tail=80
