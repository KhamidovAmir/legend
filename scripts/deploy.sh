#!/usr/bin/env bash
# helm upgrade of all services to one environment. Used by both CI systems, so the deploy logic lives in one place.
#   scripts/deploy.sh <env> <image-tag> <registry-prefix> [service ...]
# --atomic: a failed rollout (pods not Ready in time) is rolled back automatically.
set -euo pipefail
env=${1:?environment}; tag=${2:?image tag}; registry=${3:?registry prefix}; shift 3
services=("$@"); [ ${#services[@]} -eq 0 ] && services=(exception-service resolution-service integration-service analytics-service)
for service in "${services[@]}"; do
  echo "==> $service -> demp-$env ($tag)"
  helm upgrade --install "$service" deploy/helm/demp-service \
    --namespace "demp-$env" \
    -f "services/$service/deploy/values.yaml" \
    -f "services/$service/deploy/values-$env.yaml" \
    --set image.repository="$registry/$service" \
    --set image.tag="$tag" \
    --atomic --timeout 10m --history-max 10
done
