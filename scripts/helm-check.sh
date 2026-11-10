#!/usr/bin/env bash
# Lints / renders the common chart with the values of every service and environment.
#   scripts/helm-check.sh lint
#   scripts/helm-check.sh template dev > /tmp/dev.yaml
# Uses the local "helm" binary or, if absent, the alpine/helm image (HELM_IMAGE).
set -euo pipefail
mode=${1:-lint}; only_env=${2:-}
if command -v helm > /dev/null; then HELM=(helm); else
  HELM=(docker run --rm -v "$PWD:/work" -w /work "${HELM_IMAGE:-alpine/helm:3.16.4}")
fi
for dir in services/*/deploy; do
  svc=$(basename "$(dirname "$dir")")
  for env in dev test preprod prod; do
    [ -n "$only_env" ] && [ "$env" != "$only_env" ] && continue
    args=(deploy/helm/demp-service -f "$dir/values.yaml" -f "$dir/values-$env.yaml" --set image.tag=0.0.0-check)
    if [ "$mode" = lint ]; then
      "${HELM[@]}" lint --strict "${args[@]}" > /tmp/helm-lint.out 2>&1 || { cat /tmp/helm-lint.out; echo "FAILED: $svc/$env"; exit 1; }
      echo "ok  $svc/$env"
    else
      "${HELM[@]}" template "$svc" "${args[@]}" --namespace "demp-$env"
    fi
  done
done
