#!/usr/bin/env python3
"""Regenerates deploy/platform/prometheus-rule.yaml from deploy/observability/alerts.yml (single source of truth).
With --check it only verifies that the generated file is up to date (used in CI)."""
import sys

import yaml

HEADER = "# GENERATED from deploy/observability/alerts.yml (scripts/render-rules.py). Do not edit by hand.\n"
rules = yaml.safe_load(open("deploy/observability/alerts.yml"))
resource = {
    "apiVersion": "monitoring.coreos.com/v1",
    "kind": "PrometheusRule",
    "metadata": {"name": "demp-alerts", "labels": {"release": "kube-prometheus-stack"}},
    "spec": rules,
}
rendered = HEADER + yaml.safe_dump(resource, sort_keys=False, width=200)
path = "deploy/platform/prometheus-rule.yaml"
if "--check" in sys.argv:
    if open(path).read() != rendered:
        sys.exit(f"{path} is out of date: run scripts/render-rules.py")
    print("prometheus rule is in sync")
else:
    open(path, "w").write(rendered)
