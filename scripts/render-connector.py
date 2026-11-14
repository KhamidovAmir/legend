#!/usr/bin/env python3
"""Checks that deploy/platform/debezium-connector.yaml matches the connector JSON kept next to the service.
The JSON is the source of truth (it is what developers review); CI fails if the two drift apart."""
import json
import sys

import yaml

src = json.load(open("services/resolution-service/deploy/debezium/resolution-outbox-connector.json"))
dst = yaml.safe_load(open("deploy/platform/debezium-connector.yaml"))["spec"]["config"]
ignored = {"connector.class", "tasks.max", "database.hostname", "database.port", "database.user", "database.password"}
diff = {k for k in set(src) | set(dst) if k not in ignored and str(src.get(k)) != str(dst.get(k))}
if diff:
    sys.exit(f"connector config drift in keys: {sorted(diff)}")
print("connector config is in sync")
