#!/usr/bin/env bash
# Dev helper: prints the root cause of the last failed test run of a module. Usage: scripts/why.sh services/exception-service
f=$(ls "$1"/target/*-reports/TEST-*.xml)
grep -h -A6 "APPLICATION FAILED" $f | head -9 | cut -c1-400
grep -h -E "<(failure|error) " $f | cut -c1-${2:-500} | head -12
grep -h "Caused by:" $f | sort | uniq -c | sort -rn | cut -c1-400 | head -5
