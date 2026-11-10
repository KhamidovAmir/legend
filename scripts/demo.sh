#!/usr/bin/env bash
# Plays the external Delivery system on the local stand.
#   scripts/demo.sh failed  SHP-1           DeliveryFailed event -> exception is created, auto-redelivery starts
#   scripts/demo.sh failed  TIMEOUT-SHP-2   the stub does not answer the retry command -> UNKNOWN -> reconciliation
#   scripts/demo.sh result  <externalRequestId|-> [idempotencyKey]   async "DeliveryRetryCompleted"
#   scripts/demo.sh show    SHP-1           state in all services
set -euo pipefail
KAFKA_CONTAINER=${KAFKA_CONTAINER:-demp-kafka-1}
produce() { # topic key json
  printf '%s|%s\n' "$2" "$3" | docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$1" --property parse.key=true --property key.separator='|'
}
case "${1:-}" in
  failed)
    shipment=${2:?shipment id}
    event_id=$(cat /proc/sys/kernel/random/uuid)
    produce external.delivery.events "$shipment" "{\"eventId\":\"$event_id\",\"eventType\":\"DeliveryFailed\",\"sourceSystem\":\"DELIVERY\",\"entityType\":\"SHIPMENT\",\"entityId\":\"$shipment\",\"region\":\"MSK\",\"occurredAt\":\"$(date -u +%FT%TZ)\",\"correlationId\":\"demo-$shipment\",\"payload\":{\"reason\":\"RECIPIENT_UNAVAILABLE\"}}"
    echo "DeliveryFailed sent: shipment=$shipment eventId=$event_id correlationId=demo-$shipment"
    ;;
  result)
    request=${2:?external request id or -}
    key=${3:-}
    produce external.delivery.results "$request" "{\"externalRequestId\":\"$request\",\"idempotencyKey\":\"$key\",\"status\":\"COMPLETED\",\"result\":\"DELIVERY_RETRY_SCHEDULED\"}"
    echo "DeliveryRetryCompleted sent: externalRequestId=$request"
    ;;
  show)
    shipment=${2:?shipment id}
    q() { docker exec demp-postgres-1 psql -U postgres -d "$1" -c "$2"; }
    q exception_db "select id, exception_type, status, priority, resolution, close_reason from exception_record where business_correlation_key like '%:$shipment:%'"
    id=$(docker exec demp-postgres-1 psql -U postgres -d exception_db -Atc "select id from exception_record where business_correlation_key like '%:$shipment:%' order by created_at desc limit 1")
    [ -z "$id" ] && exit 0
    q exception_db "select event, from_status, to_status, actor, created_at from exception_history where exception_id = '$id' order by created_at"
    q resolution_db "select status, policy_code from workflow_instance where exception_id = '$id'"
    q resolution_db "select attempt_no, status, operation_id, idempotency_key from action_attempt where exception_id = '$id' order by attempt_no"
    q resolution_db "select status, result, checks_count, next_check_at from reconciliation_task where exception_id = '$id'"
    q resolution_db "select type, status, description from work_item where exception_id = '$id'"
    q integration_db "select kind, status, external_request_id, result from integration_operation where exception_id = '$id' order by created_at"
    q analytics_db "select status, automated, business_attempts, unknown_count, work_items, sla_breached from exception_fact where exception_id = '$id'"
    ;;
  *)
    sed -n '2,7p' "$0"; exit 1 ;;
esac
