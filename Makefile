COMPOSE := docker compose -f deploy/local/docker-compose.yml
MVN     := mvn -B -ntp

.PHONY: help build unit verify up up-apps down connector run-% demo logs helm-lint helm-template

help: ## show targets
	@grep -E '^[a-zA-Z%_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

build: ## compile and package all modules without tests
	$(MVN) -DskipTests -Djacoco.skip package

unit: ## unit tests only (no Docker needed)
	$(MVN) verify -DskipITs

verify: ## unit + integration tests (Testcontainers) + JaCoCo coverage gate
	$(MVN) verify

up: ## local infrastructure: postgres, kafka, connect, delivery stub, prometheus, grafana, jaeger
	$(COMPOSE) up -d --wait

up-apps: build ## infrastructure + all four services in containers + Debezium connector
	$(COMPOSE) --profile apps up -d --build --wait
	$(COMPOSE) run --rm --no-deps connect-init

down: ## stop everything and drop volumes
	$(COMPOSE) --profile apps --profile tools down -v

connector: ## register the Debezium outbox connector (resolution-service must have started once)
	$(COMPOSE) run --rm --no-deps connect-init

run-%: ## run one service on the host, e.g. make run-exception-service
	$(MVN) -pl services/$* -am -DskipTests -Djacoco.skip install -q
	java -jar services/$*/target/$*.jar

demo: ## send a DeliveryFailed event for a random shipment
	bash scripts/demo.sh failed SHP-$$RANDOM

logs: ## follow service logs (apps profile)
	$(COMPOSE) --profile apps logs -f exception-service resolution-service integration-service analytics-service

helm-lint: ## lint the chart with values of every service and environment
	bash scripts/helm-check.sh lint

helm-template: ## render manifests of every service for an environment: make helm-template ENV=dev
	bash scripts/helm-check.sh template $(ENV)
