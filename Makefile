COMPOSE_FILE := infra/docker-compose.yml

.PHONY: fmt check test test-no-run-db dashboard-typecheck dashboard-build verify demo compose-up compose-down

fmt:
	cargo fmt --all

check:
	cargo check

test:
	cargo test

test-no-run-db:
	cargo test -p api --test pipeline_persistence --no-run
	cargo test -p db --test integration_db --no-run

dashboard-typecheck:
	npm --prefix apps/dashboard run typecheck

dashboard-build:
	npm --prefix apps/dashboard run build

verify: fmt check test test-no-run-db dashboard-typecheck dashboard-build

demo:
	./scripts/demo-v0.1.sh

compose-up:
	docker compose -f $(COMPOSE_FILE) up -d postgres api

compose-down:
	docker compose -f $(COMPOSE_FILE) down
