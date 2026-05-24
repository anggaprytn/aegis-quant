COMPOSE_FILE := infra/docker-compose.yml

.PHONY: fmt fmt-check check test test-no-run-db dashboard-typecheck dashboard-build demo-syntax diff-check verify demo compose-up compose-down

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

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

demo-syntax:
	bash -n scripts/demo-v0.1.sh

diff-check:
	git diff --check

verify: fmt-check check test test-no-run-db dashboard-typecheck dashboard-build demo-syntax diff-check

demo:
	./scripts/demo-v0.1.sh

compose-up:
	docker compose -f $(COMPOSE_FILE) up -d postgres
	docker compose -f $(COMPOSE_FILE) --profile migrate run --rm migrate
	docker compose -f $(COMPOSE_FILE) up -d api

compose-down:
	docker compose -f $(COMPOSE_FILE) down
