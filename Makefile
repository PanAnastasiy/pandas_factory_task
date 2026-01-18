
ifneq (,$(wildcard ./.env))
  include .env
  export
endif

DC = docker compose

COMPOSE_FILE = docker_compose/docker-compose.yml
EXEC = docker exec -it

DB_CONTAINER = ${POSTGRES_CONTAINER_NAME}
LOGS = docker logs

ENV = --env-file .env


.PHONY: up
up:
	${DC} -f ${COMPOSE_FILE} ${ENV} up -d

.PHONY: down
down:
	${DC} -f ${COMPOSE_FILE} ${ENV} down

.PHONY: clean
clean:
	${DC} -f ${COMPOSE_FILE} ${ENV} down -v

.PHONY: restart
restart: down up

.PHONY: postgres
postgres:
	${EXEC} ${DB_CONTAINER} psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}

.PHONY: logs
logs:
	${LOGS} ${DB_CONTAINER} -f

.PHONY: shell
shell:
	${EXEC} ${DB_CONTAINER} sh

.PHONY: format
format:
	isort .
	black .

.PHONY: lint
lint:
	flake8 .
	black . --check
	isort . --check-only

.PHONY: test
test:
	pytest -v

.PHONY: pre-commit-install
pre-commit-install:
	pre-commit install

.PHONY: pre-commit-run
pre-commit-run:
	pre-commit run --all-files

.PHONY: check
check: format lint test