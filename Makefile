.RECIPEPREFIX = >
.PHONY: build up down ps logs dbt-debug smoke sync-ref
build:
> docker compose build
up:
> docker compose up -d
down:
> docker compose down
ps:
> docker compose ps
logs:
> docker compose logs -f $(s)
dbt-debug:
> docker compose exec dbt dbt debug
smoke:
> ./scripts/smoke_test.sh
sync-ref:
> docker compose exec ingestion python -m ingestion.reference_sync
