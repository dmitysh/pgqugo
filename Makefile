.PHONY: db-run
db-run:
	docker run --rm --name pgqugo_postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -v pgqugo_postgres_data:/var/lib/postgresql/data -p 5490:5432 -d postgres

.PHONY: db-stop
db-stop:
	docker stop pgqugo_postgres

.PHONY: migrate-up
migrate-up:
	goose -dir=migrations/v1 postgres postgres://postgres:postgres@localhost:5490/postgres up

.PHONY: migrate-down
migrate-down:
	goose -dir=migrations/v1 postgres postgres://postgres:postgres@localhost:5490/postgres down
