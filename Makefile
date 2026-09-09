include .envrc
DSN ?= $(IMAGE_DB_DSN)
.PHONY: run migrate-up migrate-down migrate-force migrate-version db/psql test build docker-up docker-down

run:
	@echo 'Running image API...'
	@go run ./cmd/api

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

migrate-up:
	migrate -path=./migrations -database=$(DSN) up

migrate-down:
	migrate -path=./migrations -database=$(DSN) down

migrate-force:
	migrate -path=./migrations -database=$(DSN) force $(version)

migrate-version:
	migrate -path=./migrations -database=$(DSN) version

db/psql:
	docker exec -it image-api-postgres psql -U postgres -d image_db

test:
	go test -v ./...

build:
	go build -ldflags='-s -w' -o=./bin/api ./cmd/api
