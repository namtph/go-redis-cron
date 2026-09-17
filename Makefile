.PHONY: test vet demo redis

test:
	go test ./...

vet:
	go vet ./...

redis:
	docker compose -f demo/docker-compose.yml up -d

demo:
	go run ./demo/cmd/server -redis localhost:6379 -framework gin -addr :8080
