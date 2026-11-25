include .env

migrate-up : 
	migrate -path migrations -database "postgres://${POSTGRE_USER}:${POSTGRE_PASS}@localhost:5432/${POSTGRE_DB}?sslmode=disable" up
migrate-down :
	migrate -path migrations -database "postgres://${POSTGRE_USER}:${POSTGRE_PASS}@localhost:5432/${POSTGRE_DB}?sslmode=disable" down
migrate-force1:
	migrate -path migrations -database "postgres://${POSTGRE_USER}:${POSTGRE_PASS}@localhost:5432/${POSTGRE_DB}?sslmode=disable" force 1
migrate-down1:
	migrate -path migrations -database "postgres://${POSTGRE_USER}:${POSTGRE_PASS}@localhost:5432/${POSTGRE_DB}?sslmode=disable" down 1
runapi:
	go run cmd/api/main.go
runworker:
	go build -o bin/worker cmd/worker/main.go
	./bin/worker