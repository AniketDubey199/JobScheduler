package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/AniketDubey199/internals/db"
	"github.com/AniketDubey199/internals/queue"
	worker "github.com/AniketDubey199/internals/workers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	// loading .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	constring := fmt.Sprintf("postgres://%s:%s@localhost:5432/%s?sslmode=disable",
		os.Getenv("POSTGRE_USER"),
		os.Getenv("POSTGRE_PASS"),
		os.Getenv("POSTGRE_DB"),
	)
	//pgxpool connection
	psqlcon, err := pgxpool.New(ctx, constring)
	if err != nil {
		log.Println("Cannot Connect to PostgreSql")
	}
	if err := psqlcon.Ping(ctx); err != nil {
		log.Fatalf("Unable to ping Postgres: %v", err)
	}
	defer psqlcon.Close()
	queries := db.New(psqlcon)

	//redis connection using wrapper
	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisClient, err := queue.NewClient(context.Background(), redisAddr)
	if err != nil {
		log.Println("Cannot connect to redis")
	}
	defer redisClient.Close()

	pool := worker.Newpool(3, queries, redisClient)

	pool.Start(ctx)
}
