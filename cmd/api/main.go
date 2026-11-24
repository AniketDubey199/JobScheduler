package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/AniketDubey199/internals/db"
	"github.com/AniketDubey199/internals/handlers"
	"github.com/AniketDubey199/internals/queue"
	"github.com/AniketDubey199/internals/routes"
	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	// loading .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}
	constring := fmt.Sprintf("postgres://%s:%s@localhost:5432/%s?sslmode=disable",
		os.Getenv("POSTGRE_USER"),
		os.Getenv("POSTGRE_PASS"),
		os.Getenv("POSTGRE_DB"),
	)
	//pgxpool connection
	psqlcon, err := pgxpool.New(context.Background(), constring)
	if err != nil {
		log.Println("Cannot Connect to PostgreSql")
	}
	if err := psqlcon.Ping(context.Background()); err != nil {
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

	// now setup the fiber
	app := fiber.New()
	jobhandler := handlers.NewJobHandlers(queries, redisClient)
	routes.RegisteredRoutes(app, jobhandler)

	log.Println("Producer API listening on :8080")
	log.Fatal(app.Listen(":8080"))
}
