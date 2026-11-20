-- name: CreateUser :one
INSERT into jobs (type , payload , idempotency_key , scheduled_at) values ($1 , $2 , $3 , $4 ) returning *;

