-- name: CreateJobs :one
INSERT into jobs (type , payload , idempotency_key , max_retries ,scheduled_at) values ($1 , $2 , $3 , $4 ,$5) returning *;

-- name: GetJobsByID :one
SELECT * from jobs where id = $1 limit 1 ;

-- name: GetJobsByIdempotencyKey :one
SELECT * from jobs where idempotency_key = $1 limit 1 ;

-- name: MarkJobRunning :one
UPDATE jobs set status = 'Running',
    attempts = attempts+1,
    started_at = NOW(),
    updated_at = NOW()
where id = $1 and status in ('Pending' , 'Failed') returning *;

-- name: MarkJobCompleted :exec
UPDATE jobs set status = 'Completed',
    completed_at = NOW(),
    updated_at = NOW()
where id = $1 ;

-- name: MarkJobFailed :exec
UPDATE jobs set status = $2,
    last_error = $3,
    updated_at = NOW()
where id = $1 ;