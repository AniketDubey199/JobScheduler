-- name: CreateJobs :one
INSERT into jobs (type , payload , idempotency_key , scheduled_at) values ($1 , $2 , $3 , $4 ) returning *;

-- name: GetJobsByID :one
SELECT * from jobs where id = $1 limit 1 ;

-- name: GetJobsByIdempotencyKey :one
SELECT * from jobs where idempotency_key = $1 limit 1 ;

-- name: MarkJobRunning :one
UPDATE jobs set status = 'Running',
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
    updated_at = NOW()
where id = $1 ;