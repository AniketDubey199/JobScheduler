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

-- name: ReplayDeadJobs :one
UPDATE jobs
set status = 'Pending',
    attempts = 1,
    last_error = NULL,
    updated_at = NOW()
where id = $1 and status = 'Dead' returning *;

-- name: ListJobs :many
SELECT * FROM jobs
WHERE ($1::text = '' OR status = $1)
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: GetJobCountByStatus :many
SELECT status , count(*)::BIGINT as count
from jobs group by status;

-- name: RecoverStuckJob :many
UPDATE jobs 
set 
    status = case when attempts >= max_retries then 'Dead'
    else 'Failed'
    end ,
    last_error = 'worker crashed or timed out (recovered by sweeper)',
    updated_at = NOW()
where status = 'Running'
and updated_at < $1
returning *;

-- name: MarkJobFailed :exec
UPDATE jobs set status = $2,
    last_error = $3,
    updated_at = NOW()
where id = $1 ;