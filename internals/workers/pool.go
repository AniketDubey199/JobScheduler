package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AniketDubey199/internals/db"
	"github.com/AniketDubey199/internals/queue"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Pool struct {
	concurrency int
	queries     *db.Queries
	clients     *queue.Client
	wg          sync.WaitGroup
}

func Newpool(concurrency int, queries *db.Queries, client *queue.Client) *Pool {
	return &Pool{
		concurrency: concurrency,
		queries:     queries,
		clients:     client,
	}
}

func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.concurrency; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}
	p.wg.Wait()
	log.Println("All workers stopped cleanly.")
}

func (p *Pool) runWorker(ctx context.Context, workerID int) {
	defer p.wg.Done()
	log.Printf("[Worker %d] Ready", workerID)
	for {
		select {
		case <-ctx.Done(): // for ctrl+c
			log.Printf("[Worker %d] Shutting down...", workerID)
			return
		default:
		}
		jobID, err := p.clients.Dequeue(ctx, 5*time.Second)
		if err != nil {
			if errors.Is(err, queue.ErrQueueEmpty) || errors.Is(err, context.Canceled) {
				continue
			}
		}
		p.processJob(ctx, workerID, jobID)
	}
}

func (p *Pool) processJob(ctx context.Context, workerID int, jobIDStr string) {
	var jobID pgtype.UUID
	if err := jobID.Scan(jobIDStr); err != nil {
		log.Printf("[Worker %d] Invalid UUID from Redis (%s): %v", workerID, jobIDStr, err)
		return
	}
	job, err := p.queries.MarkJobRunning(ctx, jobID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Printf("[Worker %d] Job %s skipped (already claimed or finished)", workerID, jobIDStr)
			return
		}
		log.Printf("[Worker %d] DB error claiming job %s: %v", workerID, jobIDStr, err)
		return
	}
	log.Printf("[Worker %d] Processing job %s (type=%s, attempt=%d)", workerID, jobIDStr, job.Type, job.Attempts)
	jobctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = executeJob(jobctx, job)
	if err != nil {
		log.Printf("[Worker %d] Job %s FAILED: %v", workerID, jobIDStr, err)
		_ = p.queries.MarkJobFailed(ctx, db.MarkJobFailedParams{
			ID:        jobID,
			Status:    "Failed",
			LastError: pgtype.Text{String: err.Error(), Valid: true},
		})
		return
	}
	if err = p.queries.MarkJobCompleted(ctx, jobID); err != nil {
		log.Printf("[Worker %d] Failed to mark job %s completed: %v", workerID, jobIDStr, err)
		return
	}
	log.Printf("[Worker %d] Job %s COMPLETED", workerID, jobIDStr)
}

func executeJob(ctx context.Context, job db.Job) error {
	switch job.Type {
	case "send_email":
		select {
		case <-time.After(2 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
		return fmt.Errorf("unknown job type: %s", job.Type)
	}
}
