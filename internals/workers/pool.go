package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
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
	p.wg.Add(1)
	go p.retrySchedulerJob(ctx)
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
func (p *Pool) retrySchedulerJob(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	log.Println("[Scheduler] Retry scheduler started")

	for {
		select {
		case <-ctx.Done():
			log.Println("[Scheduler] Shutting down...")
			return
		case <-ticker.C:
			duejobs, err := p.clients.FetchRetryEntries(ctx, 100)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Printf("[Scheduler] Error fetching due retries: %v", err)
				}
				continue
			}
			for _, jobID := range duejobs {
				err := p.clients.Enqueue(ctx, jobID)
				if err != nil {
					log.Printf("[Scheduler] Failed to re-enqueue job %s: %v", jobID, err)
					continue
				}
				log.Printf("[Scheduler] Re-enqueued due job %s", jobID)
			}
		}
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
		p.handlejobFailure(workerID, job, err)
		return
	}
	if err = p.queries.MarkJobCompleted(ctx, jobID); err != nil {
		log.Printf("[Worker %d] Failed to mark job %s completed: %v", workerID, jobIDStr, err)
		return
	}
	log.Printf("[Worker %d] Job %s COMPLETED", workerID, jobIDStr)
}

func (p *Pool) handlejobFailure(workerID int, job db.Job, execErr error) {
	jobIDStr := job.ID.String()

	if job.Attempts >= job.MaxRetries {
		log.Printf("[Worker %d] Job %s exhausted retries (%d/%d). Moving to DLQ. Error: %v",
			workerID, jobIDStr, job.Attempts, job.MaxRetries, execErr)
		_ = p.queries.MarkJobFailed(context.Background(), db.MarkJobFailedParams{
			ID:        job.ID,
			Status:    "Dead",
			LastError: pgtype.Text{String: execErr.Error(), Valid: true},
		})
		if err := p.clients.MovetoDQL(context.Background(), jobIDStr); err != nil {
			log.Printf("[Worker %d] Failed to push job %s to DLQ: %v", workerID, jobIDStr, err)
		}
		return
	}
	delay := calculateBackoff(job.Attempts)
	log.Printf("[Worker %d] Job %s FAILED (attempt %d/%d). Retrying in %v. Error: %v",
		workerID, jobIDStr, job.Attempts, job.MaxRetries, delay.Round(time.Millisecond), execErr)

	_ = p.queries.MarkJobFailed(context.Background(), db.MarkJobFailedParams{
		ID:        job.ID,
		Status:    "Failed",
		LastError: pgtype.Text{String: execErr.Error(), Valid: true},
	})

	if err := p.clients.ScheduledRetry(context.Background(), jobIDStr, delay); err != nil {
		log.Printf("[Worker %d] Failed to schedule retry for job %s: %v", workerID, jobIDStr, err)
	}
}

func calculateBackoff(attempts int32) time.Duration { // calclulating the jitter in every retry
	if attempts < 1 {
		attempts = 1
	}
	base := 2 * time.Second
	delay := base * time.Duration(1<<(attempts-1))
	maxDelay := 5 * time.Second
	if delay > maxDelay {
		delay = maxDelay
	}
	jitter := time.Duration(rand.Intn(500)) * time.Millisecond
	return delay + jitter
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
	case "fail_test":
		return errors.New("simulated downstream service failure")
	default:
		return fmt.Errorf("unknown job type: %s", job.Type)
	}
}
