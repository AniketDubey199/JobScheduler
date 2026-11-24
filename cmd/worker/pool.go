package worker

import (
	"context"
	"log"
	"sync"

	"github.com/AniketDubey199/internals/db"
	"github.com/AniketDubey199/internals/queue"
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
	for i := 1; i < p.concurrency; i++ {
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
		case <-ctx.Done():
			log.Printf("[Worker %d] Shutting down...", workerID)
			return
		default:
		}

	}
}
