package handlers

import (
	"github.com/AniketDubey199/internals/db"
	"github.com/AniketDubey199/internals/queue"
)

type JobHandler struct {
	queries *db.Queries
	clients *queue.Client
}

func NewJobHandlers(queries *db.Queries, client *queue.Client) *JobHandler {
	return &JobHandler{
		queries: queries,
		clients: client,
	}
}
