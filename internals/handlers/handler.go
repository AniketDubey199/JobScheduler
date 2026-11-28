package handlers

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/AniketDubey199/internals/db"
	"github.com/AniketDubey199/internals/queue"
	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type CreateJobRequest struct {
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	MaxRetries int32           `json:"max_retries"`
}

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

func (jb *JobHandler) CreateJob(c fiber.Ctx) error {
	var reqbdy CreateJobRequest
	if err := c.Bind().JSON(&reqbdy); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid body",
		})
	}
	if reqbdy.Type == "" || len(reqbdy.Payload) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "type and payload are required",
		})
	}
	idemkeyheader := c.Get("Idempotency-Key")
	var idemkey pgtype.Text
	if idemkeyheader != "" {
		idemkey = pgtype.Text{String: idemkeyheader, Valid: true}

		existingJob, err := jb.queries.GetJobsByIdempotencyKey(c, idemkey) // this is to find if that id is database or not
		if err == nil {
			return c.Status(fiber.StatusOK).JSON(existingJob)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "database lookup failed",
			})
		}
		if reqbdy.MaxRetries <= 0 {
			reqbdy.MaxRetries = 3
		}
	}

	jobs, err := jb.queries.CreateJobs(c, db.CreateJobsParams{
		Type:           reqbdy.Type,
		Payload:        reqbdy.Payload,
		IdempotencyKey: idemkey,
		MaxRetries:     reqbdy.MaxRetries,
		ScheduledAt:    pgtype.Timestamptz{Time: time.Now(), Valid: true},
	})

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to create job",
		})
	}
	if err := jb.clients.Enqueue(c, jobs.ID.String()); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "job saved, but failed to enqueue",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(jobs)
}

func (jb *JobHandler) GetJobByID(c fiber.Ctx) error {
	var id pgtype.UUID
	if err := id.Scan(c.Params("id")); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "cannot get the id (get jobs by ID)",
		})
	}
	jobs, err := jb.queries.GetJobsByID(c, id)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(jobs)
}

func (jb *JobHandler) RetryJob(c fiber.Ctx) error {
	idparams := c.Params("id")

	var id pgtype.UUID
	if err := id.Scan(idparams); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid job UUID",
		})
	}
	job, err := jb.queries.ReplayDeadJobs(c, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": "job not found or not in DEAD status",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to reset job for retry",
		})
	}
	jobIDString := job.ID.String()
	_ = jb.clients.RemovefromDQL(c, jobIDString)

	if err := jb.clients.Enqueue(c, jobIDString); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "job reset in DB, but failed to enqueue in Redis",
		})
	}
	return c.Status(fiber.StatusOK).JSON(job)

}

func (jb *JobHandler) ListAllJobs(c fiber.Ctx) error {
	status := c.Query("status", "")

	limit, err := strconv.Atoi(c.Query("limit", "20"))
	if err != nil || limit > 100 || limit <= 0 {
		limit = 20
	}

	offset, err := strconv.Atoi(c.Query("offset", "0"))
	if err != nil || offset < 0 {
		offset = 0
	}

	jobs, err := jb.queries.ListJobs(c.Context(), db.ListJobsParams{
		Column1: status,
		Limit:   int32(limit),
		Offset:  int32(offset),
	})

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to list jobs",
		})
	}
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"jobs":   jobs,
		"limit":  limit,
		"offset": offset,
	})
}

func (jb *JobHandler) GetStats(c fiber.Ctx) error {
	redisStats, err := jb.clients.GetQueueStats(c.Context())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to fetch Redis queue stats",
		})
	}
	rows, err := jb.queries.GetJobCountByStatus(c.Context())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to fetch DB job counts",
		})
	}
	dbCounts := make(map[string]int64, len(rows))
	for _, r := range rows {
		dbCounts[r.Status] = r.Count
	}
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"queues": redisStats,
		"jobs":   dbCounts,
	})
}
