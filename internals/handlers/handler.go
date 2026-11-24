package handlers

import (
	"encoding/json"
	"errors"
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
			"error": "cannot get the id",
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
