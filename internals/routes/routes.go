package routes

import (
	"github.com/AniketDubey199/internals/handlers"
	"github.com/gofiber/fiber/v3"
)

func RegisteredRoutes(app *fiber.App, jobhandler *handlers.JobHandler) {
	jobs := app.Group("/jobs")
	jobs.Post("/", jobhandler.CreateJob)
	jobs.Get("/:id", jobhandler.GetJobByID)
}
