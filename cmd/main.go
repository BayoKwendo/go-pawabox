package main

import (
	"context"
	"fiberapp/config"
	"fiberapp/controllers"
	"fiberapp/database"
	"fiberapp/routes"
	"fiberapp/services"
	"os"
	"os/signal"
	"time"

	"github.com/gofiber/fiber/v2"
	fibercors "github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/sirupsen/logrus"
)

func main() {
	// initialize logger
	logger := config.NewLogger()
	logrus.SetLevel(logrus.InfoLevel)
	_ = logger // logger is used by packages via config package
	// 1. Initialize database connection
	logrus.Info("📦 Initializing database connection...")
	if err := database.ConnectPostgres("config.yml"); err != nil {
		logrus.Fatalf("❌ Failed to connect to database: %v", err)
	}
	defer database.Close()
	logrus.Info("✅ Database connected successfully")

	// 2. Create database instance
	db := database.NewDatabase()

	// 3. Initialize services
	logrus.Info("📦 Initializing services...")
	luckyService := services.NewLuckyNumberService(db)

	controllers.InitLuckyNumberService(db)

	logrus.Info("✅ Services initialized successfully")

	// 4. Start background cleanup for inactive players
	app := fiber.New(fiber.Config{
		IdleTimeout:  60 * time.Second,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		AppName:      "Lucky Number Game API",
	})

	// middlewares
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()

		defer func() {
			duration := time.Since(start)
			logrus.WithFields(logrus.Fields{
				"method":   c.Method(),
				"path":     c.Path(),
				"duration": duration.String(),
				"status":   c.Response().StatusCode,
			}).Info("Request completed")

			// Log slow requests
			if duration > 500*time.Millisecond {
				logrus.WithFields(logrus.Fields{
					"method":   c.Method(),
					"path":     c.Path(),
					"duration": duration.String(),
				}).Warn("Slow request detected")
			}
		}()

		return c.Next()
	})

	app.Use(recover.New())
	app.Use(fibercors.New(fibercors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Content-Type, Authorization",
	}))

	// Inject services into context for route handlers
	app.Use(func(c *fiber.Ctx) error {
		// Store services in context for route handlers to access
		c.Locals("luckyService", luckyService)
		c.Locals("db", db)
		return c.Next()
	})

	// register routes
	routes.RegisterRoutes(app)

	// Test endpoint to verify everything is working
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":    "healthy",
			"service":   "Lucky Number Game API",
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})

	logrus.Info("🚀 Starting server on port 3007...")

	// graceful shutdown
	go func() {
		if err := app.Listen(":3007"); err != nil {
			logrus.Fatalf("❌ Failed to start server: %v", err)
		}
	}()

	// wait for signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	logrus.Info("🛑 Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.Shutdown(); err != nil {
		logrus.Errorf("❌ Error during shutdown: %v", err)
	}

	<-ctx.Done()
	logrus.Info("✅ Server gracefully stopped")
}
