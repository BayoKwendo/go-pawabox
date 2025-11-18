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
	// Initialize logger
	logger := config.NewLogger()
	logrus.SetLevel(logrus.WarnLevel) // Lower verbosity for high-load

	_ = logger

	// Connect to database
	logrus.Info("📦 Connecting to database...")
	if err := database.ConnectPostgres("config.yml"); err != nil {
		logrus.Fatalf("❌ Database connection failed: %v", err)
	}
	defer database.Close()
	db := database.NewDatabase()
	logrus.Info("✅ Database connected")

	// Initialize services
	luckyService := services.NewLuckyNumberService(db)
	controllers.InitLuckyNumberService(db)
	logrus.Info("✅ Services initialized")

	// Create Fiber app with Prefork enabled (uses all CPU cores)
	app := fiber.New(fiber.Config{
		Prefork:           true,
		IdleTimeout:       60 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		AppName:           "Lucky Number Game API",
		ReduceMemoryUsage: true,
	})

	// Recover from panics
	app.Use(recover.New())

	// CORS
	app.Use(fibercors.New(fibercors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Content-Type, Authorization",
	}))

	// Minimal request logger to reduce overhead
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		duration := time.Since(start)

		if duration > 500*time.Millisecond {
			logrus.WithFields(logrus.Fields{
				"method":   c.Method(),
				"path":     c.Path(),
				"duration": duration,
			}).Warn("Slow request detected")
		}
		return err
	})

	// Inject services into context
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("luckyService", luckyService)
		c.Locals("db", db)
		return c.Next()
	})

	// Register routes
	routes.RegisterRoutes(app)

	// Health check endpoint
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":    "healthy",
			"service":   "Lucky Number Game API",
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})

	// Start server in a goroutine
	go func() {
		logrus.Info("🚀 Server running on port 3007...")
		if err := app.Listen(":3007"); err != nil {
			logrus.Fatalf("❌ Server failed: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	logrus.Info("🛑 Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.Shutdown(); err != nil {
		logrus.Errorf("❌ Shutdown error: %v", err)
	}

	<-ctx.Done()
	logrus.Info("✅ Server stopped gracefully")
}
