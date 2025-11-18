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
	"runtime"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"
	fibercors "github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/sirupsen/logrus"
)

var (
	requestCount uint64
)

func main() {
	// Set GOMAXPROCS to utilize all CPU cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Initialize logger with performance optimizations
	logger := config.NewLogger()
	logrus.SetLevel(logrus.InfoLevel)
	logrus.SetReportCaller(false) // Disable caller info for performance
	_ = logger

	// 1. Initialize database connection with connection pool optimizations
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

	// 4. Create Fiber app with optimized configuration
	app := fiber.New(fiber.Config{
		IdleTimeout:           60 * time.Second,
		ReadTimeout:           10 * time.Second, // Reduced from 15s
		WriteTimeout:          10 * time.Second, // Reduced from 15s
		ReadBufferSize:        8192,             // Increased buffer size
		WriteBufferSize:       8192,             // Increased buffer size
		Concurrency:           256 * 1024,       // Increased concurrency limit
		ServerHeader:          "Fiber",          // Simpler header
		AppName:               "Lucky Number Game API",
		EnablePrintRoutes:     false, // Disable in production
		DisableStartupMessage: true,  // We'll log manually
	})

	// Optimized middleware chain
	app.Use(recover.New(recover.Config{
		EnableStackTrace: false, // Disable stack trace in production
	}))

	// Optimized CORS configuration
	app.Use(fibercors.New(fibercors.Config{
		AllowOrigins:     "*",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Content-Type, Authorization",
		AllowCredentials: false,
		MaxAge:           300, // 5 minutes cache for preflight
	}))

	// Optimized logging middleware with sampling for high traffic
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		atomic.AddUint64(&requestCount, 1)

		err := c.Next()

		duration := time.Since(start)

		// Sample logging: log only 1% of requests or slow requests
		currentCount := atomic.LoadUint64(&requestCount)
		if duration > 500*time.Millisecond || currentCount%100 == 0 {
			logrus.WithFields(logrus.Fields{
				"method":   c.Method(),
				"path":     c.Path(),
				"duration": duration.Milliseconds(), // Use milliseconds for smaller logs
				"status":   c.Response().StatusCode,
				"ip":       c.IP(),
			}).Info("Slow request detected")
		}

		return err
	})

	// app.Use(func(c *fiber.Ctx) error {
	// 	start := time.Now()

	// 	defer func() {
	// 		duration := time.Since(start)
	// 		logrus.WithFields(logrus.Fields{
	// 			"method":   c.Method(),
	// 			"path":     c.Path(),
	// 			"duration": duration.String(),
	// 			"status":   c.Response().StatusCode,
	// 		}).Info("Request completed")

	// 		// Log slow requests
	// 		if duration > 500*time.Millisecond {
	// 			logrus.WithFields(logrus.Fields{
	// 				"method":   c.Method(),
	// 				"path":     c.Path(),
	// 				"duration": duration.String(),
	// 			}).Warn("Slow request detected")
	// 		}
	// 	}()

	// 	return c.Next()
	// })
	// Efficient context injection using Fiber's Locals (already optimized)
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("luckyService", luckyService)
		c.Locals("db", db)
		return c.Next()
	})

	// Register routes
	routes.RegisterRoutes(app)

	// Optimized health endpoint
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":    "healthy",
			"service":   "Lucky Number Game API",
			"timestamp": time.Now().Unix(), // Use Unix timestamp for faster serialization
		})
	})

	// Prefork for multi-core utilization (enable in production)
	// Note: Uncomment if you have multiple CPU cores and want maximum performance
	// app.Settings().Prefork = true

	logrus.Info("🚀 Starting server on port 3007...")

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		if err := app.Listen(":3007"); err != nil {
			serverErr <- err
		}
	}()

	// Wait for interrupt signal or server error
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	select {
	case <-quit:
		logrus.Info("🛑 Shutting down server...")
	case err := <-serverErr:
		logrus.Errorf("❌ Server error: %v", err)
	}

	// Graceful shutdown with timeout
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Reduced from 10s
	defer cancel()

	if err := app.Shutdown(); err != nil {
		logrus.Errorf("❌ Error during shutdown: %v", err)
	}

	logrus.Info("✅ Server gracefully stopped")
}
