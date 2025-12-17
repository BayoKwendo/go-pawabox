package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"fiberapp/config"
	"fiberapp/controllers"
	"fiberapp/database"
	"fiberapp/routes"
	"fiberapp/services"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/sirupsen/logrus"
)

var (
	requestCount uint64
)

func main() {
	// Use all available CPUs
	runtime.GOMAXPROCS(runtime.NumCPU())

	// ---------- Logger (JSON, no caller) ----------
	logger := config.NewLogger()
	logrus.SetLevel(logrus.InfoLevel)
	logrus.SetReportCaller(false)
	logrus.SetFormatter(&logrus.JSONFormatter{
		// smaller timestamp format; keep messages compact
		TimestampFormat: time.RFC3339,
	})
	_ = logger // keep existing usage if needed elsewhere

	// ---------- Database ----------
	logrus.Info("📦 Initializing database connection...")
	// Let database.ConnectPostgres manage pooling config using config.yml.
	if err := database.ConnectPostgres("config.yml"); err != nil {
		logrus.Fatalf("❌ Failed to connect to database: %v", err)
	}
	defer database.Close()
	logrus.Info("✅ Database connected successfully")

	db := database.NewDatabase()

	// ---------- Services ----------
	logrus.Info("📦 Initializing services...")
	luckyService := services.NewLuckyNumberService(db)
	controllers.InitLuckyNumberService(db)
	logrus.Info("✅ Services initialized successfully")

	// ---------- Fiber config (tunable via env) ----------
	// PREFORK env var enables preforking (good for CPU-bound loads / multiple forks).
	prefork := os.Getenv("PREFORK") == "true"

	// Concurrency: keep it reasonable so the runtime and kernel don't get overwhelmed.
	// Tune via env var if needed.
	defaultConcurrency := runtime.NumCPU() * 1024
	if v := os.Getenv("FIBER_CONC"); v != "" {
		// ignore parse error, keep default if invalid
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			defaultConcurrency = parsed
		}
	}

	app := fiber.New(fiber.Config{
		IdleTimeout:           60 * time.Second,
		ReadTimeout:           8 * time.Second, // reduced for faster resource release
		WriteTimeout:          8 * time.Second, // reduced for faster resource release
		ReadBufferSize:        4096,            // reasonable buffer size
		WriteBufferSize:       4096,
		Concurrency:           defaultConcurrency,
		ServerHeader:          "Fiber",
		AppName:               "Lucky Number Game API",
		EnablePrintRoutes:     false,
		DisableStartupMessage: true,
		Prefork:               prefork,
	})

	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Origin, Content-Type, Accept, Authorization, X-Access-Token",
		MaxAge:       600,
	}))
	app.Options("/*", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusNoContent)
	})

	// ---------- Middlewares ----------
	// Recover without stack trace in production
	app.Use(recover.New(recover.Config{
		EnableStackTrace: false,
	}))

	// Compression to reduce bandwidth and latency (CPU < network cost typically)
	app.Use(compress.New(compress.Config{
		Level: compress.LevelDefault,
	}))

	// Light-weight request sampling logger - sampleRateEnv or default 100 (1%)
	sampleRate := 100 // sample 1 in 100
	if s := os.Getenv("LOG_SAMPLE_RATE"); s != "" {
		if parsed, err := strconv.Atoi(s); err == nil && parsed > 0 {
			sampleRate = parsed
		}
	}
	// Seeded rand for sampling; atomic ensures safe concurrent access on counter
	rand.Seed(time.Now().UnixNano())
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		atomic.AddUint64(&requestCount, 1)

		err := c.Next()

		duration := time.Since(start)
		current := atomic.LoadUint64(&requestCount)
		// log either slow requests or probabilistically sample
		if duration > 500*time.Millisecond || (int(current)%sampleRate == 0) {
			// keep log fields minimal to reduce allocation
			logrus.WithFields(logrus.Fields{
				"m":  c.Method(),
				"p":  c.Path(),
				"d":  duration.Milliseconds(),
				"s":  c.Response().StatusCode(),
				"ip": c.IP(),
			}).Info("request")
		}
		return err
	})

	// inject shared services into context
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("luckyService", luckyService)
		c.Locals("db", db)
		return c.Next()
	})

	// ---------- Routes ----------
	routes.RegisterRoutes(app)

	// simple health check
	app.Get("/health", func(c *fiber.Ctx) error {
		// small, fast payload
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status":    "healthy",
			"service":   "Lucky Number Game API",
			"timestamp": time.Now().Unix(),
		})
	})
	// ---------- Start server ----------
	port := os.Getenv("PORT")
	if port == "" {
		port = "3007"
	}

	logrus.Infof("🚀 Starting server on port %s (prefork=%v, concurrency=%d)...", port, prefork, defaultConcurrency)

	// Use signal.NotifyContext to handle shutdowns with a cancellable Context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	// Run Listen in goroutine so we can respond to shutdown signals
	listenErr := make(chan error, 1)
	go func() {
		if err := app.Listen(":" + port); err != nil {
			listenErr <- err
		}
		close(listenErr)
	}()

	// Wait for signal or server error
	select {
	case <-ctx.Done():
		// Received termination signal
		logrus.Info("🛑 Shutdown signal received")
	case err := <-listenErr:
		if err != nil {
			logrus.Errorf("❌ Server listen error: %v", err)
		}
	}

	// Graceful shutdown with timeout (tunable via env)
	shutdownTimeout := 5 * time.Second
	if st := os.Getenv("SHUTDOWN_TIMEOUT"); st != "" {
		if parsed, err := strconv.Atoi(st); err == nil && parsed > 0 {
			shutdownTimeout = time.Duration(parsed) * time.Second
		}
	}
	_, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := app.Shutdown(); err != nil {
		logrus.Errorf("❌ Error during shutdown: %v", err)
	} else {
		// Wait for any remaining things for a short interval before exiting
		<-time.After(100 * time.Millisecond)
	}

	logrus.Info("✅ Server gracefully stopped")
}
