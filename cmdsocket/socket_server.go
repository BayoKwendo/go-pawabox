package main

import (
	"encoding/json"
	"fiberapp/database"
	"fiberapp/services"
	"fiberapp/utils"
	"log"
	"net/http"
	"os"

	"os/signal"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/sirupsen/logrus"
	socketio "github.com/zishang520/socket.io/socket"
)

func main() {
	// --- 1. Fiber Setup ---
	app := fiber.New()
	app.Use(cors.New(cors.Config{
		AllowOrigins:     "*",
		AllowCredentials: false,
	}))

	logrus.Info("📦 Initializing database connection...")
	// Let database.ConnectPostgres manage pooling config using config.yml.
	if err := database.ConnectPostgres("config.yml"); err != nil {
		logrus.Fatalf("❌ Failed to connect to database: %v", err)
	}
	defer database.Close()
	logrus.Info("✅ Database connected successfully")

	db := database.NewDatabase()

	lucky := services.NewLuckyNumberService(db)

	// --- 2. Socket.IO Server Setup ---
	// Create Socket.IO server instance
	io := socketio.NewServer(nil, nil)

	// Health check endpoint
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":  "healthy",
			"service": "Lucky Number Game API",
		})
	})

	clients := make(map[socketio.SocketId]bool)

	// --- 3. Socket.IO Events ---
	io.On("connection", func(conn ...any) {
		if len(conn) == 0 {
			return
		}

		socket := conn[0].(*socketio.Socket)
		clientId := socket.Id()

		clients[clientId] = true
		log.Printf("✅ Connected: %s | Total: %d", clientId, len(clients))

		socket.Emit("connected", map[string]interface{}{
			"id":        clientId,
			"message":   "Welcome to Lucky Number Game Socket Server",
			"timestamp": time.Now().Unix(),
		})

		// Handle ping event
		socket.On("ping", func(data ...any) {
			socket.Emit("pong", map[string]interface{}{
				"message":   "pong",
				"timestamp": time.Now().Unix(),
				"id":        clientId,
			})
		})

		// Handle message event
		socket.On("winners", func(data ...any) {

			winner, err := lucky.GetWinners()
			if err != nil {
				socket.Emit("error", map[string]interface{}{
					"Status":        false,
					"StatusCode":    1,
					"StatusMessage": err.Error(),
				})
				return
			}

			if winner == nil {
				winner = []map[string]interface{}{}
			}

			socket.Emit("winners_list", map[string]interface{}{
				"Status":        200,
				"StatusCode":    0,
				"Data":          winner,
				"StatusMessage": "Success",
			})
		})

		// Handle message event
		socket.On("online_users", func(data ...any) {

			online_users, err := lucky.GetOnlineUsers()
			if err != nil {
				socket.Emit("error", map[string]interface{}{
					"Status":        false,
					"StatusCode":    1,
					"StatusMessage": err.Error(),
				})
				return
			}

			if online_users == nil {
				online_users = []map[string]interface{}{}
			}
			var online int

			if len(online_users) == 0 {
				online = 0
			} else {
				online = int(online_users[0]["online_users"].(int64)) // or float64 depending on DB
			}

			socket.Emit("online_list", map[string]interface{}{
				"Status":        200,
				"StatusCode":    0,
				"UsersOnline":   online,
				"StatusMessage": "Success",
			})

		})

		// Handle message event
		socket.On("user", func(data ...any) {
			if len(data) == 0 {
				return
			}

			var tokenString string

			// Extract token from data - data[0] contains the actual message data
			switch v := data[0].(type) {
			case map[string]interface{}:
				// If data is a map
				if token, ok := v["token"].(string); ok {
					tokenString = token
				}
			case string:
				// If data is just a string (the token itself)
				tokenString = v
			default:
				socket.Emit("error", map[string]interface{}{
					"Status":        false,
					"StatusCode":    1,
					"StatusMessage": "invalid data format",
				})
				return
			}

			log.Printf("🔌 Disconnected: %s", tokenString)

			if tokenString == "" {
				socket.Emit("error", map[string]interface{}{
					"Status":        false,
					"StatusCode":    1,
					"StatusMessage": "missing token",
				})
				return
			}

			// Verify token using your existing logic
			claims, err := utils.VerifyJWTToken(tokenString)
			if err != nil {
				socket.Emit("error", map[string]interface{}{
					"Status":        false,
					"StatusCode":    1,
					"StatusMessage": err.Error(),
				})
				return
			}

			msisdn := claims["sub"].(string)
			log.Printf("🔌 Disconnected: %s", msisdn)

			user, err := lucky.CheckUser(msisdn, "")
			if err != nil {
				socket.Emit("error", map[string]interface{}{
					"Status":        false,
					"StatusCode":    1,
					"StatusMessage": err.Error(),
				})
				return
			}

			socket.Emit("user_info", map[string]interface{}{
				"Status":        200,
				"StatusCode":    0,
				"Data":          user,
				"StatusMessage": "Success",
			})

		})

		// Handle disconnect
		socket.On("disconnect", func(reason ...any) {
			delete(clients, clientId)
			disconnectReason := "client disconnect"
			if len(reason) > 0 {
				if r, ok := reason[0].(string); ok {
					disconnectReason = r
				}
			}
			log.Printf("🔌 Disconnected: %s | Reason: %s | Remaining: %d", clientId, disconnectReason, len(clients))
		})
	})

	// --- 4. HTTP Server Setup ---
	// Create HTTP server with Socket.IO as the main handler
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		response := map[string]interface{}{
			"status":            "healthy",
			"service":           "Lucky Number Game API",
			"port":              3006,
			"connected_clients": len(clients),
			"timestamp":         time.Now().Unix(),
		}

		jsonData, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(jsonData)
	})

	// Socket.IO handler
	mux.Handle("/socket.io/", io.ServeHandler(nil))

	// Other routes can be handled by Fiber if needed, but for simplicity using net/http
	server := &http.Server{
		Addr:    ":3006",
		Handler: mux,
	}

	// --- 5. Start & Shutdown ---
	go func() {
		log.Println("🚀 Server starting on :3006")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Server error: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("🛑 Shutting down server...")

	// Close Socket.IO server
	io.Close(nil)

	// Shut down HTTP server
	if err := server.Shutdown(nil); err != nil {
		log.Printf("❌ Server shutdown error: %v", err)
	}

	log.Println("✅ Server stopped gracefully")
}
