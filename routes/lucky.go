package routes

import (
	"fiberapp/controllers"
	"fiberapp/utils"

	"github.com/gofiber/fiber/v2"
)

func RegisterRoutes(app *fiber.App) {
	api := app.Group("/api/v1")

	api.Get("/", controllers.Hello)
	api.Get("/test", controllers.Test)
	api.Post("/place_bet_luckynumber", utils.JWTMiddleware(), controllers.PlaceBetLuckyNumber)
	api.Post("/settle_bt_luckynumber", controllers.SettleBTLuckyNumber)
	api.Post("/settle_bet_luckynumber", controllers.SettleBetLuckyNumber)

	api.Post("/initiate_deposit", utils.JWTMiddleware(), controllers.IniatateDepositLuckyNumber)

	api.Post("/settle_withdrawal", controllers.SettleWithdrawalLuckyNumber)
	api.Post("/settle_withdrawal_b2b", controllers.SettleWithdrawalB2BLuckyNumber)
	api.Get("/lucky_games", utils.JWTMiddleware(), controllers.GetGames)
	api.Post("/login", controllers.Login)

	api.Get("/user", utils.JWTMiddleware(), controllers.GetUser)

	api.Put("/user", utils.JWTMiddleware(), controllers.UpdateUser)

	api.Post("/bet_history", utils.JWTMiddleware(), controllers.GetHistoryHandler)

	api.Post("/game_history", utils.JWTMiddleware(), controllers.GetGameHistoryHandler)

	api.Post("/list_withdrawal", utils.JWTMiddleware(), controllers.GetWithdrawalHandler)

	api.Post("/list_deposit", utils.JWTMiddleware(), controllers.GetDepositHandler)

	api.Post("/register", controllers.Login)
	api.Post("/verify_otp", controllers.VerifyOTP)

	// metrics route omitted per your instruction (no Prometheus)
}
