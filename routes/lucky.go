package routes

import (
	"fiberapp/controllers"

	"github.com/gofiber/fiber/v2"
)

func RegisterRoutes(app *fiber.App) {
	api := app.Group("/api/v1")

	api.Get("/", controllers.Hello)
	api.Get("/test", controllers.Test)
	api.Post("/place_bet_luckynumber", controllers.PlaceBetLuckyNumber)
	api.Post("/settle_bt_luckynumber", controllers.SettleBTLuckyNumber)
	api.Post("/settle_bet_luckynumber", controllers.SettleBetLuckyNumber)
	api.Post("/settle_withdrawal", controllers.SettleWithdrawalLuckyNumber)
	api.Post("/settle_withdrawal_b2b", controllers.SettleWithdrawalB2BLuckyNumber)
	api.Post("/lucky_games", controllers.GetGames)

	// metrics route omitted per your instruction (no Prometheus)
}
