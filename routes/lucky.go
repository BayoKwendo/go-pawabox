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
	api.Post("/place_bet_pawabox", utils.JWTMiddleware(), controllers.PlaceBetLuckyNumber)
	api.Post("/settle_bt_luckynumber", controllers.SettleBTLuckyNumber)
	api.Post("/settle_transaction", controllers.SettleBetLuckyNumber)

	api.Post("/place_bet_spin", utils.JWTMiddleware(), controllers.PlaceBetSpin)

	api.Post("/initiate_deposit", utils.JWTMiddleware(), controllers.IniatateDepositLuckyNumber)

	api.Post("/settle_withdrawal", controllers.SettleWithdrawalLuckyNumber)
	api.Post("/settle_withdrawal_b2b", controllers.SettleWithdrawalB2BLuckyNumber)
	api.Get("/lucky_games", utils.OptionalJWTMiddleware(), controllers.GetGames)
	api.Post("/login", controllers.Login)

	api.Get("/user", utils.JWTMiddleware(), controllers.GetUser)

	api.Post("/update_profile_pic", utils.JWTMiddleware(), controllers.UpdateUserProfilePic)

	api.Put("/user", utils.JWTMiddleware(), controllers.UpdateUser)

	api.Post("/request_delete_user", utils.JWTMiddleware(), controllers.RequestAccountDeletion)

	api.Post("/delete_user", utils.JWTMiddleware(), controllers.DeleteUser)

	api.Post("/update_show_win", utils.JWTMiddleware(), controllers.UpdateUserWinStatus)

	api.Post("/bet_history", utils.JWTMiddleware(), controllers.GetHistoryHandler)

	api.Post("/game_history", utils.JWTMiddleware(), controllers.GetGameHistoryHandler)

	api.Post("/list_withdrawal", utils.JWTMiddleware(), controllers.GetWithdrawalHandler)

	api.Post("/list_deposit", utils.JWTMiddleware(), controllers.GetDepositHandler)

	api.Post("/register", controllers.Login)

	api.Post("/apply_promo", controllers.ApplyPromo)

	api.Get("/get_year", controllers.GetYear)

	api.Get("/spin_bet_type", utils.JWTMiddleware(), controllers.GetBetAmount)

	api.Post("/request_self_exclusion_period", utils.JWTMiddleware(), controllers.RequestSelfExlusion)
	api.Post("/verify_self_exclusion_period", utils.JWTMiddleware(), controllers.VerySelfExlusion)

	api.Post("/verify_otp", controllers.VerifyOTP)

	// metrics route omitted per your instruction (no Prometheus)
}
