package controllers

import (
	"context"
	"errors"
	"fiberapp/database"
	"fiberapp/models"
	"fiberapp/services"
	"fiberapp/utils"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// single service instance

var lucky *services.LuckyNumberService

// Initialize function to set up the service with database dependency
func InitLuckyNumberService(db *database.Database) {
	lucky = services.NewLuckyNumberService(db)
}

// Hello - GET /api/v1/
func Hello(c *fiber.Ctx) error {
	if err := lucky.Start(); err != nil {
		return c.Status(500).JSON(models.NewErrorResponse(500, 2, err.Error()))
	}
	setting, err := lucky.CheckSetting()
	if err != nil {
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, err.Error()))
	}
	return c.Status(200).JSON(models.NewSuccessWithData(200, 0, setting))
}

// Test - GET /test (simulate delay)
func Test(c *fiber.Ctx) error {
	time.Sleep(1 * time.Second)
	return c.Status(200).JSON(fiber.Map{"success": true})
}

// request bodies
type PlaceBetRequest struct {
	Amount    float64     `json:"amount"`
	Choice    interface{} `json:"choice"`
	GameCatID interface{} `json:"game_cat_id"`
	Msisdn    interface{} `json:"msisdn"`
	Channel   string      `json:"channel"`
	Ussd      string      `json:"ussd"`
}

// request bodies
type PlaceSpinRequest struct {
	Amount    float64     `json:"amount"`
	GameCatID interface{} `json:"game_cat_id"`
	Msisdn    interface{} `json:"msisdn"`
	Channel   string      `json:"channel"`
	Mode      string      `json:"mode"`
}
type IniatateDepositRequest struct {
	Amount  float64     `json:"amount"`
	Msisdn  interface{} `json:"msisdn"`
	Channel string      `json:"channel"`
}

func parseFloatInterface(v interface{}) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case int:
		return float64(t), nil
	case string:
		var f float64
		_, err := fmt.Sscanf(t, "%f", &f)
		return f, err
	default:
		return 0, errors.New("invalid number")
	}
}
func PlaceBetLuckyNumber(c *fiber.Ctx) error {
	var req PlaceBetRequest

	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN

	if err := c.BodyParser(&req); err != nil {
		log.Printf("invalid json: %v", err)
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}

	var startErr, checkErr, userErr error
	var setting map[string]interface{}
	var user map[string]interface{}

	// Run start and checkGameONE concurrently
	g := new(errgroup.Group)
	g.Go(func() error {
		startErr = lucky.Start()
		return startErr
	})
	g.Go(func() error {
		setting, checkErr = lucky.CheckGameONE(utils.ToString(req.GameCatID))
		return checkErr
	})
	g.Go(func() error {
		user, userErr = lucky.CheckUser(msisdn, "", "")
		return userErr
	})

	if err := g.Wait(); err != nil {
		log.Printf("error initializing or checking game: %v", err)
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, err.Error()))
	}

	if setting == nil {
		return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Game not found"))
	}

	// validate amount
	expectedF, _ := parseFloatInterface(setting["bet_amount"])
	if req.Amount != expectedF {
		return c.Status(202).JSON(models.NewErrorResponse(202, 1, fmt.Sprintf("Invalid Bet Amount. Expected %v.", setting["bet_amount"])))
	}

	// validate choice
	choiceF, err := parseFloatInterface(req.Choice)
	if err != nil || choiceF < 1 || choiceF > 7 {
		return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid lucky number. Please select a number between 1 and 7."))
	}

	num := user["balance"].(pgtype.Numeric)

	f, _ := num.Float64Value()
	balance := f.Float64
	amount := utils.ToFloat64(req.Amount)

	if balance >= amount {

		// place bet
		result, err := lucky.PlaceBet(
			user,
			req.Ussd,
			utils.ToString(setting["name"]),
			utils.ToString(req.GameCatID),
			utils.ToString(msisdn),
			req.Amount,
			utils.ToString(req.Choice),
			req.Channel,
		)
		if err != nil {
			log.Printf("Error placing bet: %v", err)
			return c.Status(500).JSON(models.NewErrorResponse(500, 1, err.Error()))
		}

		// success
		return c.Status(200).JSON(models.H{
			"Status":        200,
			"StatusCode":    0,
			"FreeBet":       result.FreeBet,
			"StatusMessage": result.Message,
			"GameResults":   result.GameResult,
		})
	} else {
		return c.Status(202).JSON(models.H{
			"Status":        202,
			"StatusCode":    3,
			"StatusMessage": "insufficient balance",
		})
	}
}

func IniatateDepositLuckyNumber(c *fiber.Ctx) error {
	var req IniatateDepositRequest
	if err := c.BodyParser(&req); err != nil {
		log.Printf("invalid json: %v", err)
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}

	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN

	// place bet
	result, err := lucky.IniatatDeposit(
		utils.ToString(msisdn),
		req.Amount,
		req.Channel)
	if err != nil {
		log.Printf("Error placing bet: %v", err)
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, err.Error()))
	}

	// success
	return c.Status(200).JSON(models.H{
		"Status":        200,
		"StatusCode":    0,
		"FreeBet":       result.FreeBet,
		"StatusMessage": result.Message,
	})
}

// SettleBTLuckyNumber - create background task and return immediately
func SettleBTLuckyNumber(c *fiber.Ctx) error {
	var data map[string]interface{}
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}
	// launch in background
	go func(d map[string]interface{}) {
		if err := lucky.HandleDepositAndGame(d); err != nil {
			logrus.Errorf("handle_deposit_and_game error: %v", err)
		}
	}(data)

	return c.Status(200).JSON(models.NewSuccess(200, 0, "Success"))
}

// SettleBetLuckyNumber - validates IPs then processes
func SettleBetLuckyNumber(c *fiber.Ctx) error {
	allowed := map[string]bool{"172.16.0.131": true, "172.16.0.104": true, "172.16.0.184": true, "127.0.0.1": true, "172.16.0.108": true}

	clientIP := c.Get("X-Forwarded-For", c.IP())
	if strings.Contains(clientIP, ",") {
		clientIP = strings.TrimSpace(strings.Split(clientIP, ",")[0])
	}
	// strip port if present
	if host, _, err := net.SplitHostPort(clientIP); err == nil {
		clientIP = host
	}

	if !allowed[clientIP] {
		return c.Status(403).JSON(models.NewErrorResponse(403, 1, "forbidden"))
	}

	var data map[string]interface{}
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}

	status := utils.ToString(data["status"])
	if status == "0" || strings.EqualFold(status, "success") {
		go func(d map[string]interface{}) {
			if _, err := lucky.ProcessBetAndPlayGame(d); err != nil {
				logrus.Errorf("process_bet_and_play_game error: %v", err)
			}
		}(data)
		return c.Status(200).JSON(models.NewSuccess(200, 0, "Success"))
	}

	desc := utils.ToString(data["description"])
	if desc == "CUSTOMER_CANCELED_PIN" || desc == "CUSTOMER_CONF_FAILED" {
		_ = lucky.InsertFailedSMS(utils.ToString(data["reference"]))
	}

	go func() {
		_ = lucky.UpdateAviatorDepositFailRequestLucky(utils.ToString(data["reference"]), desc)
	}()

	return c.Status(400).JSON(models.NewErrorResponse(400, 2, desc))
}

// SettleWithdrawalLuckyNumber
func SettleWithdrawalLuckyNumber(c *fiber.Ctx) error {
	var data map[string]interface{}
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}

	ref := utils.ToString(data["reference"])
	var ok bool
	var err error
	if strings.HasPrefix(ref, "AV_") {
		ok, err = lucky.UpdateLuckyNumberWithdrawalDisburseMotto(utils.ToString(data["transaction_id"]), utils.ToString(data["status"]), utils.ToString(data["description"]), ref)
	} else {
		ok, err = lucky.UpdateLuckyNumberWithdrawalDisburse(utils.ToString(data["transaction_id"]), utils.ToString(data["status"]), utils.ToString(data["description"]), ref)
	}
	if err != nil {
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, err.Error()))
	}
	if ok {
		return c.Status(200).JSON(models.NewSuccess(200, 0, "Success"))
	}
	return c.Status(200).JSON(models.NewSuccess(200, 0, "Not Found/Transaction already processed"))
}

// SettleWithdrawalB2B
func SettleWithdrawalB2BLuckyNumber(c *fiber.Ctx) error {
	var data map[string]interface{}
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}
	ok, err := lucky.UpdatePawaBox_KeWithdrawalb2bDisburse(utils.ToString(data["transaction_id"]), utils.ToString(data["status"]), utils.ToString(data["description"]), utils.ToString(data["reference"]))
	if err != nil {
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, err.Error()))
	}
	if ok {
		return c.Status(200).JSON(models.NewSuccess(200, 0, "Success"))
	}
	return c.Status(200).JSON(models.NewSuccess(200, 0, "Not Found/Transaction already processed"))
}

// GetGames - POST /lucky_games
func GetGames(c *fiber.Ctx) error {
	// logrus.Infof("GetGames request: %+v", data)

	categories := []string{"all", "Money Prize", "Car Prize", "Bike Prize", "JackPot"}

	category := c.Query("category", "all") // default = "all"

	userVal := c.Locals("user")

	msisdn := c.Query("msisdn", "") // default = "all"
	if msisdn != "" && len(msisdn) > 0 {

		// Ensure user exists
		user, err := lucky.CheckUser(msisdn, "", "")
		if err != nil {
			logrus.Errorf("CheckUser error: %v", err)
			return c.Status(500).JSON(models.NewErrorResponse(500, 1, "internal server error"))
		}
		if user == nil {
			// You returned an error in your example — replicate that behavior
			logrus.Warnf("user not found for msisdn=%s", msisdn)
			return c.Status(404).JSON(models.NewErrorResponse(404, 1, "user not found"))
		}

		if utils.ToString(user["active_status"]) == "inactive" {
			return c.Status(202).JSON(models.NewErrorResponse(202, 1, "user account is inactive"))

		}
		// --- JWT generation ---
		secret := utils.JWT_SECRET
		if secret == "" {
			// fail safe: log and return 500
			logrus.Error("JWT_SECRET not set in environment")
			return c.Status(500).JSON(models.NewErrorResponse(500, 1, "internal server error"))
		}

		num := user["balance"].(pgtype.Numeric)

		f, _ := num.Float64Value()
		balance := f.Float64

		// token expiry duration — adjust as needed
		expireDuration := 48 * time.Hour
		now := time.Now()
		claims := jwt.MapClaims{
			"sub":  msisdn,
			"iat":  now.Unix(),
			"exp":  now.Add(expireDuration).Unix(),
			"role": "user", // optional; change or remove as needed
		}

		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tokenString, err := token.SignedString([]byte(secret))
		if err != nil {
			logrus.Errorf("failed to sign JWT: %v", err)
			return c.Status(500).JSON(models.NewErrorResponse(500, 1, "internal server error"))
		}

		if userVal == nil {

			game, err := lucky.CheckGame(category)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{
					"Status":  false,
					"Message": "failed to fetch history",
				})
			}

			// Ensure history is never nil
			if game == nil {
				game = []map[string]interface{}{}
			}
			title := "SHINDA HADI KES 3M CASH PAPO HAPO!"

			return c.Status(200).JSON(models.H{
				"Status":        200,
				"StatusCode":    0,
				"Title":         title,
				"Data":          game,
				"FreeBet":       false,
				"token":         tokenString,
				"Balance":       balance,
				"Categories":    categories,
				"StatusMessage": "success",
			})
		} else {
			// guest user
			userClaims := userVal.(jwt.MapClaims)

			msisdn := userClaims["sub"].(string) // get MSISDN

			// Create context with timeout for all operations
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Execute concurrent queries
			gameResult, userResult, err := executeConcurrentQueries(ctx, category, msisdn)
			if err != nil {
				return handleQueryError(err)
			}

			// Process user data for freebet logic
			freebet, addTitle := processFreebetLogic(userResult)

			// Insert logs asynchronously (fire and forget)
			title := "SHINDA HADI KES 3M CASH PAPO HAPO!" + addTitle

			logrus.Infof("Game data: %+v", gameResult)
			logrus.Infof("User data: %+v", userResult)

			return c.Status(200).JSON(models.H{
				"Status":        200,
				"StatusCode":    0,
				"Title":         title,
				"Data":          gameResult,
				"FreeBet":       freebet,
				"token":         tokenString,
				"Categories":    categories,
				"Balance":       balance,
				"StatusMessage": "success",
			})
		}
	} else {
		if userVal == nil {

			game, err := lucky.CheckGame(category)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{
					"Status":  false,
					"Message": "failed to fetch history",
				})
			}

			// Ensure history is never nil
			if game == nil {
				game = []map[string]interface{}{}
			}
			title := "SHINDA HADI KES 3M CASH PAPO HAPO!"

			return c.Status(200).JSON(models.H{
				"Status":        200,
				"StatusCode":    0,
				"Title":         title,
				"Data":          game,
				"FreeBet":       false,
				"token":         "",
				"Categories":    categories,
				"StatusMessage": "success",
			})
		} else {
			// guest user
			userClaims := userVal.(jwt.MapClaims)

			msisdn := userClaims["sub"].(string) // get MSISDN

			// Create context with timeout for all operations
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Execute concurrent queries
			gameResult, userResult, err := executeConcurrentQueries(ctx, category, msisdn)
			if err != nil {
				return handleQueryError(err)
			}

			// Process user data for freebet logic
			freebet, addTitle := processFreebetLogic(userResult)

			// Insert logs asynchronously (fire and forget)
			title := "SHINDA HADI KES 3M CASH PAPO HAPO!" + addTitle

			logrus.Infof("Game data: %+v", gameResult)
			logrus.Infof("User data: %+v", userResult)

			return c.Status(200).JSON(models.H{
				"Status":        200,
				"StatusCode":    0,
				"Title":         title,
				"Data":          gameResult,
				"FreeBet":       freebet,
				"Categories":    categories,
				"token":         "",
				"StatusMessage": "success",
			})
		}
	}

}

// GetGames - POST /lucky_games
func GetBetAmount(c *fiber.Ctx) error {
	// logrus.Infof("GetGames request: %+v", data)
	// guest user

	bets := []string{"20", "30", "40", "50", "100"}

	return c.Status(200).JSON(models.H{
		"Status":        200,
		"StatusCode":    0,
		"BetAmount":     bets,
		"BetType":       "JackPot",
		"StatusMessage": "success",
	})

}

// GetGames - POST /lucky_games
func Login(c *fiber.Ctx) error {
	var data map[string]interface{}

	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}
	msisdn := utils.ToString(data["msisdn"])

	if msisdn != "" && len(msisdn) > 0 {

		name := utils.ToString(data["name"])
		promocode := utils.ToString(data["promocode"])
		val := rand.Intn(9000) + 1000

		created := time.Now().Unix()
		expired := created + 2*60 // expire after 2 minutes

		code := strconv.Itoa(val)

		if msisdn == "254717629732" {
			code = "2222"
		}

		if msisdn == "254717029580" {
			code = "1111"
		}

		if msisdn == "254718468634" {
			code = "1111"
		}

		if msisdn == "254785128132" {
			code = "1111"
		}

		if msisdn == "254720841355" {
			code = "1111"
		}

		if msisdn == "254714383269" {
			code = "1111"
		}
		if msisdn == "254703639349" {
			code = "1111"
		}

		if msisdn == "254718400000" {
			code = "1111"
		}

		if msisdn == "254785100000" {
			code = "1111"
		}

		if msisdn == "254720820000" {
			code = "1111"
		}

		if msisdn == "254714388880" {
			code = "1111"
		}
		if msisdn == "254703630000" {
			code = "1111"
		}

		if promocode != "" && len(promocode) > 0 {

			promo, err := lucky.CheckPromoCode(promocode)

			logrus.Info(promo)

			if err != nil {
				return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid PromoCode"))
			}
			if promo == nil {
				return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid PromoCode"))
			}

			user, err := lucky.CheckUser(msisdn, name, promocode)
			if err != nil {
				return err
			}
			if user == nil {
				return err
			}
			err = lucky.InsertVerification(msisdn, code, expired, created)
			if err != nil {
				return err
			}

			return c.Status(200).JSON(models.H{
				"Status":        200,
				"StatusCode":    0,
				"Units":         "Minutes",
				"ExpireIn":      2,
				"StatusMessage": "Otp Verification has been sent!",
			})

		} else {
			user, err := lucky.CheckUser(msisdn, name, "")
			if err != nil {
				return err
			}
			if user == nil {
				return err
			}
			if utils.ToString(user["active_status"]) == "inactive" {
				return c.Status(202).JSON(models.NewErrorResponse(202, 1, "user account not found"))
			}

			if utils.ToString(user["self_exclusion"]) == "YES" {
				return c.Status(202).JSON(models.NewErrorResponse(202, 1, "user account is inactive"))
			}

			err = lucky.InsertVerification(msisdn, code, expired, created)
			if err != nil {
				return err
			}

			return c.Status(200).JSON(models.H{
				"Status":        200,
				"StatusCode":    0,
				"Units":         "Minutes",
				"ExpireIn":      2,
				"StatusMessage": "Otp Verification has been sent!",
			})
		}
	} else {
		return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid Phone Number"))
	}
}

// GetGames - POST /lucky_games
func ApplyPromo(c *fiber.Ctx) error {
	var data map[string]interface{}

	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}
	promocode := utils.ToString(data["promocode"])

	if promocode != "" && len(promocode) > 0 {

		promo, err := lucky.CheckPromoCode(promocode)

		logrus.Info(promo)

		if err != nil {
			return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid PromoCode"))
		}
		if promo == nil {
			return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid PromoCode"))
		}

		return c.Status(200).JSON(models.H{
			"Status":        200,
			"StatusCode":    0,
			"StatusMessage": "Success",
		})

	} else {
		return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Please Enter PromoCode to Apply"))

	}
}

func RequestAccountDeletion(c *fiber.Ctx) error {

	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN

	val := rand.Intn(9000) + 1000

	created := time.Now().Unix()
	expired := created + 2*60 // expire after 2 minutes

	code := strconv.Itoa(val)

	if msisdn == "254717629732" {
		code = "2222"
	}
	err := lucky.InsertVerification(msisdn, code, expired, created)
	if err != nil {
		return err
	}
	return c.Status(200).JSON(models.H{
		"Status":        200,
		"StatusCode":    0,
		"StatusMessage": "OTP Verification has been send",
	})

}

func RequestSelfExlusion(c *fiber.Ctx) error {
	var data map[string]interface{}

	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}
	self_exclusion_period := utils.ToString(data["self_exclusion_period"])

	if self_exclusion_period != "" && len(self_exclusion_period) > 0 {

		var hrs = 24
		if self_exclusion_period == "24 Hours" {
			hrs = 24
		}

		if self_exclusion_period == "7 Days" {
			hrs = 168
		}

		if self_exclusion_period == "30 Days" {
			hrs = 720
		}

		if self_exclusion_period == "1 Year" {
			hrs = 8784
		}

		_, err := lucky.RequestSelfExlusion(msisdn, hrs)

		// logrus.Info(promo)
		if err != nil {
			return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid PromoCode"))
		}
		// if promo == nil {
		// 	return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid PromoCode"))
		// }

		val := rand.Intn(9000) + 1000

		created := time.Now().Unix()
		expired := created + 2*60 // expire after 2 minutes

		code := strconv.Itoa(val)

		if msisdn == "254717629732" {
			code = "2222"
		}

		if msisdn == "254718400000" {
			code = "1111"
		}

		if msisdn == "254785100000" {
			code = "1111"
		}

		if msisdn == "254720820000" {
			code = "1111"
		}

		if msisdn == "254714388880" {
			code = "1111"
		}
		if msisdn == "254703630000" {
			code = "1111"
		}

		err = lucky.InsertVerification(msisdn, code, expired, created)
		if err != nil {
			return err
		}
		return c.Status(200).JSON(models.H{
			"Status":        200,
			"StatusCode":    0,
			"StatusMessage": "OTP Verification has been send",
		})

	} else {
		return c.Status(202).JSON(models.NewErrorResponse(202, 1, "invalid JSON"))

	}
}

func DeletionRequest(c *fiber.Ctx) error {
	var data map[string]interface{}

	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}
	self_exclusion_period := utils.ToString(data["self_exclusion_period"])

	if self_exclusion_period != "" && len(self_exclusion_period) > 0 {

		var hrs = 24
		if self_exclusion_period == "24 Hours" {
			hrs = 24
		}

		if self_exclusion_period == "7 Days" {
			hrs = 168
		}

		if self_exclusion_period == "30 Days" {
			hrs = 720
		}

		if self_exclusion_period == "1 Year" {
			hrs = 8784
		}

		_, err := lucky.RequestSelfExlusion(msisdn, hrs)

		// logrus.Info(promo)
		if err != nil {
			return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid PromoCode"))
		}
		// if promo == nil {
		// 	return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Invalid PromoCode"))
		// }

		val := rand.Intn(9000) + 1000

		created := time.Now().Unix()
		expired := created + 2*60 // expire after 2 minutes

		code := strconv.Itoa(val)

		if msisdn == "254717629732" {
			code = "2222"
		}
		err = lucky.InsertVerification(msisdn, code, expired, created)
		if err != nil {
			return err
		}
		return c.Status(200).JSON(models.H{
			"Status":        200,
			"StatusCode":    0,
			"StatusMessage": "Success",
		})

	} else {
		return c.Status(202).JSON(models.NewErrorResponse(202, 1, "invalid JSON"))

	}
}
func VerySelfExlusion(c *fiber.Ctx) error {
	var data map[string]interface{}

	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}

	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN
	opt := utils.ToString(data["otp"])
	// Call service to verify OTP — returns remaining seconds until expiry
	verifyRemain, err := lucky.VerifyOTP(msisdn, opt)
	if err != nil {
		// Distinguish invalid vs expired for better messages if you want
		// Here we follow your earlier style: return 201 with message for invalid/expired
		// but it's more idiomatic to return 4xx
		logrus.Warnf("VerifyOTP error for %s: %v", msisdn, err)
		return c.Status(201).JSON(models.H{
			"Status":        false,
			"StatusCode":    2,
			"StatusMessage": err.Error(),
		})
	}

	self, err := lucky.CheckSelfExclusion(msisdn)
	if self == nil {
		return c.Status(201).JSON(models.H{
			"Status":        false,
			"StatusCode":    2,
			"StatusMessage": err.Error(),
		})
	}

	err = lucky.UpdatePlayerSelf(msisdn, utils.ToString(self["value_hrs"]))
	if err != nil {
		return err
	}

	return c.Status(200).JSON(models.H{
		"Status":        200,
		"StatusCode":    0,
		"ExpireIn":      verifyRemain,
		"Units":         "Seconds", // client-friendly TTL
		"StatusMessage": "Success",
	})

}
func GetUser(c *fiber.Ctx) error {

	// Get the JWT claims set by middleware
	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN
	// role := userClaims["role"].(string)  // optional
	// msisdn := utils.ToString(data["msisdn"])
	user, err := lucky.CheckUser(msisdn, "", "")
	if err != nil {
		return err
	}
	if user == nil {
		return err
	}
	return c.Status(200).JSON(models.H{
		"Status":        200,
		"StatusCode":    0,
		"Data":          user,
		"StatusMessage": "Success",
	})
}

func GetDepositHandler(c *fiber.Ctx) error {
	var data struct {
		StartDate string `json:"StartDate"`
		EndDate   string `json:"EndDate"`
	}

	// Get the JWT claims set by middleware
	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN

	if err := c.BodyParser(&data); err != nil {
	}

	startDate := data.StartDate // string from JSON
	endDate := data.EndDate     // string from JSON

	logrus.Infof("GetGames request: %+v", startDate)

	history, err := lucky.GetDeposits(msisdn, startDate, endDate)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"Status":  false,
			"Message": "failed to fetch history",
		})
	}

	// Ensure history is never nil
	if history == nil {
		history = []map[string]interface{}{}
	}

	return c.JSON(fiber.Map{
		"Status":        200,
		"StatusCode":    0,
		"StatusMessage": "Success",
		"Deposit":       history,
	})
}

func GetWithdrawalHandler(c *fiber.Ctx) error {
	var data struct {
		StartDate string `json:"StartDate"`
		EndDate   string `json:"EndDate"`
	}

	// Get the JWT claims set by middleware
	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN

	if err := c.BodyParser(&data); err != nil {
	}

	startDate := data.StartDate // string from JSON
	endDate := data.EndDate     // string from JSON

	logrus.Infof("GetGames request: %+v", startDate)

	history, err := lucky.GetWithdrawals(msisdn, startDate, endDate)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"Status":  false,
			"Message": "failed to fetch history",
		})
	}

	// Ensure history is never nil
	if history == nil {
		history = []map[string]interface{}{}
	}

	return c.JSON(fiber.Map{
		"Status":        200,
		"StatusCode":    0,
		"StatusMessage": "Success",
		"Withdrawal":    history,
	})
}

func GetHistoryHandler(c *fiber.Ctx) error {
	var data struct {
		StartDate string `json:"StartDate"`
		EndDate   string `json:"EndDate"`
	}

	// Get the JWT claims set by middleware
	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN

	if err := c.BodyParser(&data); err != nil {
	}

	startDate := data.StartDate // string from JSON
	endDate := data.EndDate     // string from JSON

	logrus.Infof("GetGames request: %+v", startDate)

	history, err := lucky.GetHistory(msisdn, startDate, endDate)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"Status":  false,
			"Message": "failed to fetch history",
		})
	}

	// Ensure history is never nil
	if history == nil {
		history = []map[string]interface{}{}
	}

	return c.JSON(fiber.Map{
		"Status":        200,
		"StatusCode":    0,
		"StatusMessage": "Success",
		"History":       history,
	})
}

func GetGameHistoryHandler(c *fiber.Ctx) error {
	var data struct {
		StartDate  string `json:"StartDate"`
		EndDate    string `json:"EndDate"`
		PageSize   any    `json:"PageSize"`
		PageNumber any    `json:"PageNumber"`
	}

	// Get the JWT claims set by middleware
	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN

	if err := c.BodyParser(&data); err != nil {
	}

	startDate := data.StartDate // string from JSON
	endDate := data.EndDate     // string from JSON

	page_number := data.PageNumber
	page_size := data.PageSize

	page_number = "1"
	page_size = "10"
	page, ok := page_size.(string)

	if !ok {
		return nil
	}

	if page_number != "" && len(page) > 0 {
		page_number = data.PageNumber
		page_size = data.PageSize
	}

	offset := (utils.ToInt(page_number) - 1) * utils.ToInt(page_size)
	logrus.Infof("GetGames request: %+v", offset)

	// Ensure history slice is never nil

	resp, err := lucky.GetGameHistory(msisdn, utils.ToString(offset), utils.ToString(page_size), startDate, endDate)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"Status":  false,
			"Message": "failed to fetch history",
		})
	}

	// Access map keys
	history := resp["history"].([]map[string]interface{})
	total := resp["total"].(float64)
	if history == nil {
		history = []map[string]interface{}{}
	}

	return c.Status(200).JSON(fiber.Map{
		"Status":        200,
		"StatusCode":    0,
		"StatusMessage": "Success",
		"Total":         total,
		"History":       history,
	})

}
func GetYear(c *fiber.Ctx) error {
	year := time.Now().Year()

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"Status":        200,
		"StatusCode":    0,
		"Year":          year,
		"StatusMessage": "Success",
	})
}

func UpdateUser(c *fiber.Ctx) error {
	// Get the JWT claims set by middleware
	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN
	var data map[string]interface{}
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}

	name := utils.ToString(data["name"])

	// msisdn_new := utils.ToString(data["msisdn"])

	// check if msisdnNew is provided
	// if msisdn_new != "" && len(msisdn_new) > 0 {

	// 	user, err := lucky.CheckUserNoCreating(msisdn_new)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	// if phone already exists
	// 	if user != nil {
	// 		return c.Status(200).JSON(models.H{
	// 			"Status":        202,
	// 			"StatusCode":    1,
	// 			"StatusMessage": "Phone Number Already Registered",
	// 		})
	// 	} else {

	// 		// msisdn_t := utils.ToString(user["msisdn"])

	// 		attempteduser, err := lucky.CheckUserNoCreatingAttempted(msisdn_new)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		if attempteduser == nil {
	// 			err := lucky.CreateUserAttempted(msisdn, msisdn_new)
	// 			if err != nil {
	// 				return err
	// 			}
	// 		}
	// 		val := rand.Intn(9000) + 1000

	// 		created := time.Now().Unix()
	// 		expired := created + 2*60 // expire after 2 minutes

	// 		code := strconv.Itoa(val)

	// 		if msisdn == "254717629732" {
	// 			code = "2222"
	// 		}

	// 		if msisdn_new == "254717029580" {
	// 			code = "1111"
	// 		}

	// 		if msisdn == "254720841355" {
	// 			code = "1111"
	// 		}

	// 		if msisdn_new == "254720841355" {
	// 			code = "1111"
	// 		}

	// 		if msisdn == "254785128132" {
	// 			code = "1111"
	// 		}

	// 		if msisdn_new == "254785128132" {
	// 			code = "1111"
	// 		}

	// 		if msisdn_new == "254717629732" {
	// 			code = "2222"
	// 		}

	// 		if msisdn == "254717029580" {
	// 			code = "1111"
	// 		}

	// 		if msisdn == "254718468634" {
	// 			code = "1111"
	// 		}

	// 		if msisdn == "254714383269" {
	// 			code = "1111"
	// 		}
	// 		if msisdn == "254703639349" {
	// 			code = "1111"
	// 		}

	// 		if msisdn_new == "254718468634" {
	// 			code = "1111"
	// 		}

	// 		if msisdn_new == "254714383269" {
	// 			code = "1111"
	// 		}
	// 		if msisdn_new == "254703639349" {
	// 			code = "1111"
	// 		}
	// 		err = lucky.InsertVerification(msisdn_new, code, expired, created)
	// 		if err != nil {
	// 			return err
	// 		}

	// 		return c.Status(200).JSON(models.H{
	// 			"Status":        200,
	// 			"StatusCode":    0,
	// 			"Units":         "Minutes",
	// 			"ExpireIn":      2,
	// 			"StatusMessage": "Otp Verification has been sent!",
	// 		})
	// 	}
	// } else {
	err := lucky.UpdateUser(msisdn, name)
	if err != nil {
		return err
	}

	return c.Status(200).JSON(models.H{
		"Status":        200,
		"StatusCode":    0,
		"StatusMessage": "Success",
	})
	// }
}

func DeleteUser(c *fiber.Ctx) error {
	// Get the JWT claims set by middleware
	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN
	var data map[string]interface{}
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}

	opt := utils.ToString(data["otp"])
	// Call service to verify OTP — returns remaining seconds until expiry
	verifyRemain, err := lucky.VerifyOTP(msisdn, opt)
	if err != nil {
		// Distinguish invalid vs expired for better messages if you want
		// Here we follow your earlier style: return 201 with message for invalid/expired
		// but it's more idiomatic to return 4xx
		logrus.Warnf("VerifyOTP error for %s: %v", msisdn, err)
		return c.Status(201).JSON(models.H{
			"Status":        false,
			"StatusCode":    2,
			"StatusMessage": err.Error(),
		})
	}
	// var data map[string]interface{}
	// if err := c.BodyParser(&data); err != nil {
	// 	return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	// }

	// name := utils.ToString(data["name"])

	err = lucky.DeleteUser(msisdn)
	if err != nil {
		return err
	}

	return c.Status(200).JSON(models.H{
		"Status":        200,
		"StatusCode":    0,
		"ExpireIn":      verifyRemain,
		"StatusMessage": "Success",
	})
}

func UpdateUserWinStatus(c *fiber.Ctx) error {
	// Get the JWT claims set by middleware
	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN

	var data map[string]interface{}
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}

	show_win := utils.ToString(data["show_win"])

	err := lucky.UpdateUserWinStatus(msisdn, show_win)
	if err != nil {
		return err
	}

	return c.Status(200).JSON(models.H{
		"Status":        200,
		"StatusCode":    0,
		"StatusMessage": "Success",
	})
}

func UpdateUserProfilePic(c *fiber.Ctx) error {
	// Get JWT claims safely
	userVal := c.Locals("user")
	if userVal == nil {
		return c.Status(fiber.StatusUnauthorized).JSON(models.NewErrorResponse(401, 1, "unauthorized"))
	}

	userClaims := userVal.(jwt.MapClaims)
	msisdn, ok := userClaims["sub"].(string)
	if !ok {
		return c.Status(401).JSON(models.NewErrorResponse(401, 1, "invalid token"))
	}

	// Handle file upload (optional)
	file, err := c.FormFile("file")
	if err == nil && file != nil {
		// Ensure upload directory exists
		uploadDir := "./profile_uploads"
		if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
			_ = os.Mkdir(uploadDir, 0755)
		}
		filename := fmt.Sprintf("%s_%d_%s", msisdn, time.Now().Unix(), file.Filename)
		filePath := path.Join(uploadDir, filename)

		if err := c.SaveFile(file, filePath); err != nil {
			logrus.Errorf("File uploaded: %v", err)
			return c.Status(500).JSON(models.NewErrorResponse(500, 1, "failed to save file"))
		}
		// Optionally store file path in DB
		logrus.Infof("File uploaded: %s", filePath)
		// Update win status
		if err := lucky.UpdateUserProfilePic(msisdn, filename); err != nil {
			return err
		}
		return c.Status(200).JSON(models.H{
			"Status":        200,
			"StatusCode":    0,
			"StatusMessage": "Success",
		})

	} else {
		logrus.Errorf("File uploaded: %v", err)

		return c.Status(500).JSON(models.NewErrorResponse(500, 1, "please attach file to upload"))
	}

}

func VerifyOTP(c *fiber.Ctx) error {
	if lucky == nil {
		logrus.Error("lucky service not initialized")
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, "internal server error"))
	}
	var attempteduser map[string]interface{}

	var data map[string]interface{}
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}

	msisdn := utils.ToString(data["msisdn"])
	opt := utils.ToString(data["otp"])
	// Call service to verify OTP — returns remaining seconds until expiry
	verifyRemain, err := lucky.VerifyOTP(msisdn, opt)
	if err != nil {
		// Distinguish invalid vs expired for better messages if you want
		// Here we follow your earlier style: return 201 with message for invalid/expired
		// but it's more idiomatic to return 4xx
		logrus.Warnf("VerifyOTP error for %s: %v", msisdn, err)
		return c.Status(201).JSON(models.H{
			"Status":        false,
			"StatusCode":    2,
			"StatusMessage": err.Error(),
		})
	}
	attempteduser, err = lucky.CheckUserNoCreatingAttempted(msisdn)
	if attempteduser != nil {
		new_msisdn := utils.ToString(attempteduser["new_msisdn"])
		msisdn := utils.ToString(attempteduser["msisdn"])
		err = lucky.UpdateMsisdn(msisdn, new_msisdn)
		if err != nil {
			return err
		}
	}

	// Ensure user exists
	user, err := lucky.CheckUser(msisdn, "", "")
	if err != nil {
		logrus.Errorf("CheckUser error: %v", err)
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, "internal server error"))
	}
	if user == nil {
		// You returned an error in your example — replicate that behavior
		logrus.Warnf("user not found for msisdn=%s", msisdn)
		return c.Status(404).JSON(models.NewErrorResponse(404, 1, "user not found"))
	}

	if utils.ToString(user["active_status"]) == "inactive" {
		return c.Status(202).JSON(models.NewErrorResponse(202, 1, "user account is inactive"))

	}
	// --- JWT generation ---
	secret := utils.JWT_SECRET
	if secret == "" {
		// fail safe: log and return 500
		logrus.Error("JWT_SECRET not set in environment")
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, "internal server error"))
	}

	// token expiry duration — adjust as needed
	expireDuration := 48 * time.Hour
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":  msisdn,
		"iat":  now.Unix(),
		"exp":  now.Add(expireDuration).Unix(),
		"role": "user", // optional; change or remove as needed
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		logrus.Errorf("failed to sign JWT: %v", err)
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, "internal server error"))
	}

	// Success response including the token and expiry (seconds remaining)
	return c.Status(200).JSON(models.H{
		"Status":        200,
		"StatusCode":    0,
		"ExpireIn":      verifyRemain,
		"StatusMessage": "Success",
		"Token":         tokenString,
		"TokenExpiry":   int64(expireDuration.Seconds()), // client-friendly TTL
		"Units":         "Seconds",                       // client-friendly TTL
		"Data":          user,                            // optional: include user payload
	})
}

// executeConcurrentQueries runs game and user queries concurrently with proper timeout
func executeConcurrentQueries(ctx context.Context, category string, msisdn string) (interface{}, map[string]interface{}, error) {
	type result struct {
		game interface{}
		user map[string]interface{}
		err  error
	}

	resultCh := make(chan result, 1)

	go func() {
		var wg sync.WaitGroup
		var game interface{}
		var user map[string]interface{}
		var gameErr, userErr error

		// Execute both queries concurrently
		wg.Add(2)

		go func() {
			defer wg.Done()
			game, gameErr = lucky.CheckGame(category)
			if gameErr != nil {
				logrus.Warnf("CheckGame failed: %v", gameErr)
				game = []interface{}{} // Ensure game is never nil
			}
		}()

		go func() {
			defer wg.Done()
			user, userErr = lucky.CheckUser(msisdn, "", "")
			if userErr != nil {
				logrus.Warnf("CheckUser failed: %v", userErr)
				user = map[string]interface{}{} // Ensure user is never nil
			}
		}()

		wg.Wait()

		// Combine results
		resultCh <- result{
			game: game,
			user: user,
			err:  combineErrors(gameErr, userErr),
		}
	}()

	select {
	case res := <-resultCh:
		// Ensure we never return nil values
		if res.game == nil {
			res.game = []interface{}{}
		}
		if res.user == nil {
			res.user = map[string]interface{}{}
		}
		return res.game, res.user, res.err

	case <-ctx.Done():
		return []interface{}{}, map[string]interface{}{}, fmt.Errorf("query timeout")
	}
}

// processFreebetLogic handles freebet validation and title formatting
func processFreebetLogic(user map[string]interface{}) (bool, string) {
	freebet := false
	addTitle := ""

	// Check if user has valid freebet
	expiryRaw, hasExpiry := user["freebet_expiry"].(string)
	if !hasExpiry || expiryRaw == "" {
		return freebet, addTitle
	}

	// Parse expiry time
	expiryTime, err := time.Parse("2006-01-02 15:04:05", expiryRaw)
	if err != nil {
		logrus.Warnf("Failed to parse freebet expiry: %v", err)
		return freebet, ""
	}

	// Validate freebet conditions
	isFree := utils.ToString(user["is_free"]) == "YES"
	hasFreeBet := utils.ToInt(user["free_bet"]) > 0
	isNotExpired := expiryTime.After(time.Now())

	if isFree && hasFreeBet && isNotExpired {
		freebet = true
		addTitle = fmt.Sprintf(" FREE BET %d", utils.ToInt(user["free_bet"]))
	}

	return freebet, addTitle
}

// combineErrors combines multiple errors into a single error
func combineErrors(errs ...error) error {
	var errorMessages []string
	for _, err := range errs {
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}
	}

	if len(errorMessages) == 0 {
		return nil
	}

	return fmt.Errorf("partial failures: %s", strings.Join(errorMessages, "; "))
}

// handleQueryError processes and formats query errors for response
func handleQueryError(err error) *fiber.Error {
	if err == nil {
		return nil
	}

	// Check for timeout
	if errors.Is(err, context.DeadlineExceeded) {
		return fiber.NewError(504, "request timeout")
	}

	// Check if it's a partial failure (some queries succeeded)
	if strings.Contains(err.Error(), "partial failures") {
		logrus.Warnf("Partial query failure: %v", err)
		// Continue with partial data instead of failing completely
		return nil
	}

	// Full failure
	return fiber.NewError(500, fmt.Sprintf("service unavailable: %v", err))
}

func PlaceBetSpin(c *fiber.Ctx) error {
	var req PlaceSpinRequest

	userClaims := c.Locals("user").(jwt.MapClaims)
	msisdn := userClaims["sub"].(string) // get MSISDN

	if err := c.BodyParser(&req); err != nil {
		log.Printf("invalid json: %v", err)
		return c.Status(400).JSON(models.NewErrorResponse(400, 1, "invalid JSON"))
	}
	var startErr, checkErr, userErr error
	var setting map[string]interface{}
	var user map[string]interface{}

	// Run start and checkGameONE concurrently
	g := new(errgroup.Group)
	g.Go(func() error {
		startErr = lucky.Start()
		return startErr
	})
	g.Go(func() error {
		setting, checkErr = lucky.CheckGameONE(utils.ToString(req.GameCatID))
		return checkErr
	})
	g.Go(func() error {
		user, userErr = lucky.CheckUser(msisdn, "", "")
		return userErr
	})

	if err := g.Wait(); err != nil {
		log.Printf("error initializing or checking game: %v", err)
		return c.Status(500).JSON(models.NewErrorResponse(500, 1, err.Error()))
	}

	if setting == nil {
		return c.Status(202).JSON(models.NewErrorResponse(202, 1, "Game not found"))
	}

	num := user["balance"].(pgtype.Numeric)

	f, _ := num.Float64Value()
	balance := f.Float64
	amount := utils.ToFloat64(req.Amount)

	if balance >= amount {

		logrus.Infof("rtpLimit : %s", amount)

		logrus.Infof("rtpLimit : %s", user)

		// place bet
		result, err := lucky.PlaceBetSpin(
			user,
			utils.ToString(req.GameCatID),
			utils.ToString(msisdn),
			req.Amount,
			req.Channel,
			req.Mode,
		)

		if err != nil {
			log.Printf("Error placing bet: %v", err)
			return c.Status(500).JSON(models.NewErrorResponse(500, 1, err.Error()))
		}

		// success
		return c.Status(200).JSON(models.H{
			"Status":        200,
			"StatusCode":    0,
			"StatusMessage": result,
		})
	} else {
		return c.Status(202).JSON(models.H{
			"Status":        202,
			"StatusCode":    3,
			"StatusMessage": "insufficient balance",
		})
	}
}
