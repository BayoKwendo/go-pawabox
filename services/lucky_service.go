package services

import (
	"context"
	"fiberapp/database"
	"fiberapp/utils"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// LuckyNumberService handles the lucky number game logic
type LuckyNumberService struct {
	mu          sync.Mutex
	db          *database.Database // Your database client
	playersData map[int64]*PlayerData
	texts       map[string]map[string]string // SMS templates
}

type GenerateWinAmountsParams struct {
	Msisdn           string
	KPI              map[string]interface{}
	DefaultRTP       float64
	AdjustmentRTP    float64
	PlayerRTP        float64
	Reference        string
	BetAmount        float64
	SelectedNumber   string
	PlayerID         int64
	MinWinMultiplier float64
	MaxWinMultiplier float64
	MaxExposure      float64
	GameNameInit     string
	PlayerLostCount  int64
	MinLossCount     int
	MaxWon           float64
	VigPercentage    float64
	RTPOverload      float64
}

type WinAmount struct {
	Value float64
	Item  string
}

type PlayGameParams struct {
	TransactionID  string
	Shortcode      string
	Name           string
	BetHistory     []map[string]interface{}
	GameCatID      string
	User           map[string]interface{}
	Msisdn         string
	Amount         float64
	SelectedNumber string
	Reference      string
	BetType        string
	Description    string
	Channel        string
	USSD           string
	GameName       string
}

// PlayerData stores player information
type PlayerData struct {
	TotalBets   float64
	Payout      float64
	TotalLosses float64
	Sessions    string
	CurrentRTP  float64
	History     []map[string]interface{}
}

// PlaceBetResult represents the result of a bet placement
type PlaceBetResult struct {
	FreeBet bool
	Message string
}

// NewLuckyNumberService creates a new LuckyNumberService instance
func NewLuckyNumberService(db *database.Database) *LuckyNumberService {
	return &LuckyNumberService{
		db:          db,
		playersData: make(map[int64]*PlayerData),
		texts: map[string]map[string]string{
			"results": {
				"win":       "Box %d wins! You won: %s. Numbers: %s. Free bets: %d. Ref: %s. Tax: %d%% (%s)",
				"loss":      "Box %d loses. Numbers: %s. Free bets: %d. Ref: %s",
				"jackpot":   "Congratulations! Jackpot win! Ref: %s, Item: %s, Amount: %.0f",
				"cancelled": "Transaction cancelled. Please try again.",
			},
		},
	}
}

func (s *LuckyNumberService) Start() error {
	// Initialize connections if needed
	return nil
}

func (s *LuckyNumberService) CheckSetting() (map[string]interface{}, error) {
	ctx := context.Background()
	return s.db.CheckSetting(ctx)
}

func (s *LuckyNumberService) CheckGameONE(gameCatID interface{}) (map[string]interface{}, error) {
	ctx := context.Background()

	// Type assertion to string
	catIDStr, ok := gameCatID.(string)
	if !ok {
		return nil, fmt.Errorf("gameCatID must be a string, got %T", gameCatID)
	}

	return s.db.CheckGameONE(ctx, catIDStr)
}
func (s *LuckyNumberService) CheckGame() (interface{}, error) {
	ctx := context.Background()

	return s.db.CheckGames(ctx)
}

func (s *LuckyNumberService) CheckUser(msisdn string) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		log.Printf("PANIC PREVENTION: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}

	ctx := context.Background()
	return s.db.CheckUser(ctx, msisdn)
}

func (s *LuckyNumberService) InsertLogs(msisdn, sessionId, serviceCode, ussdString string) error {
	ctx := context.Background()
	_, err := s.db.InsertUSSDLogs(ctx, msisdn, sessionId, serviceCode, ussdString)
	return err
}

// PlaceBet handles the main betting logic
func (s *LuckyNumberService) PlaceBet(ussd string, name string, gameCatID string, msisdn string, amount float64, selectedNumber string, channel string) (PlaceBetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()

	// Check user
	user, err := s.db.CheckUser(ctx, msisdn)
	if err != nil {
		return PlaceBetResult{}, err
	}

	mnoCategory := s.getMNOCategory(msisdn)

	// Create attempted user if doesn't exist
	if user == nil {
		_, err := s.db.CreateUserAttempted(ctx, mnoCategory, msisdn)

		logrus.Errorf("Error placing bet: %v", err)

		if err != nil {
			return PlaceBetResult{}, err
		}
	}

	gameID := s.randomString(10)

	// Check if user has active free bet
	if user != nil && s.hasActiveFreeBet(user) {
		totalBetsHist, err := s.db.CheckBets(ctx, msisdn)
		if err != nil {
			return PlaceBetResult{}, err
		}
		_, err = s.db.UpdateUserLucky(ctx, msisdn)
		if err != nil {
			return PlaceBetResult{}, err
		}

		// Refresh user data
		user, err = s.db.CheckUser(ctx, msisdn)
		if err != nil {
			return PlaceBetResult{}, err
		}

		// Play game immediately for free bet
		err = s.playGame(ctx, gameID, "", name, totalBetsHist, gameCatID, user, msisdn, amount, selectedNumber, gameID, "free_bet", "free bet", channel, ussd, name)
		if err != nil {
			return PlaceBetResult{}, err
		}

		return PlaceBetResult{FreeBet: true, Message: "Free Bet placed successful! Jaribu Tena."}, nil
	} else {
		// Regular bet - adjust amount based on previous bet
		adjustedAmount, err := s.adjustBetAmount(ctx, msisdn, amount)
		if err != nil {
			return PlaceBetResult{}, err
		}

		_, err = s.db.UpdateUserLuckyFree(ctx, msisdn)
		if err != nil {
			return PlaceBetResult{}, err
		}

		_, err = s.db.InsertIntoDepositLuckyRequest(ctx, ussd, name, mnoCategory, gameCatID, adjustedAmount, msisdn, selectedNumber, gameID, channel)
		if err != nil {
			return PlaceBetResult{}, err
		}

		_, err = s.db.InsertSTK(ctx, name, mnoCategory, gameID, msisdn, adjustedAmount, "00000")
		if err != nil {
			return PlaceBetResult{}, err
		}

		return PlaceBetResult{FreeBet: false, Message: "Kukamilisha BET weka M-Pesa PIN yako."}, nil
	}
}

// HandleDepositAndGame processes deposit and starts the game
func (s *LuckyNumberService) HandleDepositAndGame(data map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()

	transactionID, _ := data["transaction_id"].(string)
	reference, _ := data["reference"].(string)
	name, _ := data["name"].(string)

	// Check transaction and deposit request
	checkTransaction, err := s.db.CheckTransaction(ctx, transactionID)
	if err != nil {
		return err
	}

	stkUSSD, err := s.db.CheckDepositRequestLucky(ctx, reference)
	if err != nil {
		return err
	}

	if checkTransaction == nil && stkUSSD != nil && stkUSSD["msisdn"] != nil {
		msisdn := stkUSSD["msisdn"].(string)
		user, err := s.db.CheckUser(ctx, msisdn)
		if err != nil {
			return err
		}

		// Create user if doesn't exist
		if user == nil {
			mnoCategory := s.getMNOCategory(msisdn)
			_, err = s.db.CreateUser(ctx, mnoCategory, msisdn)
			if err != nil {
				return err
			}
			user, err = s.db.CheckUser(ctx, msisdn)
			if err != nil {
				return err
			}
		}

		amount := stkUSSD["amount"].(float64)
		_, err = s.db.UpdateUserAviatorBalInfoLucky(ctx, amount, msisdn, name)
		if err != nil {
			return err
		}

		// Extract game data and start the game
		gameCatID := stkUSSD["game_cat_id"].(string)
		selectedNumber := stkUSSD["selected_box"].(string)
		channel, _ := stkUSSD["channel"].(string)
		ussd, _ := stkUSSD["ussd"].(string)
		gameName, _ := stkUSSD["game"].(string)

		err = s.playGame(ctx, transactionID, "", name, nil, gameCatID, user, msisdn, amount, selectedNumber, reference, "normal", "deposit game", channel, ussd, gameName)
		if err != nil {
			return err
		}
	}

	return nil
}

// SettleDeposit handles deposit settlement
func (s *LuckyNumberService) SettleDeposit(name, transactionID, reference string) (map[string]interface{}, error) {
	ctx := context.Background()

	// Check if transaction already exists
	transactionExists, err := s.db.CheckTransaction(ctx, transactionID)
	if err != nil {
		logrus.Errorf("Error checking transaction: %v", err)
		return nil, err
	}
	logrus.Infof("Transaction already : %s", transactionExists)

	if transactionExists != nil {
		logrus.Infof("Transaction already exists: %s", transactionID)
		return nil, fmt.Errorf("transaction already exists")
	}
	// Check deposit request
	depositRequest, err := s.db.CheckDepositRequestLucky(ctx, reference)
	if err != nil {
		logrus.Errorf("Error checking deposit request: %v", err)
		return nil, err
	}

	if depositRequest == nil {
		logrus.Errorf("Deposit request not found for reference: %s", reference)
		return nil, fmt.Errorf("deposit request not found")
	}

	logrus.Infof("depositRequest already : %s", depositRequest)

	msisdn := utils.ToString(depositRequest["msisdn"])
	if msisdn == "" {
		logrus.Errorf("MSISDN not found in deposit request: %s", reference)
		return nil, fmt.Errorf("msisdn not found in deposit request")
	}

	// Check if user exists
	user, err := s.db.CheckUser(ctx, msisdn)
	if err != nil {
		logrus.Errorf("Error checking user: %v", err)
		return nil, err
	}
	logrus.Infof("user already : %s", user)

	// Create user if doesn't exist
	if user == nil {
		carrier := s.getMNOCategory(msisdn)
		_, err := s.db.CreateUser(ctx, carrier, msisdn)
		if err != nil {
			logrus.Errorf("Error creating user: %v", err)
			return nil, err
		}
		// Get the newly created user
		user, err = s.db.CheckUser(ctx, msisdn)
		if err != nil {
			logrus.Errorf("Error getting new user: %v", err)
			return nil, err
		}
	}
	amount := (depositRequest["amount"]).(float64)
	// Update user balance
	_, err = s.db.UpdateUserAviatorBalInfoLucky(ctx, amount, msisdn, name)
	if err != nil {
		logrus.Errorf("Error updating user balance: %v", err)
		return nil, err
	}
	logrus.Infof("Deposit settled successfully: reference=%s, msisdn=%s, amount=%.2f",
		reference, msisdn, amount)

	return depositRequest, nil
}

// ProcessBetAndPlayGame handles the main game logic
func (s *LuckyNumberService) ProcessBetAndPlayGame(data map[string]interface{}) (map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	ref := utils.ToString(data["reference"])

	// Settle deposit first
	deposit, err := s.SettleDeposit(
		utils.ToString(data["name"]),
		utils.ToString(data["transaction_id"]),
		ref,
	)
	if err != nil {
		logrus.Errorf("Failed to settle deposit: %v", err)
		return nil, fmt.Errorf("failed to settle deposit: %w", err)
	}

	logrus.Infof("Deposit settled: %+v", deposit)

	if deposit != nil {
		msisdn := utils.ToString(deposit["msisdn"])

		// Run user and bet history checks concurrently
		var user map[string]interface{}
		var betshist []map[string]interface{}
		var userErr, betHistErr error

		var wg sync.WaitGroup
		wg.Add(2)

		// Check user concurrently
		go func() {
			defer wg.Done()
			user, userErr = s.db.CheckUser(ctx, msisdn)
		}()

		// Check bet history concurrently
		go func() {
			defer wg.Done()
			betshist, betHistErr = s.db.CheckBets(ctx, msisdn)
		}()

		wg.Wait()

		// Check for errors
		if userErr != nil {
			return nil, fmt.Errorf("failed to check user: %w", userErr)
		}
		if betHistErr != nil {
			return nil, fmt.Errorf("failed to check bet history: %w", betHistErr)
		}

		// Extract data from the request
		err := s.playGame(ctx,
			utils.ToString(data["transaction_id"]),
			utils.ToString(data["shortcode"]),
			utils.ToString(data["name"]),
			betshist,
			utils.ToString(deposit["game_cat_id"]), // Use toString instead of type assertion
			user,
			msisdn,
			(deposit["amount"]).(float64), // Use toFloat64 instead of type assertion
			utils.ToString(deposit["selected_box"]),
			ref,
			"normal",
			utils.ToString(data["description"]),
			utils.ToString(deposit["channel"]),
			utils.ToString(data["ussd"]),      // Use toString instead of type assertion
			utils.ToString(data["game_name"])) // Use toString instead of type assertion

		if err != nil {
			return nil, fmt.Errorf("failed to play game: %w", err)
		}
		return nil, err

		// return map[string]interface{}{
		// 	"Status":        200,
		// 	"StatusCode":    0,
		// 	"StatusMessage": "Success",
		// }, nil
	}

	return nil, fmt.Errorf("deposit settlement failed")
}

// Helper methods
func (s *LuckyNumberService) randomString(length int) string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func (s *LuckyNumberService) getMNOCategory(msisdn string) string {
	return "SAFARICOM" // Simplified for Kenya
}

func (s *LuckyNumberService) hasActiveFreeBet(user map[string]interface{}) bool {
	isFree, ok1 := user["is_free"].(string)
	freeBet, ok2 := user["free_bet"].(float64)
	freebetExpiry, ok3 := user["freebet_expiry"].(string)

	if !ok1 || !ok2 || !ok3 {
		return false
	}

	if isFree != "YES" || freeBet <= 0 {
		return false
	}

	// Check if free bet hasn't expired
	if freebetExpiry != "" {
		expiryTime, err := time.Parse("2006-01-02 15:04:05", freebetExpiry)
		if err == nil && time.Now().Before(expiryTime) {
			return true
		}
	}

	return false
}

func (s *LuckyNumberService) adjustBetAmount(ctx context.Context, msisdn string, amount float64) (float64, error) {
	previousBet, err := s.db.CheckBettoBet(ctx, msisdn)
	if err != nil {
		return amount, err
	}
	if previousBet != nil && len(previousBet) > 0 {
		betRecord := previousBet[0]
		previousAmount, ok := betRecord["amount"].(float64)
		if ok {
			if previousAmount == amount {
				return amount - 1, nil
			} else if previousAmount == (amount - 1) {
				return amount + 1, nil
			}
		}
	}
	return amount, nil
}

// playGame contains the main game logic
func (s *LuckyNumberService) playGame(ctx context.Context, transactionID, shortcode, name string, history interface{}, gameCatID string, player map[string]interface{}, msisdn string, betAmount float64, selectedNumber, reference, betType, description, channel, ussd, gameName string) error {
	// Get settings
	setting, err := s.db.CheckSetting(ctx)
	if err != nil {
		return err
	}

	// Get game info
	game, err := s.db.CheckGamePlay(ctx, gameCatID)
	if err != nil {
		return err
	}

	// Get KPI data
	kpi, err := s.db.CheckSettingKPI(ctx)
	if err != nil {
		return err
	}

	// Get house data
	house, err := s.db.CheckHousePawaBoxKe(ctx)
	if err != nil {
		return err
	}

	// Calculate current RTP
	totalBets := house["total_bets"].(float64) + betAmount
	currentRTP := 0.0
	if totalBets > 0 {
		currentRTP = house["total_wins"].(float64) / totalBets
	}

	defaultRTP := setting["default_rtp"].(float64) + setting["jackpot_percentage"].(float64)
	if currentRTP > defaultRTP {
		currentRTP = defaultRTP
	}

	// Calculate player RTP
	playerTotalBets := player["total_bets"].(float64)
	// playerRTP := 0.0
	// if playerTotalBets > 0 {
	// 	playerRTP = (player["payout"].(float64) / playerTotalBets) * 100
	// }

	// Register player and record bet
	err = s.bet(ctx, reference, player["id"].(int64), playerTotalBets, betAmount)
	if err != nil {
		return err
	}

	// Calculate basket and house values
	globalRTP := setting["default_rtp"].(float64) + setting["adjustmentable_rtp"].(float64)
	basketValue := betAmount * (globalRTP / 100)
	houseValue := (setting["vig_percentage"].(float64) / 100) * betAmount
	jackpotValue := (setting["jackpot_percentage"].(float64) / 100) * betAmount

	// Update jackpot for specific games
	gameInit := game["name_init"].(string)
	if s.isJackpotGame(gameInit) {
		_, err = s.db.UpdateJackpotKitNameInit(ctx, jackpotValue, gameInit)
		if err != nil {
			return err
		}
	}

	// Calculate taxes
	withholdTaxJackpot := (setting["withholding"].(float64) / 100) * jackpotValue
	exciseTaxAmount := (setting["excise_duty"].(float64) / 100) * betAmount
	exciseTaxAmountRound := round(exciseTaxAmount)

	// Handle deposit based on bet type
	var depositTask error
	if betType == "normal" {
		_, depositTask = s.db.UpdateAviatorDepositRequestLucky(ctx, transactionID, reference, description)
	} else {
		_, depositTask = s.db.InsertIntoDepositLuckyRequestBonus(ctx, betType, ussd, gameName, s.getMNOCategory(msisdn), gameCatID, betAmount, msisdn, selectedNumber, reference, channel)
	}

	// Execute all database operations
	tasks := []func() error{
		func() error { return depositTask },
		func() error {
			_, err := s.db.UpdateKPIHandle(ctx, betAmount)
			return err
		},

		func() error {
			_, err := s.db.UpdateKPIPayouts(ctx, jackpotValue, round(withholdTaxJackpot), exciseTaxAmountRound)
			return err
		},
		func() error {
			_, err := s.db.DeleteUserAttempted(ctx, msisdn)
			return err
		},
		func() error {
			_, err := s.db.InsertTaxQueue(ctx, reference, betAmount, exciseTaxAmountRound, betAmount-exciseTaxAmountRound, "excise", msisdn)
			return err
		},
		func() error {
			_, err := s.db.InsertB2BWithdrawalB2B(ctx, reference, msisdn, exciseTaxAmountRound, "Placed")
			return err
		},
		func() error {
			_, err := s.db.CreateDepositRecordLucky(ctx, msisdn, betAmount, transactionID, shortcode, name, reference, betType)
			return err
		},
		func() error {
			_, err := s.db.UpdateJackpotKit(ctx, jackpotValue)
			return err
		},
		func() error {
			_, err := s.db.UpdateUserRTP(ctx, player["id"].(int64))
			return err
		},
		func() error {
			_, err := s.db.CreateBet(ctx, msisdn, selectedNumber, betAmount, "", reference, "Pending", betType, channel)
			return err
		},
		func() error {
			_, err := s.db.UpdateHousePawaBoxKeBets(ctx, betAmount)
			return err
		},
		func() error {
			_, err := s.db.UpdateKPIDeposit(ctx, betAmount)
			return err
		},
		func() error {
			_, err := s.db.InsertHouseLogsPawaBoxKeGameID(ctx, reference, "total_bets", msisdn, betAmount)
			return err
		},
		func() error {
			_, err := s.db.UpdateHouseLucyNumberHouseCurrentRTP(ctx)
			return err
		},
		func() error {
			_, err := s.db.UpdateHousePawaBoxKeHouse(ctx, houseValue)
			return err
		},
		func() error {
			_, err := s.db.UpdateKPIVIG(ctx, houseValue)
			return err
		},
		func() error {
			_, err := s.db.InsertHouseLogsPawaBoxKeGameID(ctx, reference, "house_income", msisdn, houseValue)
			return err
		},
		func() error {
			_, err := s.db.UpdateHousePawaBoxKeBasket(ctx, basketValue)
			return err
		},
		func() error {
			_, err := s.db.InsertHouseBasketLogs(ctx, 0, basketValue, basketValue, fmt.Sprintf("%.2f added to the basket:- game id %s", basketValue, reference))
			return err
		},
		func() error {
			_, err = s.db.InsertCustomerLogsPawaBoxKe(ctx, betAmount, "deposit", utils.ToString(player["id"]), "customer deposit: lucky", reference)
			return err
		},
	}

	for _, task := range tasks {
		if err := task(); err != nil {
			return err
		}
	}

	// Check for jackpot winner
	jackpotWinner, err := s.db.CheckJackpotWinner(ctx)
	if err != nil {
		return err
	}

	// Determine game outcome
	minLossCount := rand.Intn(int(setting["min_loss_count"].(float64))) + 1

	playerFrequency := int64(0)
	if freq, ok := player["frequency"].(int32); ok {
		playerFrequency = int64(freq)
	} else if freq, ok := player["frequency"].(int64); ok {
		playerFrequency = freq
	}

	playerLostCount := int64(0)
	if lost, ok := player["lost_count"].(int32); ok {
		playerLostCount = int64(lost)
	} else if lost, ok := player["lost_count"].(int64); ok {
		playerLostCount = lost
	}

	// Handle jackpot win condition
	if playerFrequency > 10 && playerLostCount > int64(minLossCount) && jackpotWinner != nil {
		return s.handleJackpotWin(ctx, player, msisdn, betAmount, selectedNumber, reference, setting, game, kpi, jackpotWinner)
	} else {
		return s.handleNormalGame(ctx, player, msisdn, betAmount, selectedNumber, reference, setting, game, kpi, minLossCount)
	}
}

// bet records a bet for a player
func (s *LuckyNumberService) bet(ctx context.Context, reference string, playerID int64, totalBets, amount float64) error {
	_, err := s.db.UpdateUserBet(ctx, amount, playerID)
	if err != nil {
		return err
	}
	_, err = s.db.InsertCustomerLogsPawaBoxKe(ctx, amount, "bet", utils.ToString(playerID), "customer placed bet", reference)
	if err != nil {
		return err
	}

	return nil
}

// win records a win for a player
func (s *LuckyNumberService) win(ctx context.Context, playerID int64, payout, bets float64, winItem string, withholdTax, taxDeductedAmount, amount float64, msisdn, reference string) error {
	amountNew := round(amount)
	withholdTaxNew := round(withholdTax)
	taxDeductedAmountNew := round(taxDeductedAmount)

	// Insert into withdrawals
	_, err := s.db.InsertIntoWithdrawalsLucky(ctx, amount, taxDeductedAmountNew, withholdTaxNew, winItem, msisdn, reference)
	if err != nil {
		return err
	}

	// Check settings
	setting, err := s.db.CheckSetting(ctx)
	if err != nil {
		return err
	}

	if setting != nil {
		checkWithdrawal, err := s.db.CheckWithdrawalsPawaBoxKe(ctx, reference)
		if err != nil {
			return err
		}

		if checkWithdrawal != nil && checkWithdrawal["msisdn"] != nil {
			// Insert tax queue
			_, err := s.db.InsertTaxQueue(ctx, reference, amount, withholdTax, taxDeductedAmount, "withholding", msisdn)
			if err != nil {
				return err
			}

			// Insert B2B withdrawal
			_, err = s.db.InsertB2BWithdrawalB2B(ctx, reference, msisdn, taxDeductedAmountNew, "Won")
			if err != nil {
				return err
			}

			// Handle different withdrawal amounts
			var withdrawalTask error
			if amountNew >= 60000 {
				_, withdrawalTask = s.db.InsertIntoPendingWithdrawalsLucky(ctx, taxDeductedAmountNew, withholdTaxNew, winItem, msisdn, reference)
			} else {
				_, withdrawalTask = s.db.InsertWithdrawalQueue(ctx, reference, msisdn, taxDeductedAmountNew, "http?")
			}

			if withdrawalTask != nil {
				return withdrawalTask
			}

			// Update various records
			tasks := []func() error{
				func() error {
					_, err := s.db.UpdateRESTLossUser(ctx, amountNew, playerID)
					return err
				},
				func() error {
					_, err := s.db.InsertCustomerLogsPawaBoxKe(ctx, amountNew, "withdraw", utils.ToString(playerID), "customer withdrawal: luckynumber", reference)
					return err
				},
				func() error {
					_, err := s.db.UpdateHouseLuckyWins(ctx, amountNew)
					return err
				},
				func() error {
					_, err := s.db.UpdateHouseLuckyBasketWins(ctx, amountNew)
					return err
				},
				func() error {
					_, err := s.db.InsertHouseBasketLogs(ctx, amountNew, 0, -amountNew, fmt.Sprintf("%.2f deducted from the basket:- game id %s", amountNew, reference))
					return err
				},
				func() error {
					_, err := s.db.InsertHouseLogsPawaBoxKeGameID(ctx, reference, "total_wins", msisdn, amountNew)
					return err
				},
				func() error {
					_, err := s.db.UpdatePawaBoxKeWithdrawalRequest(ctx, reference)
					return err
				},
			}

			for _, task := range tasks {
				if err := task(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// lose records a loss for a player
func (s *LuckyNumberService) lose(ctx context.Context, playerID int64, reference string, msisdn string, lostCount int64, totalLosses, amount float64) error {
	tasks := []func() error{
		func() error {
			_, err := s.db.UpdateUserLossCount(ctx, amount, playerID)
			return err
		},
		func() error {
			_, err := s.db.InsertCustomerLogsPawaBoxKe(ctx, amount, "lost", utils.ToString(playerID), fmt.Sprintf("customer lost %.2f", amount), reference)
			return err
		},
		func() error {
			_, err := s.db.UpdateHouseLuckyHouseLosses(ctx, amount)
			return err
		},
		func() error {
			_, err := s.db.InsertHouseLogsPawaBoxKeGameID(ctx, reference, "total_losses", msisdn, amount)
			return err
		},
		func() error {
			_, err := s.db.InsertB2BWithdrawalB2B(ctx, reference, msisdn, 0, "Lost")
			return err
		},
	}

	for _, task := range tasks {
		if err := task(); err != nil {
			return err
		}
	}

	return nil
}

// Helper functions
func (s *LuckyNumberService) isJackpotGame(gameInit string) bool {
	jackpotGames := []string{"pawa_supa", "pawa_jackpot", "mega_jackpot", "pawa_demio"}
	for _, game := range jackpotGames {
		if game == gameInit {
			return true
		}
	}
	return false
}

func (s *LuckyNumberService) handleJackpotWin(ctx context.Context, player map[string]interface{}, msisdn string, betAmount float64, selectedNumber, reference string, setting, game, kpi, jackpotWinner map[string]interface{}) error {
	// Implementation for jackpot win handling
	// This would include the complex jackpot win logic from Python
	return nil
}

func (s *LuckyNumberService) handleNormalGame(ctx context.Context, player map[string]interface{}, msisdn string, betAmount float64, selectedNumber, reference string, setting, game, kpi map[string]interface{}, minLossCount int) error {
	// Convert types safely
	playerID := utils.ToInt64(player["id"])
	playerLostCount := utils.ToInt64(player["lost_count"])
	playerFreeBet := utils.ToInt64(player["free_bet"])
	playerPayout := utils.ToFloat64(player["payout"])
	playerTotalBets := utils.ToFloat64(player["total_bets"])
	playerTotalLosses := utils.ToFloat64(player["total_losses"])
	defaultRTP := utils.ToFloat64(setting["default"])
	adjustmentableRTP := utils.ToFloat64(setting["adjustmentable_rtp"])
	minWinMultiplier := utils.ToFloat64(setting["min_win_multipier"])
	maxWinMultiplier := utils.ToFloat64(setting["max_win_multipier"])
	vigPercentage := utils.ToFloat64(setting["vig_percentage"])
	rtpOverload := utils.ToFloat64(setting["rtp_overload"])
	withholding := utils.ToFloat64(setting["withholding"])
	maxWon := utils.ToFloat64(setting["max_won"])

	gameMaxExposure := utils.ToFloat64(game["max_exposure"])
	gameNameInit := utils.ToString(game["name_init"])

	kpiPayout := utils.ToFloat64(kpi["payout"])
	kpiBet := utils.ToFloat64(kpi["bet"])
	kpiRTP := utils.ToFloat64(kpi["rtp"])

	// Generate win amounts
	winAmounts, err := s.GenerateWinAmounts(ctx, GenerateWinAmountsParams{
		Msisdn:           msisdn,
		KPI:              kpi,
		DefaultRTP:       defaultRTP,
		AdjustmentRTP:    adjustmentableRTP,
		PlayerRTP:        utils.ToFloat64(player["rtp"]),
		Reference:        reference,
		BetAmount:        betAmount,
		SelectedNumber:   selectedNumber,
		PlayerID:         playerID,
		MinWinMultiplier: minWinMultiplier,
		MaxWinMultiplier: maxWinMultiplier,
		MaxExposure:      gameMaxExposure,
		GameNameInit:     gameNameInit,
		PlayerLostCount:  playerLostCount,
		MinLossCount:     minLossCount,
		MaxWon:           maxWon,
		VigPercentage:    vigPercentage,
		RTPOverload:      rtpOverload,
	})
	if err != nil {
		return fmt.Errorf("failed to generate win amounts: %w", err)
	}

	logrus.Infof("Win amounts generated: %+v", winAmounts)

	// 🔥 CRITICAL SAFETY CHECKS - Add these lines
	if winAmounts == nil {
		return fmt.Errorf("winAmounts is nil after generation")
	}

	winAmount, exists := winAmounts[selectedNumber]
	if !exists {
		logrus.Errorf("Selected number %s not found in winAmounts: %v", selectedNumber, winAmounts)
		return fmt.Errorf("selected number %s not found in win amounts", selectedNumber)
	}

	// Random increment calculation
	randomIncrement := rand.Float64() * 10 // Random between 0-10
	increment := (defaultRTP / 100) * randomIncrement

	// Get current RTP and adjust if needed - add safety check
	var currentRTP float64
	if s.playersData != nil {
		if playerData, exists := s.playersData[playerID]; exists {
			currentRTP = playerData.CurrentRTP
			if currentRTP > defaultRTP {
				currentRTP = defaultRTP + increment
			}
		}
	}

	logrus.Infof("Min loss count: %d", minLossCount)
	logrus.Infof("Win amounts: %+v", winAmounts)

	// 🔥 Use the safely accessed winAmount instead of direct map access
	winAmountValue := winAmount.Value
	winItem := winAmount.Item

	logrus.Infof("Win amount: %.2f", winAmountValue)
	logrus.Infof("Max won: %.2f", maxWon)
	logrus.Infof("Default RTP: %.2f", defaultRTP)
	logrus.Infof("Player RTP: %.2f", utils.ToFloat64(player["rtp"]))

	// Calculate current RTP for the day - add division by zero check
	var currentRTPDay float64
	if kpiBet > 0 {
		currentRTPDay = ((kpiPayout + winAmountValue) / kpiBet) * 100
	} else {
		currentRTPDay = 0
		logrus.Warn("kpiBet is zero, cannot calculate RTP")
	}

	logrus.Infof("Default RTP: %.2f", defaultRTP)
	logrus.Infof("Player RTP: %.2f", utils.ToFloat64(player["rtp"]))
	logrus.Infof("Global RTP: %.2f", utils.ToFloat64(player["rtp"])) // Assuming rtp_player is same
	logrus.Infof("Current RTP: %.2f", kpiRTP)
	logrus.Infof("Current RTP Day: %.2f", currentRTPDay)
	logrus.Infof("Player lost count: %d", playerLostCount)
	logrus.Infof("Basket value: %.2f", utils.ToFloat64(kpi["basket_value"]))
	logrus.Infof("Win amount: %.2f", winAmountValue)

	basketValue := utils.ToFloat64(kpi["basket_value"])

	// Win condition
	if winAmountValue > 0 && (defaultRTP+adjustmentableRTP) >= math.Round(currentRTPDay*100)/100 && basketValue > winAmountValue {
		// Player wins
		resultMessage := fmt.Sprintf("Box %s wins. Numbers: %+v", selectedNumber, winAmounts)
		logrus.Info(resultMessage)

		// Update bet as win
		_, err := s.db.UpdateLuckyBetWin(ctx, resultMessage, reference, winAmountValue, "Win")
		if err != nil {
			return fmt.Errorf("failed to update lucky bet win: %w", err)
		}

		// Calculate tax
		withholdTax := (withholding / 100) * winAmountValue
		taxDeductedAmount := winAmountValue - withholdTax

		// Update KPI payouts
		_, err = s.db.UpdateKPIPayouts(ctx, winAmountValue, withholdTax, 0)
		if err != nil {
			return fmt.Errorf("failed to update KPI payouts: %w", err)
		}

		// Update win amounts with tax deducted values - SAFELY
		winAmounts[selectedNumber] = WinAmount{
			Value: taxDeductedAmount,
			Item:  FormatToMZN(taxDeductedAmount),
		}

		// Handle win logic
		err = s.win(ctx, playerID, playerPayout, playerTotalBets, winItem, withholdTax, taxDeductedAmount, winAmountValue, msisdn, reference)
		if err != nil {
			return fmt.Errorf("failed to handle win: %w", err)
		}

		// Round amounts
		withholdTax = math.Round(withholdTax)
		taxDeductedAmount = math.Round(taxDeductedAmount)

		// Create win message
		message := s.createWinMessage(selectedNumber, winAmounts, playerFreeBet, reference, withholding, withholdTax)
		logrus.Infof("Player MSISDN: %s", msisdn)

		// Queue SMS
		senderID := "Funua Pesa"
		_, err = s.db.InsertIntoSMSQueue(ctx, msisdn, message, senderID, "game_response")
		if err != nil {
			return fmt.Errorf("failed to insert SMS queue: %w", err)
		}

		// Update RTP
		_, err = s.db.UpdateHouseLucyNumberHouseCurrentRTP(ctx)
		if err != nil {
			return fmt.Errorf("failed to update RTP: %w", err)
		}

		logrus.Infof("Player %s won: %.2f (tax: %.2f)", msisdn, taxDeductedAmount, withholdTax)
		return nil

	} else {
		// Player loses - SAFELY update
		winAmounts[selectedNumber] = WinAmount{
			Value: 0,
			Item:  "0",
		}

		// Handle loss
		err := s.lose(ctx, playerID, reference, msisdn, playerLostCount, playerTotalLosses, betAmount)
		if err != nil {
			return fmt.Errorf("failed to handle loss: %w", err)
		}

		// Build loss message
		resultMessage := fmt.Sprintf("Box %s loses. Numbers: (%+v)", selectedNumber, winAmounts)
		logrus.Info(resultMessage)

		message := s.createLossMessage(selectedNumber, winAmounts, playerFreeBet, reference)
		logrus.Infof("Player MSISDN: %s", msisdn)

		// Queue SMS
		senderID := "Funua Pesa"
		_, err = s.db.InsertIntoSMSQueue(ctx, msisdn, message, senderID, "game_response")
		if err != nil {
			return fmt.Errorf("failed to insert SMS queue: %w", err)
		}

		// Update bet as loss
		_, err = s.db.UpdateLuckyBet(ctx, resultMessage, reference, "Lose")
		if err != nil {
			return fmt.Errorf("failed to update lucky bet: %w", err)
		}

		// Record lost transaction
		_, err = s.db.InsertB2BWithdrawalB2B(ctx, reference, msisdn, 0, "Lost")
		if err != nil {
			return fmt.Errorf("failed to insert B2B withdrawal: %w", err)
		}

		logrus.Infof("Player %s lost bet: %.2f", msisdn, betAmount)
		return nil
	}
}

// GenerateWinAmounts generates unique win amounts for each box number
func (s *LuckyNumberService) GenerateWinAmounts(ctx context.Context, params GenerateWinAmountsParams) (map[string]WinAmount, error) {
	// Initialize random
	rand.Seed(time.Now().UnixNano())

	// Generate 7 unique random numbers between 1-7
	chosenNumbers := generateUniqueNumbers(1, 8, 7)
	numZeroBoxes := rand.Intn(3) + 1 // 1-3
	rand.Intn(2)                     // 0-1

	boxes := make(map[string]WinAmount)
	totalAssigned := 0.0

	// Get basket value
	basket, err := s.db.CheckBasketLucky(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check basket: %w", err)
	}
	basketValue := utils.ToFloat64(basket["amount"])

	logrus.Infof("Max won: %.2f", params.MaxWon)
	maxWinAmount := params.MaxWon

	// Calculate min and max win amounts
	minWinAmount := params.BetAmount * params.MinWinMultiplier
	maxWinAmountCalc := math.Min(params.BetAmount*params.MaxWinMultiplier, params.MaxExposure)

	newBasketValue := basketValue * 0.80 // max win in basket

	if newBasketValue > minWinAmount {
		maxWinAmountCalc = math.Min(newBasketValue, params.MaxExposure)
	}

	winAward := ""

	// Select random boxes for awards
	numSelectedBoxes := rand.Intn(3) // 0-2
	selectedBoxes := selectRandomBoxes(chosenNumbers, numSelectedBoxes)

	logrus.Infof("Selected boxes: %v", selectedBoxes)

	// Step 1: Create boxes for each chosen number
	for _, num := range chosenNumbers {
		numStr := fmt.Sprintf("%d", num)
		var winAmount float64

		if rand.Float64() < 0.5 {
			// 50% chance for smaller wins
			winAmount = rand.Float64()*(minWinAmount*20-minWinAmount) + minWinAmount
		} else {
			// 50% chance for larger wins
			winAmount = rand.Float64()*(maxWinAmountCalc-minWinAmount) + minWinAmount
		}

		// Check for awards
		awards, err := s.db.CheckAwardsLucky(ctx, winAmount, params.GameNameInit)
		if err == nil && awards != nil && contains(selectedBoxes, num) {
			winAward = utils.ToString(awards["name"])
		} else {
			winAward = FormatToMZN(winAmount)
		}

		// Handle special win conditions
		if params.PlayerLostCount >= int64(params.MinLossCount) && maxWinAmount >= minWinAmount && numStr == params.SelectedNumber {
			logrus.Infof("Player count: %d, Max loss count: %d", params.PlayerLostCount, params.MinLossCount)
			logrus.Infof("Min win amount: %.2f, Max won: %.2f", minWinAmount, params.MaxWon)
			logrus.Infof("Selected number: %s, Current num: %d", params.SelectedNumber, num)

			var specialWinAmount float64
			if rand.Float64() < 0.5 {
				specialWinAmount = rand.Float64()*(minWinAmount*20-minWinAmount) + minWinAmount
				if rand.Float64() < 0.5 {
					specialWinAmount = rand.Float64()*(800-minWinAmount) + minWinAmount
				}
			} else {
				specialWinAmount = rand.Float64()*(800-minWinAmount) + minWinAmount
			}

			if specialWinAmount > params.MaxWon {
				specialWinAmount = params.MaxWon
			}

			item := winAward
			if !contains(selectedBoxes, num) {
				item = FormatToMZN(specialWinAmount)
			}

			boxes[numStr] = WinAmount{
				Value: specialWinAmount,
				Item:  item,
			}
		} else {
			item := winAward
			if !contains(selectedBoxes, num) {
				item = FormatToMZN(winAmount)
			}

			boxes[numStr] = WinAmount{
				Value: winAmount,
				Item:  item,
			}
		}

		totalAssigned += winAmount
	}

	// Set zero boxes
	if len(chosenNumbers) > 0 {
		candidateBoxes := make([]int, 0)
		for _, num := range chosenNumbers {
			if fmt.Sprintf("%d", num) != params.SelectedNumber {
				candidateBoxes = append(candidateBoxes, num)
			}
		}

		// Set some boxes to zero
		zeroBoxes := selectRandomBoxes(candidateBoxes, numZeroBoxes)
		for _, zeroBox := range zeroBoxes {
			boxes[fmt.Sprintf("%d", zeroBox)] = WinAmount{
				Value: 0,
				Item:  "0",
			}
		}

		// Set special award box
		awardsWin, err := s.db.CheckAwardsLuckyRandom(ctx, params.GameNameInit)
		if err == nil && awardsWin != nil {
			zeroBox := selectRandomBox(candidateBoxes)
			boxes[fmt.Sprintf("%d", zeroBox)] = WinAmount{
				Value: utils.ToFloat64(awardsWin["value"]),
				Item:  utils.ToString(awardsWin["name"]),
			}

			// Remove used box from candidates
			candidateBoxes = removeElement(candidateBoxes, zeroBox)
		}

		// Set max exposure box
		if len(candidateBoxes) > 0 {
			exposureBox := selectRandomBox(candidateBoxes)
			boxes[fmt.Sprintf("%d", exposureBox)] = WinAmount{
				Value: params.MaxExposure,
				Item:  FormatToMZN(params.MaxExposure),
			}

			// Remove used box from candidates
			candidateBoxes = removeElement(candidateBoxes, exposureBox)
		}

		// Set random min amount box
		if len(candidateBoxes) > 0 {
			randomMinAmount := rand.Float64()*(minWinAmount*1.2-minWinAmount) + minWinAmount
			exposureMinBox := selectRandomBox(candidateBoxes)
			boxes[fmt.Sprintf("%d", exposureMinBox)] = WinAmount{
				Value: randomMinAmount,
				Item:  FormatToMZN(randomMinAmount),
			}
		}
	}

	logrus.Infof("Player lost count: %d", params.PlayerLostCount)
	logrus.Infof("Max loss count: %d", params.MinLossCount)

	// Force win logic
	forceWin := params.PlayerLostCount >= int64(params.MinLossCount+10)

	if forceWin {
		return s.handleForceWin(ctx, boxes, params, basketValue, minWinAmount, maxWinAmountCalc)
	}

	// Check if selected box has a win
	if winAmount, exists := boxes[params.SelectedNumber]; exists && winAmount.Value > 0 {
		return s.handlePotentialWin(ctx, boxes, params, basketValue, minWinAmount, maxWinAmountCalc)
	}

	return boxes, nil
}

// handleForceWin handles forced win logic
func (s *LuckyNumberService) handleForceWin(ctx context.Context, boxes map[string]WinAmount, params GenerateWinAmountsParams, basketValue, minWinAmount, maxWinAmount float64) (map[string]WinAmount, error) {
	logrus.Info("Player reached loss limit, forcing a win using adjustable_rtp")

	// Determine target RTP
	targetRTP := params.DefaultRTP + params.AdjustmentRTP

	// Compute safe win range
	baseMultiplier := params.AdjustmentRTP / 100
	potentialWin := utils.ToFloat64(params.KPI["bet"]) * baseMultiplier

	// Compute max allowed payout
	maxAllowedPayout := (targetRTP/100)*utils.ToFloat64(params.KPI["bet"]) - utils.ToFloat64(params.KPI["payout"])

	logrus.Infof("[FORCE-WIN DEBUG] target_rtp=%.2f, adjustable_rtp=%.2f, bet=%.2f, payout=%.2f",
		targetRTP, params.AdjustmentRTP, utils.ToFloat64(params.KPI["bet"]), utils.ToFloat64(params.KPI["payout"]))
	logrus.Infof("base_multiplier=%.4f, potential_win=%.2f, max_allowed_payout=%.2f",
		baseMultiplier, potentialWin, maxAllowedPayout)

	// Derive forced amount
	forcedAmount := math.Min(math.Max(potentialWin, minWinAmount), maxWinAmount)
	forcedAmount = math.Min(forcedAmount, maxAllowedPayout)

	// Add random variation
	forcedAmount *= rand.Float64()*0.2 + 0.9 // ±10%
	forcedAmount = math.Min(math.Max(forcedAmount, minWinAmount), maxWinAmount)

	// Recalculate RTP
	kpiBet := utils.ToFloat64(params.KPI["bet"])
	var currentRTPDay float64
	if kpiBet != 0 {
		currentRTPDay = ((utils.ToFloat64(params.KPI["payout"]) + forcedAmount) / kpiBet) * 100
	}

	logrus.Infof("[FORCE-WIN RTP CHECK] target_rtp=%.2f, current_rtp_day=%.2f, forced_amount=%.2f",
		targetRTP, currentRTPDay, forcedAmount)

	// Adjust if RTP exceeds target
	if currentRTPDay > targetRTP {
		reducedTargetRTP := math.Max(targetRTP-2, 0)
		logrus.Infof("[FORCE-WIN ADJUSTMENT] RTP above target, reducing to %.2f", reducedTargetRTP)

		for i := 0; i < 10; i++ {
			if kpiBet == 0 {
				break
			}

			currentRTPDay = ((utils.ToFloat64(params.KPI["payout"]) + forcedAmount) / kpiBet) * 100
			if currentRTPDay <= reducedTargetRTP+0.1 {
				break
			}

			forcedAmount -= forcedAmount * 0.05 // reduce by 5% each step
		}

		forcedAmount = math.Min(math.Max(forcedAmount, maxAllowedPayout), maxWinAmount)
	}

	// Check basket coverage
	if forcedAmount > basketValue || forcedAmount < 1 {
		boxes[params.SelectedNumber] = WinAmount{Value: 0, Item: "0"}
		return boxes, nil
	}

	// Assign final forced win
	amount := math.Round(forcedAmount*100) / 100
	boxes[params.SelectedNumber] = WinAmount{
		Value: amount,
		Item:  FormatToMZN(amount),
	}

	logrus.Infof("[FORCE-WIN COMPLETE] Forced win=%.2f, adjustable_rtp=%.2f, target_rtp=%.2f, basket=%.2f",
		amount, params.AdjustmentRTP, targetRTP, basketValue)

	return boxes, nil
}

// handlePotentialWin handles potential win logic with RTP checks
func (s *LuckyNumberService) handlePotentialWin(ctx context.Context, boxes map[string]WinAmount, params GenerateWinAmountsParams, basketValue, minWinAmount, maxWinAmount float64) (map[string]WinAmount, error) {
	// Get player data
	player, err := s.db.CheckUser(ctx, params.Msisdn)
	if err != nil {
		return nil, fmt.Errorf("failed to check user: %w", err)
	}

	// mxWin := utils.ToFloat64(player["total_bets"]) + params.BetAmount - utils.ToFloat64(player["payout"])
	// maxWonCalc := (params.DefaultRTP / 100) * mxWin

	amount := boxes[params.SelectedNumber].Value

	// Calculate RTPs
	playerRTP := ((utils.ToFloat64(player["payout"]) + amount) / utils.ToFloat64(player["total_bets"])) * 100

	kpiBet := utils.ToFloat64(params.KPI["bet"])
	var currentRTPDay float64
	if kpiBet != 0 {
		currentRTPDay = ((utils.ToFloat64(params.KPI["payout"]) + amount) / kpiBet) * 100
	}

	logrus.Infof("RTP before: %.2f", currentRTPDay)
	logrus.Infof("Amount before: %.2f", amount)

	// RTP adjustment logic
	if params.PlayerLostCount >= int64(params.MinLossCount) && currentRTPDay > params.DefaultRTP {
		if kpiBet != 0 {
			margin := rand.Float64()*0.8 + 0.1 // 0.1-0.9%
			targetRTP := (params.DefaultRTP + params.AdjustmentRTP) - margin
			maxAllowedPayout := (targetRTP/100)*kpiBet - utils.ToFloat64(params.KPI["payout"])

			if maxAllowedPayout > minWinAmount {
				amount = rand.Float64()*(maxAllowedPayout-minWinAmount) + minWinAmount
			} else {
				randomPercentage := rand.Float64()*0.39 + 0.6 // 0.6-0.99
				minRandom := params.BetAmount + ((minWinAmount - params.BetAmount) * randomPercentage)
				amount = rand.Float64()*(minWinAmount-minRandom) + minRandom
			}

			amount = math.Round(amount*100) / 100
			currentRTPDay = ((utils.ToFloat64(params.KPI["payout"]) + amount) / kpiBet) * 100
		} else {
			amount = minWinAmount
		}

		logrus.Infof("RTP after: %.2f", currentRTPDay)
		logrus.Infof("Amount after: %.2f", amount)
		logrus.Infof("Min win amount: %.2f", minWinAmount)
	}

	// Various win condition checks
	if amount > basketValue ||
		minWinAmount > amount ||
		(currentRTPDay > (params.DefaultRTP+params.AdjustmentRTP) && params.PlayerLostCount >= int64(params.MinLossCount)) ||
		(utils.ToFloat64(params.KPI["rtp"]) > (params.DefaultRTP+params.AdjustmentRTP) && params.PlayerLostCount >= int64(params.MinLossCount)) ||
		(currentRTPDay > params.DefaultRTP && int64(params.MinLossCount) > params.PlayerLostCount) ||
		(utils.ToFloat64(params.KPI["rtp"]) > params.DefaultRTP && int64(params.MinLossCount) > params.PlayerLostCount) ||
		(playerRTP > (params.AdjustmentRTP + params.DefaultRTP + params.VigPercentage + params.RTPOverload)) {

		boxes[params.SelectedNumber] = WinAmount{Value: 0, Item: "0"}
		return boxes, nil
	}

	// Final win assignment
	boxes[params.SelectedNumber] = WinAmount{
		Value: amount,
		Item:  FormatToMZN(amount),
	}
	return boxes, nil
}

// Helper functions
func generateUniqueNumbers(min, max, count int) []int {
	numbers := make([]int, max-min)
	for i := range numbers {
		numbers[i] = min + i
	}
	rand.Shuffle(len(numbers), func(i, j int) {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	})
	return numbers[:count]
}

func selectRandomBoxes(numbers []int, count int) []int {
	if count >= len(numbers) {
		return numbers
	}
	rand.Shuffle(len(numbers), func(i, j int) {
		numbers[i], numbers[j] = numbers[j], numbers[i]
	})
	return numbers[:count]
}

func selectRandomBox(numbers []int) int {
	return numbers[rand.Intn(len(numbers))]
}

func contains(slice []int, item int) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func removeElement(slice []int, element int) []int {
	for i, v := range slice {
		if v == element {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// FormatToMZN formats amount as MZN currency

func FormatToMZN(n float64) string {
	s := strconv.FormatFloat(n, 'f', 2, 64) // keep 2 decimal places
	parts := strings.Split(s, ".")
	intPart := parts[0]

	length := len(intPart)
	b := make([]byte, 0, length+length/3)

	for i, c := range intPart {
		if i > 0 && (length-i)%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, byte(c))
	}

	if len(parts) > 1 {
		b = append(b, '.')
		b = append(b, parts[1]...)
	}

	return string(b)
}

// Helper methods
func (s *LuckyNumberService) createWinMessage(selectedNumber string, winAmounts map[string]WinAmount, freeBet int64, reference string, withholding, withholdTax float64) string {
	var boxes []string
	for num, winAmount := range winAmounts {
		boxes = append(boxes, fmt.Sprintf("Box %s - %s", num, winAmount.Item))
	}
	sort.Strings(boxes)

	return fmt.Sprintf(utils.Texts["results"]["win"],
		selectedNumber,
		winAmounts[selectedNumber].Item,
		strings.Join(boxes, ", "),
		freeBet,
		reference,
		int(withholding),
		FormatToMZN(withholdTax),
	)
}

func (s *LuckyNumberService) createLossMessage(selectedNumber string, winAmounts map[string]WinAmount, freeBet int64, reference string) string {
	var boxes []string
	for num, winAmount := range winAmounts {
		// Format the item properly if it's a number
		itemDisplay := winAmount.Item
		log.Println("cddddddd ddd d %s", winAmount.Item)

		log.Println("cddddddd valu d %s", winAmount.Value)
		// If Item is empty or not properly formatted, use the Value
		if itemDisplay == "" || itemDisplay == "0" {
			itemDisplay = winAmount.Item
		}

		boxes = append(boxes, fmt.Sprintf("Box %s - %s", num, itemDisplay))
	}
	sort.Strings(boxes)

	return fmt.Sprintf(utils.Texts["results"]["loss"],
		selectedNumber,
		strings.Join(boxes, "\n"), // Use \n for better formatting
		freeBet,
		reference,
	)
}

// Update methods for various operations
func (s *LuckyNumberService) UpdateAviatorDepositFailRequestLucky(ref string, desc string) error {
	_, err := s.db.UpdateAviatorDepositFailRequestLucky(context.Background(), ref, desc)
	return err
}
func (s *LuckyNumberService) UpdateLuckyNumberWithdrawalDisburse(txid, status, desc, ref string) (bool, error) {
	return s.db.UpdatePawaBoxKeWithdrawalDisburse(context.Background(), txid, status, desc, ref)
}

func (s *LuckyNumberService) UpdateLuckyNumberWithdrawalDisburseMotto(txid, status, desc, ref string) (bool, error) {
	return s.db.UpdatePawaBoxKeWithdrawalDisburseMotto(context.Background(), txid, status, desc, ref)
}

func (s *LuckyNumberService) UpdatePawaBox_KeWithdrawalb2bDisburse(txid, status, desc, ref string) (bool, error) {
	return s.db.UpdatePawaBoxKeWithdrawalB2BDisburse(context.Background(), txid, status, desc, ref)
}

func (s *LuckyNumberService) InsertFailedSMS(ref string) error {
	ctx := context.Background()

	// Check deposit request
	stkUSSD, err := s.db.CheckDepositRequestLuckyFailed(ctx, ref)
	if err != nil {
		return fmt.Errorf("failed to check deposit request: %w", err)
	}

	if stkUSSD == nil || stkUSSD["msisdn"] == nil {
		log.Printf("No deposit request found or no MSISDN for reference: %s", ref)
		return nil
	}

	msisdn, ok := stkUSSD["msisdn"].(string)
	if !ok {
		return fmt.Errorf("invalid msisdn type for reference: %s", ref)
	}

	message := s.texts["results"]["cancelled"]
	senderID := "LuckyNumber"

	// Insert into SMS queue and ignore the returned ID
	_, err = s.db.InsertIntoSMSQueue(ctx, msisdn, message, senderID, "game_response")
	if err != nil {
		return fmt.Errorf("failed to insert failed SMS: %w", err)
	}

	log.Printf("Failed SMS queued for %s with reference: %s", msisdn, ref)
	return nil
}

// Utility function
func round(value float64) float64 {
	return float64(int(value + 0.5))
}
