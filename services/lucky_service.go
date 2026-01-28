package services

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fiberapp/database"
	"fiberapp/utils"
	"fmt"
	"log"
	"math"
	mathrand "math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// PRNG pool for high concurrency
var prngPool *sync.Pool

func init() {
	fastRng.Seed(time.Now().UnixNano())

	prngPool = &sync.Pool{
		New: func() any {
			// Seed each PRNG with crypto
			var seedBytes [8]byte
			_, err := rand.Read(seedBytes[:])
			if err != nil {
				seed := int64(binary.LittleEndian.Uint64(seedBytes[:]))
				return mathrand.New(mathrand.NewSource(seed))
			}
			seed := int64(binary.LittleEndian.Uint64(seedBytes[:]))
			return mathrand.New(mathrand.NewSource(seed))
		},
	}
}

// get a PRNG from the pool
func getRand() *mathrand.Rand {
	return prngPool.Get().(*mathrand.Rand)
}

// put back the PRNG into the pool
func putRand(r *mathrand.Rand) {
	prngPool.Put(r)
}

// ---------------- Random primitives ----------------

func cryptoRandInt(min, max int) int {
	if max <= min {
		return min
	}
	r := getRand()
	n := r.Intn(max - min)
	putRand(r)
	return n + min
}

func cryptoRandFloat() float64 {
	r := getRand()
	f := r.Float64()
	putRand(r)
	return f
}

func cryptoRandFloatRange(min, max float64) float64 {
	return min + cryptoRandFloat()*(max-min)
}

func cryptoRandIndex(length int) int {
	if length <= 0 {
		return 0
	}
	r := getRand()
	idx := r.Intn(length)
	putRand(r)
	return idx
}

// ---------------- Sampling / shuffling ----------------

func cryptoRandSample(arr []int, k int) []int {
	if k > len(arr) {
		k = len(arr)
	}

	tmp := append([]int{}, arr...)
	out := make([]int, 0, k)

	for i := 0; i < k; i++ {
		idx := cryptoRandIndex(len(tmp))
		out = append(out, tmp[idx])
		tmp = append(tmp[:idx], tmp[idx+1:]...)
	}

	return out
}

func cryptoRandUniqueInts(min, max, count int) []int {
	arr := make([]int, max-min)
	for i := range arr {
		arr[i] = min + i
	}

	out := make([]int, 0, count)
	for len(out) < count && len(arr) > 0 {
		idx := cryptoRandIndex(len(arr))
		out = append(out, arr[idx])
		arr = append(arr[:idx], arr[idx+1:]...)
	}
	return out
}

func CryptoShuffle[T any](numbers []T) {
	n := len(numbers)
	for i := n - 1; i > 0; i-- {
		j := cryptoRandIndex(i + 1)
		numbers[i], numbers[j] = numbers[j], numbers[i]
	}
}

// LuckyNumberService handles the lucky number game logic
type LuckyNumberService struct {
	mu          sync.Mutex
	db          *database.Database // Your database client
	playersData map[int64]*PlayerData
	texts       map[string]map[string]string // SMS templates
}

type Bet struct {
	ID             int64                 `json:"id"`
	MSISDN         string                `json:"msisdn"`
	Amount         float64               `json:"amount"`
	ResultStatus   string                `json:"result_status"`
	Game           string                `json:"game"`
	Channel        string                `json:"channel"`
	SelectedNumber string                `json:"selected_number"`
	Results        map[int]BetResultItem `json:"results"`
	DateCreated    time.Time             `json:"date_created"`
	Status         string                `json:"status"`
	Reference      string                `json:"reference"`
	Narrative      string                `json:"narrative"`
	WinAmount      float64               `json:"win_amount"`
	BetType        string                `json:"bet_type"`
	LastUpdatedOn  time.Time             `json:"last_updated_on"`
}

// Nested struct for results map
type BetResultItem struct {
	Value float64 `json:"value"`
	Item  string  `json:"item"` // store as string to handle both numbers and text (like "Smart TV")
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
	GameResult PlaceBetResultDisplay `json:"GameResult"` // JSON string
	FreeBet    string                `json:"FreeBet"`
	Message    string                `json:"Message"`
}

type SpinResponse struct {
	Row       []string `json:"row"`
	Win       bool     `json:"win"`
	WinAmount float64  `json:"win_amount"`
	GameID    string   `json:"game_id"`
}

type PlaceBetResultDisplay struct {
	Boxes         map[string]WinAmount `json:"Boxes"` // JSON string
	ResultStatus  string               `json:"ResultStatus"`
	WinAmount     float64              `json:"WinAmount"`
	JackPot       string               `json:"JackPot"`
	GameID        string               `json:"GameID"`
	SelectedBox   string               `json:"SelectedBox"`
	ResultMessage string               `json:"ResultMessage"`
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
func (s *LuckyNumberService) CheckGame(category string) (interface{}, error) {
	ctx := context.Background()

	return s.db.CheckGames(ctx, category)
}

// VerifyOTP verifies an OTP and returns remaining seconds until expiry (ExpireIn).
// Returns (0, error) on invalid/expired OTP or other errors.
func (s *LuckyNumberService) VerifyOTP(msisdn, otp string) (int64, error) {
	if s == nil || s.db == nil {
		log.Printf("PANIC PREVENTION: s=%p, s.db=%p", s, s.db)
		return 0, fmt.Errorf("service or database not initialized")
	}

	ctx := context.Background()
	now := time.Now().Unix() // seconds

	// Step 1 — Check if there is an unused OTP (status = 0)
	checked, err := s.db.GetOTPChecked(ctx, msisdn, otp)
	if err != nil {
		logrus.Errorf("GetOTPChecked error: %v", err)
		return 0, err
	}
	if checked == nil {
		// invalid otp
		logrus.Warnf("Invalid OTP for msisdn=%s", msisdn)
		return 0, fmt.Errorf("Wrong Code")
	}

	// Step 2 — Verify expiry (expired > now)
	verified, err := s.db.GetOTPVerified(ctx, msisdn, otp, now)
	if err != nil {
		logrus.Errorf("GetOTPVerified error: %v", err)
		return 0, err
	}

	// Step 3 — Mark OTP as used (status = 1) using id from checked row

	if _, err := s.db.UpdateIntoVerification(ctx, checked["id"].(int32)); err != nil {
		logrus.Errorf("UpdateIntoVerification error: %v", err)
		return 0, err
	}

	if _, err := s.db.UpdateIntoRegistration(ctx, msisdn); err != nil {
		logrus.Errorf("UpdateIntoVerification error: %v", err)
		return 0, err
	}

	if _, err := s.db.UpdateIntoPlayer(ctx, msisdn); err != nil {
		logrus.Errorf("UpdateIntoVerification error: %v", err)
		return 0, err
	}
	// Step 4 — If verified == nil → expired
	if verified == nil {
		logrus.Warnf("OTP expired for msisdn=%s", msisdn)
		return 0, fmt.Errorf("otp expired")
	}

	// Compute remaining seconds until expiry
	expiredVal, ok := verified["expired"]
	if !ok {
		// If the column is missing, treat as success but no expiry info.
		return 0, nil
	}

	var expiredSec int64
	switch v := expiredVal.(type) {
	case int64:
		expiredSec = v
	case int:
		expiredSec = int64(v)
	case float64:
		expiredSec = int64(v)
	case string:
		// attempt parse if stored as string
		var parsed int64
		_, err := fmt.Sscan(v, &parsed)
		if err == nil {
			expiredSec = parsed
		} else {
			// if it's a timestamp string, try parsing RFC3339
			if t, perr := time.Parse(time.RFC3339, v); perr == nil {
				expiredSec = t.Unix()
			} else {
				// unknown format
				expiredSec = 0
			}
		}
	default:
		expiredSec = 0
	}

	remain := expiredSec - now
	if remain < 0 {
		// expired (this branch should be rare because GetOTPVerified already checks expired > now)
		return 0, fmt.Errorf("otp expired")
	}

	// success: return remaining seconds until expiry
	return remain, nil
}

func (s *LuckyNumberService) CheckUser(msisdn string, name string, promocode string) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		log.Printf("PANIC PREVENTION: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}
	ctx := context.Background()

	user, err := s.db.CheckUser(ctx, msisdn)
	if err != nil {
		logrus.Errorf("Error checking user: %v", err)
		return nil, err
	}
	logrus.Infof("user already : %s", user)

	// Create user if doesn't exist
	if user == nil {
		carrier := s.getMNOCategory(msisdn)
		promo := s.randomString(5)

		_, err := s.db.CreateUser(ctx, carrier, msisdn, name, promo, promocode)
		if err != nil {
			logrus.Errorf("Error creating user: %v", err)
			return nil, err
		}
		_, errd := s.db.CreatePromo(ctx, msisdn, promo)
		if errd != nil {
			logrus.Errorf("Error creating promo: %v", err)
			return nil, err
		}
		// Get the newly created user
		user, err = s.db.CheckUser(ctx, msisdn)
		if err != nil {
			logrus.Errorf("Error getting new user: %v", err)
			return nil, err
		}
		return user, nil
	} else {
		return user, nil
	}
}

func (s *LuckyNumberService) CheckUserNoCreating(msisdn string) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		log.Printf("PANIC PREVENTION: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}
	ctx := context.Background()

	user, err := s.db.CheckUser(ctx, msisdn)
	if err != nil {
		logrus.Errorf("Error checking user: %v", err)
		return nil, err
	}
	logrus.Infof("user already : %s", user)
	// Create user if doesn't exist
	if user == nil {
		return user, nil
	} else {
		return user, nil
	}
}

func (s *LuckyNumberService) CheckUserNoCreatingAttempted(msisdn string) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		log.Printf("PANIC PREVENTION: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}
	ctx := context.Background()

	user, err := s.db.CheckUserAttempted(ctx, msisdn)
	if err != nil {
		logrus.Errorf("Error checking user: %v", err)
		return nil, err
	}
	logrus.Infof("user already : %s", user)
	// Create user if doesn't exist
	if user == nil {
		return user, nil
	} else {
		return user, nil
	}
}

func (s *LuckyNumberService) CheckSelfExclusion(msisdn string) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		log.Printf("PANIC PREVENTION: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}
	ctx := context.Background()

	self, err := s.db.CheckSelfExclusion(ctx, msisdn)
	if err != nil {
		logrus.Errorf("Error checking user: %v", err)
		return nil, err
	}
	logrus.Infof("self already : %s", self)
	// Create user if doesn't exist
	if self == nil {
		return self, nil
	} else {
		return self, nil
	}
}
func (s *LuckyNumberService) CheckPromoCode(promocode string) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		log.Printf("PANIC PREVENTION: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}
	ctx := context.Background()
	promo, err := s.db.CheckPromoCode(ctx, promocode)
	if err != nil {
		logrus.Errorf("Error checking promo: %v", err)
		return nil, err
	}
	logrus.Infof("promo already : %s", promo)
	// Create user if doesn't exist
	return promo, nil
}

func (s *LuckyNumberService) RequestSelfExlusion(msisdn string, hrs int) (map[string]interface{}, error) {
	if s == nil || s.db == nil {
		log.Printf("PANIC PREVENTION: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}
	ctx := context.Background()
	_, err := s.db.RequestSelfExlusion(ctx, msisdn, hrs)
	if err != nil {
		logrus.Errorf("Error checking promo: %v", err)
		return nil, err
	}
	// Create user if doesn't exist
	return nil, err
}

func (s *LuckyNumberService) GetDeposits(msisdn string, startDate, endDate string) ([]map[string]interface{}, error) {
	if s == nil || s.db == nil {
		logrus.Warnf("Service or DB not initialized: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}

	ctx := context.Background()

	var startPtr, endPtr *string

	if startDate != "" {
		startPtr = &startDate
	}
	if endDate != "" {
		endPtr = &endDate
	}
	// Call DB method with date range
	history, err := s.db.CheckDeposits(ctx, msisdn, startPtr, endPtr)
	if err != nil {
		logrus.Errorf("Error checking history for msisdn %s: %v", msisdn, err)
		return nil, err
	}

	return history, nil
}

func (s *LuckyNumberService) GetWithdrawals(msisdn string, startDate, endDate string) ([]map[string]interface{}, error) {
	if s == nil || s.db == nil {
		logrus.Warnf("Service or DB not initialized: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}

	ctx := context.Background()

	var startPtr, endPtr *string

	if startDate != "" {
		startPtr = &startDate
	}
	if endDate != "" {
		endPtr = &endDate
	}
	// Call DB method with date range
	history, err := s.db.CheckWithdrawal(ctx, msisdn, startPtr, endPtr)
	if err != nil {
		logrus.Errorf("Error checking history for msisdn %s: %v", msisdn, err)
		return nil, err
	}

	return history, nil
}

func (s *LuckyNumberService) GetWinners() ([]map[string]interface{}, error) {
	if s == nil || s.db == nil {
		logrus.Warnf("Service or DB not initialized: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}

	ctx := context.Background()

	// Call DB method with date range
	history, err := s.db.GetWinners(ctx)
	if err != nil {
		return nil, err
	}

	return history, nil
}

func (s *LuckyNumberService) GetOnlineUsers() ([]map[string]interface{}, error) {
	if s == nil || s.db == nil {
		logrus.Warnf("Service or DB not initialized: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}

	ctx := context.Background()

	// Call DB method with date range
	onlineusers, err := s.db.GetOnlineUsers(ctx)
	if err != nil {
		return nil, err
	}

	return onlineusers, nil
}
func (s *LuckyNumberService) GetGameHistory(msisdn string, offset string, page_size string, startDate, endDate string) ([]map[string]interface{}, error) {
	if s == nil || s.db == nil {
		logrus.Warnf("Service or DB not initialized: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}

	ctx := context.Background()

	var startPtr, endPtr *string

	if startDate != "" {
		startPtr = &startDate
	}
	if endDate != "" {
		endPtr = &endDate
	}
	// Call DB method with date range
	history, err := s.db.CheckGameHistory(ctx, msisdn, startPtr, endPtr, offset, page_size)
	if err != nil {
		logrus.Errorf("Error checking history for msisdn %s: %v", msisdn, err)
		return nil, err
	}

	return history, nil
}

func (s *LuckyNumberService) GetHistory(msisdn string, startDate, endDate string) ([]map[string]interface{}, error) {
	if s == nil || s.db == nil {
		logrus.Warnf("Service or DB not initialized: s=%p, s.db=%p", s, s.db)
		return nil, fmt.Errorf("service or database not initialized")
	}

	ctx := context.Background()

	var startPtr, endPtr *string

	if startDate != "" {
		startPtr = &startDate
	}
	if endDate != "" {
		endPtr = &endDate
	}
	// Call DB method with date range
	history, err := s.db.CheckHistory(ctx, msisdn, startPtr, endPtr)
	if err != nil {
		logrus.Errorf("Error checking history for msisdn %s: %v", msisdn, err)
		return nil, err
	}

	return history, nil
}

func (s *LuckyNumberService) InsertLogs(msisdn, sessionId, serviceCode, ussdString string) error {
	ctx := context.Background()
	_, err := s.db.InsertUSSDLogs(ctx, msisdn, sessionId, serviceCode, ussdString)
	return err
}
func (s *LuckyNumberService) InsertSessionID(
	playerID int64,
	channel string,
	hours int64,
	now time.Time,
) (int64, error) {
	ctx := context.Background()
	sessionID, err := s.db.InsertSessionID(ctx, playerID, channel, hours, now)
	if err != nil {
		return 0, err
	}
	return sessionID, nil
}

func (s *LuckyNumberService) UpdateUser(player_id, name string) error {
	ctx := context.Background()
	_, err := s.db.UpdateUserInfo(ctx, player_id, name)
	return err
}
func (s *LuckyNumberService) UpdateMsisdn(msisdn, newmsisdn string) error {
	ctx := context.Background()
	_, err := s.db.DeleteUserAttempted(ctx, msisdn)
	_, err = s.db.UpdateUserMsisdn(ctx, msisdn, newmsisdn)
	return err
}

func (s *LuckyNumberService) UpdatePlayerSelf(msisdn string, hrs string) error {
	ctx := context.Background()
	err := s.db.UpdateSelfExclusion(ctx, msisdn)
	err = s.db.UpdatePlayerSelf(ctx, msisdn, hrs)
	return err
}
func (s *LuckyNumberService) DeleteUser(msisdn string) error {
	ctx := context.Background()
	_, err := s.db.DeleteUserInfo(ctx, msisdn)
	return err
}

func (s *LuckyNumberService) CreateUserAttempted(msisdn string, new_msisdn string) error {
	ctx := context.Background()
	_, err := s.db.CreateUserAttempted(ctx, msisdn, new_msisdn)
	return err
}
func (s *LuckyNumberService) UpdateUserWinStatus(msisdn, show_win string) error {
	ctx := context.Background()
	_, err := s.db.UpdateUserWinStatus(ctx, msisdn, show_win)
	return err
}

func (s *LuckyNumberService) UpdateUserProfilePic(player_id, filename string) error {
	ctx := context.Background()
	_, err := s.db.UpdateUserProfilePic(ctx, player_id, filename)
	return err
}
func (s *LuckyNumberService) InsertVerification(msisdn string, code string, expired int64, created int64) error {
	ctx := context.Background()

	message := fmt.Sprintf(
		"Your OTP Code is: %s",
		code,
	)
	// Queue SMS
	er := s.sendsms(msisdn, message)
	if er != nil {
		return fmt.Errorf("failed to insert SMS queue: %w", er)
	}

	_, err := s.db.InsertVerification(ctx, msisdn, code, expired, created)
	return err
}
func (s *LuckyNumberService) IniatatDeposit(msisdn string, amount float64, user_id, channel string) (PlaceBetResult, error) {
	// NOTE: removed s.mu.Lock() / defer s.mu.Unlock() — do not serialize DB ops globally.

	// Give each request a reasonable timeout so slow DB calls don't hang forever.
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	// 1) Check user
	user, err := s.db.CheckUser(ctx, msisdn)
	if err != nil {
		logrus.Errorf("CheckUser error: %v", err)
		return PlaceBetResult{}, err
	}
	mnoCategory := s.getMNOCategory(msisdn)
	// 2) Create user if missing (do this synchronously)
	if user == nil {
		promo := s.randomString(5)

		if _, err := s.db.CreateUser(ctx, mnoCategory, msisdn, "", promo, ""); err != nil {
			logrus.Errorf("CreateUser error: %v", err)
			return PlaceBetResult{}, err
		}
		_, errd := s.db.CreatePromo(ctx, msisdn, promo)
		if errd != nil {
			logrus.Errorf("Error creating promo: %v", err)
			return PlaceBetResult{}, err
		}
		// optionally re-fetch user if you need returned fields
	}
	// 3) compute adjusted amount (synchronous because it likely reads DB)
	adjustedAmount, err := s.adjustBetAmount(ctx, msisdn, amount)
	if err != nil {
		logrus.Errorf("adjustBetAmount error: %v", err)
		return PlaceBetResult{}, err
	}
	// 4) generate id / game id
	gameID := "WEB_" + s.randomString(10)
	// 5) Run the two inserts concurrently: InsertIntoDepositLuckyRequest and InsertSTK

	g, egCtx := errgroup.WithContext(ctx)

	err = s.SendPaymentRequest(msisdn, utils.ToString(amount), gameID)
	if err != nil {
		fmt.Println("Payment error:", err)
	}

	// Insert deposit request
	g.Go(func() error {
		// Use the db method which should use the pool and acquire a connection per call.
		_, err := s.db.InsertIntoDepositLuckyRequest(egCtx, "", "", mnoCategory, "0", adjustedAmount, msisdn, "0", gameID, channel, user_id)
		if err != nil {
			logrus.Errorf("InsertIntoDepositLuckyRequest error: %v", err)
			return err
		}
		return nil
	})
	// Insert STK record concurrently
	g.Go(func() error {
		_, err := s.db.InsertSTK(egCtx, "", mnoCategory, gameID, msisdn, adjustedAmount, "00000")
		if err != nil {
			logrus.Errorf("InsertSTK error: %v", err)
			return err
		}
		return nil
	})
	// wait for both
	if err := g.Wait(); err != nil {
		// one (or both) failed
		return PlaceBetResult{}, err
	}

	// Success
	return PlaceBetResult{FreeBet: "false", Message: "Kukamilisha BET weka M-Pesa PIN yako."}, nil
}

func (s *LuckyNumberService) SendPaymentRequest(msisdn string, amount string, gameID string) error {
	// Generate gameID

	// Create request body JSON
	payload := map[string]interface{}{
		"amount":    amount,
		"msisdn":    msisdn,
		"reference": gameID,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}

	// Prepare HTTPS client
	client := &http.Client{
		Timeout: 20 * time.Second,
	}

	req, err := http.NewRequest("POST", "http://172.16.0.184:8008/api/v1/initiate_deposit", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("creating request failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("https request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("api error: status %d", resp.StatusCode)
	}

	return nil
}

func (s *LuckyNumberService) sendsms(msisdn string, message string) error {

	// ctx := context.Background()
	// senderID := "LuckyNumber"
	// _, err := s.db.InsertIntoSMSQueue(ctx, msisdn, message, senderID, "game_response")
	// // Create request body JSON
	// payload := map[string]interface{}{
	// 	"message": message,
	// 	"msisdn":  msisdn,
	// }
	// jsonData, err := json.Marshal(payload)
	// if err != nil {
	// 	return fmt.Errorf("json marshal error: %w", err)
	// }
	// // Prepare HTTPS client
	// client := &http.Client{
	// 	Timeout: 20 * time.Second,
	// }
	// req, err := http.NewRequest("POST", "http://172.16.0.184:8008/api/v1/insert_sms", bytes.NewBuffer(jsonData))
	// if err != nil {
	// 	return fmt.Errorf("creating request failed: %w", err)
	// }

	// req.Header.Set("Content-Type", "application/json")

	// // Send request
	// resp, err := client.Do(req)
	// if err != nil {
	// 	return fmt.Errorf("https request failed: %w", err)
	// }
	// defer resp.Body.Close()
	// if resp.StatusCode != http.StatusOK {
	// 	return fmt.Errorf("api error: status %d", resp.StatusCode)
	// }
	return nil
}

// PlaceBet handles the main betting logic
func (s *LuckyNumberService) PlaceBet(session_id string, session_start string, user map[string]interface{}, ussd string, name string, gameCatID string, msisdn string, amount float64, selectedNumber string, channel string) (PlaceBetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()

	gameID := s.randomString(10)

	// 3. Handle free bet
	if user != nil && s.hasActiveFreeBet(user) {
		logrus.Infof("Freebet is working: %v", user)

		var totalBetsHist []Bet // adjust type to your CheckBets return type
		var wg sync.WaitGroup
		var errCheckBets, errUpdateUser error

		wg.Add(2)
		// Run CheckBets in parallel
		go func() {
			defer wg.Done()
			_, errCheckBets = s.db.CheckBets(ctx, msisdn)
		}()
		// Run UpdateUserLucky in parallel
		go func() {
			defer wg.Done()
			_, errUpdateUser = s.db.UpdateUserLucky(ctx, msisdn)
		}()

		wg.Wait()

		if errCheckBets != nil {
			return PlaceBetResult{}, errCheckBets
		}
		if errUpdateUser != nil {
			return PlaceBetResult{}, errUpdateUser
		}

		// Refresh user data after updates

		// Play game immediately
		game_result, err := s.playGame(ctx, session_id, session_start, totalBetsHist, gameCatID, user, msisdn, amount, selectedNumber, gameID,
			"free_bet", channel, ussd, name)
		if err != nil {
			return PlaceBetResult{}, err
		}

		return PlaceBetResult{GameResult: game_result, FreeBet: "true", Message: "Free Bet Placed Successful"}, nil
	} else {
		num := user["balance"].(pgtype.Numeric)

		var totalBetsHist, err = s.db.CheckBets(ctx, msisdn)
		if err != nil {
			return PlaceBetResult{}, err
		}

		f, _ := num.Float64Value()
		balance := f.Float64
		if balance >= amount {

			game_result, err := s.playGame(ctx,
				session_id,
				session_start,
				totalBetsHist,
				gameCatID, // Use toString instead of type assertion
				user,
				msisdn,
				amount, // Use toFloat64 instead of type assertion
				selectedNumber,
				gameID,
				"normal",
				channel,
				"",
				name)

			if err != nil {
				return PlaceBetResult{}, err
			}

			return PlaceBetResult{GameResult: game_result, FreeBet: "false", Message: "Bet Placed Successful"}, nil
		} else {

			return PlaceBetResult{}, fmt.Errorf("insufficient balance")
		}
	}
}

// // HandleDepositAndGame processes deposit and starts the game
// func (s *LuckyNumberService) HandleDepositAndGame(data map[string]interface{}) error {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	ctx := context.Background()

// 	transactionID, _ := data["transaction_id"].(string)
// 	reference, _ := data["reference"].(string)
// 	name, _ := data["name"].(string)

// 	// Check transaction and deposit request
// 	checkTransaction, err := s.db.CheckTransaction(ctx, transactionID)
// 	if err != nil {
// 		return err
// 	}

// 	stkUSSD, err := s.db.CheckDepositRequestLucky(ctx, reference)
// 	if err != nil {
// 		return err
// 	}

// 	if checkTransaction == nil && stkUSSD != nil && stkUSSD["msisdn"] != nil {
// 		msisdn := stkUSSD["msisdn"].(string)
// 		user, err := s.db.CheckUser(ctx, msisdn)
// 		if err != nil {
// 			return err
// 		}

// 		// Create user if doesn't exist
// 		if user == nil {
// 			mnoCategory := s.getMNOCategory(msisdn)
// 			promo := s.randomString(5)

// 			_, err = s.db.CreateUser(ctx, mnoCategory, msisdn, "", promo, "")
// 			if err != nil {
// 				return err
// 			}
// 			_, errd := s.db.CreatePromo(ctx, msisdn, promo)
// 			if errd != nil {
// 				logrus.Errorf("Error creating promo: %v", err)
// 				return err
// 			}
// 			user, err = s.db.CheckUser(ctx, msisdn)
// 			if err != nil {
// 				return err
// 			}
// 		}

// 		amount := stkUSSD["amount"].(float64)
// 		_, err = s.db.UpdateUserAviatorBalInfoLucky(ctx, amount, msisdn, name)
// 		if err != nil {
// 			return err
// 		}

// 		// Extract game data and start the game
// 		gameCatID := stkUSSD["game_cat_id"].(string)
// 		selectedNumber := stkUSSD["selected_box"].(string)
// 		channel, _ := stkUSSD["channel"].(string)
// 		ussd, _ := stkUSSD["ussd"].(string)
// 		gameName, _ := stkUSSD["game"].(string)

// 		_, err = s.playGame(ctx, nil, gameCatID, user, msisdn, amount, selectedNumber, reference, "normal", channel, ussd, gameName)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// SettleDeposit handles deposit settlement
func (s *LuckyNumberService) SettleDeposit(data map[string]interface{}, msisdn string, amount float64, name, transactionID, betType, reference string, description, ussd, shortcode, gameName string) (map[string]interface{}, error) {
	ctx := context.Background()

	// Check if transaction already exists
	transactionExists, err := s.db.CheckTransaction(ctx, transactionID)
	if err != nil {
		logrus.Errorf("Error checking transaction: %v", err)
		return nil, err
	}

	logrus.Infof("Transaction already : %s", transactionExists)

	if len(transactionExists) > 0 {
		logrus.Info("No transaction found, safe to insert")
		logrus.Infof("Transaction already exists: %d records", transactionID)
		logrus.Infof("Transaction already exists: %d records", len(transactionExists))
		return nil, err
		// handle duplicate
	} else {
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
			promo := s.randomString(5)

			_, err := s.db.CreateUser(ctx, carrier, msisdn, "", promo, "")
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

		var gameCatID = utils.ToString(depositRequest["game_cat_id"]) // Use toString instead of type assertion
		var selectedNumber = utils.ToString(depositRequest["selected_box"])
		var channel = utils.ToString(depositRequest["channel"])

		// Update user balance
		errs := make(chan error, 5)

		num := user["balance"].(pgtype.Numeric)

		bal := num
		// Now you can add

		// total := amount // var userBalance float64 = 250.0

		message := fmt.Sprintf(
			"Account balance yako ni: Ksh.%.2f\n\nBONYEZA *463# UKAMILISHE BET YAKO",
			bal,
		)

		if depositRequest == nil {
			reference := s.randomString(10)

			var gameCatID = "0" // Use toString instead of type assertion
			var selectedNumber = "0"
			var channel = "direct"

			logrus.Infof("depositRequest already : %s", depositRequest)

			go func() {
				err := s.db.InsertIntoDepositLuckyRequestComplete(ctx, data, transactionID, description, gameName, s.getMNOCategory(msisdn), channel, gameCatID, amount, msisdn, selectedNumber, reference, utils.ToInt64(user["player_id"]))
				errs <- err
				// }
			}()

			go func() {
				err = s.sendsms(msisdn, message)
			}()
			// collect errors

			logrus.Infof("Deposit settled successfully: reference=%s, msisdn=%s, amount=%.2f, %s",
				reference, msisdn, amount, depositRequest)

			return depositRequest, nil
		} else {
			msisdn := utils.ToString(depositRequest["msisdn"])
			if msisdn == "" {
				logrus.Errorf("MSISDN not found in deposit request: %s", reference)
				return nil, fmt.Errorf("msisdn not found in deposit request")
			}
			logrus.Infof("depositRequest already : %s", depositRequest)
			amount := (depositRequest["amount"]).(float64)
			// message := fmt.Sprintf(
			// 	"Account balance yako ni: Ksh.%.2f\n\nBONYEZA *463# UKAMILISHE BET YAKO",
			// 	total,
			// )
			// go func() {
			// 	_, err := s.db.UpdateUserAviatorBalInfoLucky(ctx, amount, msisdn, name)
			// 	errs <- err
			// }()
			go func() {
				if betType == "normal" {
					err := s.db.UpdateAviatorDepositRequestLucky(ctx, data, transactionID, reference, description)
					errs <- err
				} else {
					betType = "free_bet"
					_, err := s.db.InsertIntoDepositLuckyRequestBonus(ctx, betType, ussd, gameName, s.getMNOCategory(msisdn), gameCatID, amount, msisdn, selectedNumber, reference, channel)
					errs <- err
				}
			}()
			go func() {
				err = s.sendsms(msisdn, message)
				errs <- err
			}()

			// go func() {
			// 	_, err := s.db.DeleteUserAttempted(ctx, msisdn)
			// 	errs <- err
			// }()

			// go func() {
			// 	_, err := s.db.UpdateKPIDeposit(ctx, amount)
			// 	errs <- err
			// }()
			// go func() {
			// 	_, err := s.db.CreateDepositRecordLucky(ctx, msisdn, amount, transactionID, shortcode, name, reference, betType)
			// 	errs <- err
			// }()
			// go func() {
			// 	_, err := s.db.InsertCustomerLogsPawaBoxKe(ctx, amount, "deposit", utils.ToString(user["id"]), "customer deposit: lucky", reference)
			// 	errs <- err
			// }()
			// collect errors
			// for i := 0; i < 5; i++ {
			// 	if err := <-errs; err != nil {
			// 		logrus.Errorf("DB operation failed: %v", err)
			// 		// Note: cannot rollback since they are already executed individually
			// 	}
			// }

			logrus.Infof("Deposit settled successfully: reference=%s, msisdn=%s, amount=%.2f, %s",
				reference, msisdn, amount, depositRequest)

			return depositRequest, nil
		}

	}
}

// ProcessBetAndPlayGame handles the main game logic
func (s *LuckyNumberService) ProcessBetAndPlayGame(data map[string]interface{}) (map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ref := utils.ToString(data["reference"])
	// Settle deposit first
	_, err := s.SettleDeposit(
		data,
		utils.ToString(data["msisdn"]),
		utils.ToFloat64(data["amount"]),
		utils.ToString(data["name"]),
		utils.ToString(data["transaction_id"]),
		"normal",
		ref,
		utils.ToString(data["description"]),
		utils.ToString(data["ussd"]),
		utils.ToString(data["shortcode"]),
		utils.ToString(data["game_name"]))

	// logrus.Infof("process_bet_and_play_game error: %v", ref)

	if err != nil {
		logrus.Errorf("Failed to settle deposit: %v", err)
		return nil, fmt.Errorf("failed to settle deposit: %w", err)
	}

	return nil, err

}

// Helper methods
func (s *LuckyNumberService) randomString(length int) string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[cryptoRandIndex(len(charset))]
	}
	return string(result)
}

func (s *LuckyNumberService) getMNOCategory(msisdn string) string {
	return "SAFARICOM" // Simplified for Kenya
}
func (s *LuckyNumberService) hasActiveFreeBet(user map[string]interface{}) bool {
	isFree, ok1 := user["is_free"].(string)
	freeBet, ok2 := user["free_bet"].(float64)
	expiryTime, ok3 := user["freebet_expiry"].(time.Time)

	logrus.Infof("Freebet is working: is_free=%s, free_bet=%.2f, freebet_expiry=%v", isFree, freeBet, expiryTime)

	if !ok1 || !ok2 || !ok3 {
		return false
	}

	if isFree != "YES" || freeBet <= 0 {
		return false
	}

	if time.Now().Before(expiryTime) {
		return true
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
func (s *LuckyNumberService) playGame(ctx context.Context, session_id, session_start, history interface{}, gameCatID string, player map[string]interface{}, msisdn string, betAmount float64, selectedNumber, reference, betType, channel, ussd, gameName string) (PlaceBetResultDisplay, error) {
	// Get settings
	var (
		setting interface{}
		game    interface{}
		kpi     interface{}
		house   interface{}
	)
	var (
		errSetting, errGame, errKPI, errHouse error
	)
	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		defer wg.Done()
		setting, errSetting = s.db.CheckSetting(ctx)
	}()
	go func() {
		defer wg.Done()
		game, errGame = s.db.CheckGamePlay(ctx, gameCatID)
	}()

	go func() {
		defer wg.Done()
		kpi, errKPI = s.db.CheckSettingKPI(ctx)
	}()

	go func() {
		defer wg.Done()
		house, errHouse = s.db.CheckHousePawaBoxKe(ctx)
	}()
	wg.Wait()

	if errSetting != nil {
		return PlaceBetResultDisplay{}, errSetting
	}
	if errGame != nil {
		return PlaceBetResultDisplay{}, errGame
	}
	if errKPI != nil {
		return PlaceBetResultDisplay{}, errKPI
	}
	if errHouse != nil {
		return PlaceBetResultDisplay{}, errHouse
	}

	// Now you can use setting, game, kpi, house as interface{} and type assert when needed
	houseMap, ok := house.(map[string]interface{})
	if !ok {
		return PlaceBetResultDisplay{}, fmt.Errorf("house is not a map")
	}

	settingMap, ok := setting.(map[string]interface{})
	if !ok {
		return PlaceBetResultDisplay{}, fmt.Errorf("setting is not a map")
	}
	gameMap, ok := game.(map[string]interface{})
	if !ok {
		return PlaceBetResultDisplay{}, fmt.Errorf("game is not a map")
	}
	kpiMap, ok := kpi.(map[string]interface{})
	if !ok {
		return PlaceBetResultDisplay{}, fmt.Errorf("kpi is not a map")
	}

	basket, err := s.db.CheckBasketLucky(ctx)

	if err != nil {
		return PlaceBetResultDisplay{}, fmt.Errorf("failed to fetch baskets: %w", err)
	}

	// Calculate current RTP
	totalBets := houseMap["total_bets"].(float64) + betAmount
	currentRTP := 0.0
	if totalBets > 0 {
		currentRTP = houseMap["total_wins"].(float64) / totalBets
	}
	defaultRTP := settingMap["default_rtp"].(float64) + settingMap["jackpot_percentage"].(float64)
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
	err = s.bet(ctx, reference, player["player_id"].(int64), playerTotalBets, betAmount)
	if err != nil {
		return PlaceBetResultDisplay{}, err
	}

	// Calculate basket and house values
	globalRTP := settingMap["default_rtp"].(float64) + settingMap["adjustmentable_rtp"].(float64)
	basketValue := betAmount * (globalRTP / 100)
	houseValue := (settingMap["vig_percentage"].(float64) / 100) * betAmount
	jackpotValue := (settingMap["jackpot_percentage"].(float64) / 100) * betAmount

	// Update jackpot for specific games
	gameInit := gameMap["name_init"].(string)
	if s.isJackpotGame(gameInit) {
		_, err = s.db.UpdateJackpotKitNameInit(ctx, jackpotValue, gameInit)
		if err != nil {
			return PlaceBetResultDisplay{}, err
		}
	}

	// Calculate taxes

	var withholdTaxJackpot = 0.00
	if settingMap["withholding"].(float64) > 0 {
		withholdTaxJackpot = (settingMap["withholding"].(float64) / 100) * jackpotValue
	}

	var exciseTaxAmount = 0.00
	if settingMap["excise_duty"].(float64) > 0 {
		exciseTaxAmount = (settingMap["excise_duty"].(float64) / 100) * betAmount
	}

	// Handle deposit based on bet type
	var depositTask func() error
	if betType == "free_bet" {
		depositTask = func() error {
			_, err := s.db.InsertIntoDepositLuckyRequestBonus(ctx, betType, ussd, gameName,
				s.getMNOCategory(msisdn), gameCatID, betAmount, msisdn, selectedNumber, reference, channel)
			return err
		}
	} else {
		depositTask = func() error { return nil }
	}

	var updateUserRTPTask func() error
	if betType == "normal" {
		updateUserRTPTask = func() error {
			_, err := s.db.UpdateUserRTP(ctx, betAmount, player["player_id"].(int64))
			return err
		}
	} else {
		updateUserRTPTask = func() error { return nil }
	}

	// if betType == "free_bet" {
	// 	updateKPITask = func() error {
	// 		_, err := s.db.InsertIntoDepositLuckyRequestBonus(ctx, betType, ussd, gameName,
	// 			s.getMNOCategory(msisdn), gameCatID, betAmount, msisdn, selectedNumber, reference, channel)
	// 		return err
	// 	}
	// } else {
	// 	depositTask = func() error { return nil }
	// }

	// Execute all database operations
	tasks := []func() error{
		depositTask,
		updateUserRTPTask,
		func() error {

			gross := betAmount
			statenet := betAmount - jackpotValue - exciseTaxAmount
			err := s.db.CreateBet(ctx, utils.ToString(basket["id"]), basketValue, utils.ToInt64(player["player_id"]), withholdTaxJackpot, exciseTaxAmount, utils.ToInt64(session_id), utils.ToString(session_start), gross, statenet, houseValue, jackpotValue, selectedNumber, betAmount, "", reference, "Pending", betType, gameCatID, gameName, fmt.Sprintf("%.2f added to the basket:- game id %s", basketValue, reference), channel)

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
	}
	// Run all tasks in parallel
	errs := make(chan error, len(tasks))
	wg.Add(len(tasks))
	for _, task := range tasks {
		t := task // capture loop variable
		go func() {
			defer wg.Done()
			if err := t(); err != nil {
				errs <- err
			}
		}()
	}

	// Wait for all tasks to finish
	wg.Wait()
	close(errs)

	// Check for errors
	for err := range errs {
		if err != nil {
			return PlaceBetResultDisplay{}, err
		}
	}

	// Check for jackpot winner
	jackpotWinner, err := s.db.CheckJackpotWinner(ctx)
	if err != nil {
		return PlaceBetResultDisplay{}, err
	}

	// Determine game outcome
	minLossCount := cryptoRandIndex(int(settingMap["min_loss_count"].(float64)))

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
	if playerFrequency > 10 && playerLostCount > int64(minLossCount) && jackpotWinner != nil {

		// Handle jackpot win condition
		// if playerFrequency > 10 && jackpotWinner != nil {
		return s.handleJackpotWin(ctx, player, msisdn, betAmount, utils.ToInt(selectedNumber), reference, settingMap, gameMap, kpiMap, jackpotWinner)
	} else {
		return s.handleNormalGame(ctx, player, msisdn, betAmount, selectedNumber, reference, settingMap, gameMap, kpiMap, minLossCount)
	}
}

// bet records a bet for a player
func (s *LuckyNumberService) bet(ctx context.Context, reference string, playerID int64, totalBets, amount float64) error {
	_, err := s.db.UpdateUserBet(ctx, amount, playerID)
	if err != nil {
		return err
	}
	// _, err = s.db.InsertCustomerLogsPawaBoxKe(ctx, amount, "bet", utils.ToString(playerID), "customer placed bet", reference)
	// if err != nil {
	// 	return err
	// }

	return nil
}

// win records a win for a player
func (s *LuckyNumberService) winJackpot(ctx context.Context, playerID int64, payout, bets float64, winItem string, withholdTax, taxDeductedAmount, amount float64, msisdn, reference string) error {
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
			_, err = s.db.InsertWithdrawalQueue(ctx, reference, msisdn, taxDeductedAmountNew, "http?")
			if err != nil {
				return err
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

func (s *LuckyNumberService) GenerateWinJackpotWinner(
	ctx context.Context,
	msisdn string,
	kpi map[string]interface{},
	defaultRTP, playerRTP float64,
	reference string,
	betAmount float64,
	selectedNumber int,
	playerID int,
	minWinMultiplier, maxWinMultiplier float64,
	maxExposure float64,
	nameInit string,
	playerCount, maxLossCount int,
	maxWon, vigPercentage float64,
	itemWinValue float64,
	itemWon string) (map[int]WinAmount, error) {
	//-------------------------------------
	// Step 1 — Choose 7 unique box numbers
	//-------------------------------------
	chosen := cryptoRandUniqueInts(1, 8, 7) // {1..7}
	numZeroBoxes := cryptoRandInt(0, 3)     // 0–2

	boxes := make(map[int]WinAmount)

	minWinAmount := betAmount * minWinMultiplier
	maxWinAmount := maxExposure

	//-------------------------------------
	// Step 2 — Assign random win amounts
	//-------------------------------------
	for _, num := range chosen {

		var winAmt float64

		if cryptoRandFloat() < 0.5 {
			// small range
			winAmt = cryptoRandFloatRange(minWinAmount, minWinAmount*20)
		} else {
			winAmt = cryptoRandFloatRange(minWinAmount, maxWinAmount)
		}

		boxes[num] = WinAmount{
			Value: winAmt,
			Item:  FormatToMZN(winAmt),
		}
	}

	//-------------------------------------
	// Step 3 — Zero out random boxes (except selected box)
	//-------------------------------------
	candidates := make([]int, 0)
	for _, n := range chosen {
		if n != selectedNumber {
			candidates = append(candidates, n)
		}
	}

	zeroBoxes := cryptoRandSample(candidates, numZeroBoxes)
	for _, zb := range zeroBoxes {
		boxes[zb] = WinAmount{Value: 0, Item: "0"}
	}

	//-------------------------------------
	// Step 4 — Add a random AWARD box
	//-------------------------------------
	award, err := s.db.CheckAwardsLuckyRandom(ctx, nameInit)
	if err != nil {
		return nil, err
	}

	if len(candidates) > 0 {
		rnd := candidates[cryptoRandInt(0, len(candidates))]
		boxes[rnd] = WinAmount{
			Value: utils.ToFloat64(award["value"]),
			Item:  utils.ToString(award["name"]),
		}
	}

	//-------------------------------------
	// Step 5 — Set selected box winning
	//-------------------------------------
	boxes[selectedNumber] = WinAmount{
		Value: itemWinValue,
		Item:  itemWon,
	}

	return boxes, nil
}

func (s *LuckyNumberService) handleJackpotWin(
	ctx context.Context,
	player map[string]interface{},
	msisdn string,
	betAmount float64,
	selectedNumber int,
	reference string,
	setting, game, kpi, jackpotWinner map[string]interface{}) (PlaceBetResultDisplay, error) {
	// 1. Preconditions
	// 2. Update jackpot Kity (lock-in winner)
	// -------------------------------
	_, err := s.db.UpdateJackpotKitUpdate(ctx, utils.ToInt(jackpotWinner["id"]))

	defaultRTP := utils.ToFloat64(setting["default_rtp"])
	playerPayout := utils.ToFloat64(player["payout"])
	playerID := utils.ToInt64(player["player_id"])

	playerTotalBets := utils.ToFloat64(player["total_bets"])
	withholding := utils.ToFloat64(setting["withholding"])
	jackpotpercentage := utils.ToFloat64(setting["jackpot_percentage"])
	mx_win := playerTotalBets + betAmount - playerPayout
	playerFreeBet := utils.ToInt64(player["free_bet"])

	default_e := defaultRTP + jackpotpercentage
	max_won := (default_e / 100) * mx_win
	maxWon := utils.ToFloat64(max_won)
	// -------------------------------
	// 3. Generate jackpot win
	// -------------------------------
	winBoxes, err := s.GenerateWinJackpotWinner(
		ctx,
		msisdn,
		kpi,
		defaultRTP,
		utils.ToFloat64(player["rtp"]),
		reference,
		betAmount,
		selectedNumber,
		utils.ToInt(player["player_id"]),
		utils.ToFloat64(setting["min_win_multipier"]),
		utils.ToFloat64(setting["max_win_multipier"]),
		utils.ToFloat64(game["max_exposure"]),
		utils.ToString(game["name_init"]),
		utils.ToInt(player["lost_count"]),
		utils.ToInt(setting["min_loss_count"]),
		maxWon,
		utils.ToFloat64(setting["vig_percentage"]),
		utils.ToFloat64(jackpotWinner["cost"]),
		utils.ToString(jackpotWinner["item_name"]),
	)
	// -------------------------------
	// 4. Adjust jackpot win amount if needed
	// -------------------------------
	nameInit := utils.ToString(jackpotWinner["name_init"])
	isSpecialJackpot := nameInit == "pw_jackport" || nameInit == "pw_ist" || nameInit == "pw_mega"
	if isSpecialJackpot {
		winBox := winBoxes[selectedNumber]
		winBox.Value = utils.ToFloat64(jackpotWinner["cost"])
		winBox.Item = utils.ToString(jackpotWinner["item_name"])
		winBoxes[selectedNumber] = winBox

	}
	if winBoxes[selectedNumber].Value < 1 {
		winBox := winBoxes[selectedNumber]
		winBox.Value = utils.ToFloat64(jackpotWinner["cost"])
		winBox.Item = utils.ToString(jackpotWinner["item_name"])
		winBoxes[selectedNumber] = winBox

	}
	winAmount := winBoxes[selectedNumber].Value
	winItem := winBoxes[selectedNumber].Item
	logrus.Infof("Box %d wins jackpot: %+v", selectedNumber, winBoxes)
	// -------------------------------
	// 5. Mark bet as WIN
	// -------------------------------
	resultMessage := fmt.Sprintf("Box %s wins. Numbers: %+v", selectedNumber, winAmount)
	logrus.Info(resultMessage)
	// 6. Calculate withholding tax

	withholdTax := (withholding / 100) * winAmount
	taxDeductedAmount := winAmount - withholdTax
	// -------------------------------

	g, ct := errgroup.WithContext(ctx)

	// 1. Update bet as win
	g.Go(func() error {
		_, err := s.db.UpdateLuckyBetWin(
			ct,
			fmt.Sprintf("Box %d wins. Numbers: %+v", selectedNumber, winBoxes),
			"PAWABOX",
			reference,
			winAmount,
			"Win",
		)
		return err
	})

	// 2. Update jackpot entry
	g.Go(func() error {
		_, err := s.db.UpdateJackpotKity(
			ct,
			utils.ToInt(jackpotWinner["id"]),
		)
		return err
	})

	// 3. Update player loss stats
	g.Go(func() error {
		_, err := s.db.UpdatePlayerRestLossJackpot(
			ct,
			winAmount,
			utils.ToInt(player["player_id"]),
		)
		return err
	})
	// 4. Insert into Jackpot winners
	g.Go(func() error {
		_, err := s.db.InsertIntoJackPotWinners(
			ct,
			taxDeductedAmount,
			winItem,
			reference,
			utils.ToString(game["name"]),
			utils.ToString(jackpotWinner["item_name"]),
			utils.ToString(jackpotWinner["id"]),
			winAmount,
			msisdn,
		)
		return err
	})
	// Wait for all goroutines
	if err := g.Wait(); err != nil {
		return PlaceBetResultDisplay{}, err
	}

	// winBoxes[selectedNumber] = WinAmount{
	// 	Value: taxDeductedAmount,
	// 	Item:  FormatToMZN(taxDeductedAmount),
	// }
	// // Handle win logic

	converted := make(map[string]WinAmount)

	for k, v := range winBoxes {
		converted[fmt.Sprintf("%d", k)] = v
	}
	// msg := s.createWinMessage(converted)
	message := s.createWinMessage(utils.ToString(selectedNumber), converted, playerFreeBet, reference, withholding, withholdTax)
	logrus.Infof("Player MSISDN: %s", msisdn)
	resultd, err := s.ResultDisplay(utils.ToString(selectedNumber), converted, playerFreeBet, reference)
	// Queue SMS
	err = s.sendsms(msisdn, message)
	if err != nil {
		return PlaceBetResultDisplay{}, fmt.Errorf("failed to insert SMS queue: %w", err)
	}
	// -------------------------------
	if !isSpecialJackpot {
		err = s.winJackpot(ctx, playerID, playerPayout, playerTotalBets, winItem, withholdTax, taxDeductedAmount, winAmount, msisdn, reference)
		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to handle win: %w", err)
		}

		message := s.createJackpotMessage(utils.ToString(selectedNumber), converted, playerFreeBet, reference, withholding, taxDeductedAmount, withholdTax)

		err = s.sendsms(msisdn, message)
	}

	var boxes map[string]WinAmount
	if err := json.Unmarshal([]byte(resultd), &boxes); err != nil {
		logrus.Errorf("Failed to unmarshal Boxes JSON: %v", err)
		return PlaceBetResultDisplay{}, err
	}
	// 10. Return final response
	// -------------------------------
	mresult := PlaceBetResultDisplay{
		Boxes:         boxes,
		ResultStatus:  "Win",
		WinAmount:     0,
		JackPot:       "True",
		GameID:        reference,
		SelectedBox:   utils.ToString(selectedNumber),
		ResultMessage: message,
	}

	logrus.Infof("Player %s lost bet: %.2f", msisdn, betAmount)

	// return struct + nil error
	return mresult, nil
}

func (s *LuckyNumberService) handleNormalGame(ctx context.Context, player map[string]interface{}, msisdn string, betAmount float64, selectedNumber, reference string, setting, game, kpi map[string]interface{}, minLossCount int) (PlaceBetResultDisplay, error) {
	// Convert types safely
	playerID := utils.ToInt64(player["player_id"])
	playerLostCount := utils.ToInt64(player["lost_count"])
	playerFreeBet := utils.ToInt64(player["free_bet"])
	playerPayout := utils.ToFloat64(player["payout"])
	playerTotalBets := utils.ToFloat64(player["total_bets"])
	playerTotalLosses := utils.ToFloat64(player["total_losses"])
	defaultRTP := utils.ToFloat64(setting["default_rtp"])
	adjustmentableRTP := utils.ToFloat64(setting["adjustmentable_rtp"])
	minWinMultiplier := utils.ToFloat64(setting["min_win_multipier"])
	maxWinMultiplier := utils.ToFloat64(setting["max_win_multipier"])
	vigPercentage := utils.ToFloat64(setting["vig_percentage"])
	rtpOverload := utils.ToFloat64(setting["rtp_overload"])
	withholding := utils.ToFloat64(setting["withholding"])
	jackpotpercentage := utils.ToFloat64(setting["jackpot_percentage"])

	mx_win := playerTotalBets + betAmount - playerPayout

	default_e := defaultRTP + jackpotpercentage
	max_won := (default_e / 100) * mx_win
	maxWon := utils.ToFloat64(max_won)

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
		return PlaceBetResultDisplay{}, fmt.Errorf("failed to generate win amounts: %w", err)
	}

	logrus.Infof("Win amounts generated: %+v", winAmounts)

	// 🔥 CRITICAL SAFETY CHECKS - Add these lines
	if winAmounts == nil {
		return PlaceBetResultDisplay{}, fmt.Errorf("winAmounts is nil after generation")
	}

	winAmount, exists := winAmounts[selectedNumber]
	if !exists {
		logrus.Errorf("Selected number %s not found in winAmounts: %v", selectedNumber, winAmounts)
		return PlaceBetResultDisplay{}, fmt.Errorf("selected number %s not found in win amounts", selectedNumber)
	}

	// Random increment calculation
	randomIncrement := cryptoRandFloat() * 10 // Random between 0-10
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

	logrus.Infof("kpiBet payout: %.2f", kpiBet)

	logrus.Infof("KPI payout: %.2f", kpiPayout)

	logrus.Infof("sum currentRTPDay: %.2f", winAmountValue+kpiPayout)

	if kpiBet > 0 {
		currentRTPDay = ((kpiPayout + winAmountValue) / kpiBet) * 100
	} else {
		currentRTPDay = 0
		logrus.Warn("kpiBet is zero, cannot calculate RTP")
	}

	basket, err := s.db.CheckBasketLucky(ctx)

	if err != nil {
		return PlaceBetResultDisplay{}, fmt.Errorf("failed to fetch baskets: %w", err)
	}

	num := basket["balance_minor"].(pgtype.Numeric)

	f, _ := num.Float64Value()
	basketValue := f.Float64
	// basketValue := utils.ToFloat64(basket["amount"])

	logrus.Infof("Default RTP: %.2f", defaultRTP)
	logrus.Infof("Player RTP: %.2f", utils.ToFloat64(player["rtp"]))
	logrus.Infof("Global RTP: %.2f", utils.ToFloat64(player["rtp"])) // Assuming rtp_player is same
	logrus.Infof("Current RTP: %.2f", kpiRTP)
	logrus.Infof("Current RTP Day: %.2f", currentRTPDay)
	logrus.Infof("Player lost count: %d", playerLostCount)
	logrus.Infof("Basket value: %.2f", basketValue)
	logrus.Infof("Win amount: %.2f", winAmountValue)

	var crtp = math.Round(currentRTPDay*100) / 100

	logrus.Infof("Win amount RTP: %.2f", crtp)

	logrus.Infof("Win amount RTP: %.2f", (defaultRTP + adjustmentableRTP))
	// Win condition
	if winAmountValue > 0 && (defaultRTP+adjustmentableRTP) >= crtp && basketValue > winAmountValue {
		// Player wins
		resultMessage := fmt.Sprintf("Box %s wins. Numbers: %+v", selectedNumber, winAmounts)
		logrus.Info(resultMessage)

		// Update bet as win
		_, err := s.db.UpdateLuckyBetWin(ctx, resultMessage, "PAWABOX", reference, winAmountValue, "Win")
		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to update lucky bet win: %w", err)
		}

		// Calculate tax
		withholdTax := (withholding / 100) * winAmountValue
		taxDeductedAmount := winAmountValue - withholdTax

		// Update KPI payouts
		_, err = s.db.UpdateKPIPayouts(ctx, winAmountValue, withholdTax, 0)
		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to update KPI payouts: %w", err)
		}

		// Update win amounts with tax deducted values - SAFELY
		winAmounts[selectedNumber] = WinAmount{
			Value: taxDeductedAmount,
			Item:  FormatToMZN(taxDeductedAmount),
		}

		// Handle win logic
		err = s.win(ctx, playerID, playerPayout, playerTotalBets, winItem, withholdTax, taxDeductedAmount, winAmountValue, msisdn, reference)
		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to handle win: %w", err)
		}

		// Round amounts
		withholdTax = math.Round(withholdTax)
		taxDeductedAmount = math.Round(taxDeductedAmount)

		// Create win message
		message := s.createWinMessage(selectedNumber, winAmounts, playerFreeBet, reference, withholding, withholdTax)
		logrus.Infof("Player MSISDN: %s", msisdn)

		resultd, err := s.ResultDisplay(selectedNumber, winAmounts, playerFreeBet, reference)

		// Queue SMS
		err = s.sendsms(msisdn, message)

		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to insert SMS queue: %w", err)
		}

		// Update RTP
		_, err = s.db.UpdateHouseLucyNumberHouseCurrentRTP(ctx)
		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to update RTP: %w", err)
		}

		logrus.Infof("Player %s won: %.2f (tax: %.2f)", msisdn, taxDeductedAmount, withholdTax)

		var boxes map[string]WinAmount
		if err := json.Unmarshal([]byte(resultd), &boxes); err != nil {
			logrus.Errorf("Failed to unmarshal Boxes JSON: %v", err)
			return PlaceBetResultDisplay{}, err
		}
		mresult := PlaceBetResultDisplay{
			Boxes:         boxes,
			ResultStatus:  "Win",
			WinAmount:     winAmountValue,
			JackPot:       "False",
			GameID:        reference,
			SelectedBox:   selectedNumber,
			ResultMessage: message}

		return mresult, nil

	} else {
		// Player loses - SAFELY update
		winAmounts[selectedNumber] = WinAmount{
			Value: 0,
			Item:  "0",
		}

		// Handle loss
		err := s.lose(ctx, playerID, reference, msisdn, playerLostCount, playerTotalLosses, betAmount)
		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to handle loss: %w", err)
		}

		// Build loss message
		resultMessage := fmt.Sprintf("Box %s loses. Numbers: (%+v)", selectedNumber, winAmounts)
		logrus.Info(resultMessage)

		message := s.createLossMessage(selectedNumber, winAmounts, playerFreeBet, reference)
		logrus.Infof("Player MSISDN: %s", msisdn)

		resultd, err := s.ResultDisplay(selectedNumber, winAmounts, playerFreeBet, reference)

		// Queue SMS
		err = s.sendsms(msisdn, message)
		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to insert SMS queue: %w", err)
		}

		// Update bet as loss
		_, err = s.db.UpdateLuckyBet(ctx, resultMessage, reference)
		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to update lucky bet: %w", err)
		}

		// Record lost transaction
		_, err = s.db.InsertB2BWithdrawalB2B(ctx, reference, msisdn, 0, "Lost")
		if err != nil {
			return PlaceBetResultDisplay{}, fmt.Errorf("failed to insert B2B withdrawal: %w", err)
		}

		var boxes map[string]WinAmount
		if err := json.Unmarshal([]byte(resultd), &boxes); err != nil {
			logrus.Errorf("Failed to unmarshal Boxes JSON: %v", err)
			return PlaceBetResultDisplay{}, err
		}

		mresult := PlaceBetResultDisplay{
			Boxes:         boxes,
			ResultStatus:  "Loss",
			WinAmount:     0,
			JackPot:       "False",
			GameID:        reference,
			SelectedBox:   selectedNumber,
			ResultMessage: message,
		}

		logrus.Infof("Player %s lost bet: %.2f", msisdn, betAmount)

		// return struct + nil error
		return mresult, nil
	}
}

// GenerateWinAmounts generates unique win amounts for each box number
func (s *LuckyNumberService) GenerateWinAmounts(ctx context.Context, params GenerateWinAmountsParams) (map[string]WinAmount, error) {
	// Initialize random

	// Generate 7 unique random numbers between 1-7
	chosenNumbers := cryptoRandUniqueInts(1, 8, 7)
	numZeroBoxes := cryptoRandInt(0, 3) // 0–2
	// numZeroBoxes := cryptoRandIndex(3) + 1 // 1-3

	boxes := make(map[string]WinAmount)
	totalAssigned := 0.0

	// Get basket value
	basket, err := s.db.CheckBasketLucky(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check basket: %w", err)
	}

	num := basket["balance_minor"].(pgtype.Numeric)
	f, _ := num.Float64Value()
	basketValue := f.Float64

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
	numSelectedBoxes := cryptoRandInt(1, 2) // 0-2
	selectedBoxes := selectRandomBoxes(chosenNumbers, numSelectedBoxes)

	logrus.Infof("Selected boxes: %v", selectedBoxes)

	// Step 1: Create boxes for each chosen number
	for _, num := range chosenNumbers {
		numStr := fmt.Sprintf("%d", num)
		var winAmount float64

		if cryptoRandFloat() < 0.5 {
			// 50% chance for smaller wins
			winAmount = cryptoRandFloatRange(minWinAmount, minWinAmount*20)

		} else {
			// 50% chance for larger wins
			winAmount = cryptoRandFloatRange(minWinAmount, maxWinAmountCalc)
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
			if cryptoRandFloat() < 0.5 {
				specialWinAmount = cryptoRandFloat()*(minWinAmount*20-minWinAmount) + minWinAmount
				if cryptoRandFloat() < 0.5 {
					specialWinAmount = cryptoRandFloat()*(800-minWinAmount) + minWinAmount
				}
			} else {
				specialWinAmount = cryptoRandFloat()*(800-minWinAmount) + minWinAmount
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
			randomMinAmount := cryptoRandFloat()*(minWinAmount*1.2-minWinAmount) + minWinAmount
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
	forcedAmount *= cryptoRandFloat()*0.2 + 0.9 // ±10%
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
			margin := cryptoRandFloat()*0.8 + 0.1 // 0.1-0.9%
			targetRTP := (params.DefaultRTP + params.AdjustmentRTP) - margin
			maxAllowedPayout := (targetRTP/100)*kpiBet - utils.ToFloat64(params.KPI["payout"])

			if maxAllowedPayout > minWinAmount {
				amount = cryptoRandFloat()*(maxAllowedPayout-minWinAmount) + minWinAmount
			} else {
				randomPercentage := cryptoRandFloat()*0.39 + 0.6 // 0.6-0.99
				minRandom := params.BetAmount + ((minWinAmount - params.BetAmount) * randomPercentage)
				amount = cryptoRandFloat()*(minWinAmount-minRandom) + minRandom
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

	// Correct shuffle using CryptoShuffle
	CryptoShuffle(numbers)

	if count > len(numbers) {
		count = len(numbers)
	}
	return numbers[:count]
}
func selectRandomBoxes(numbers []int, count int) []int {
	if count >= len(numbers) {
		return numbers
	}

	// Shuffle the slice using a cryptographic RNG
	CryptoShuffle(numbers)

	return numbers[:count]
}

func selectRandomBox(numbers []int) int {
	return numbers[cryptoRandIndex(len(numbers))]
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

func (s *LuckyNumberService) createJackpotMessage(selectedNumber string, winAmounts map[string]WinAmount, freeBet int64, reference string, withholding float64, tax_deducted_amount, payout float64) string {
	var boxes []string
	for num, winAmount := range winAmounts {
		boxes = append(boxes, fmt.Sprintf("Box %s - %s", num, winAmount.Item))
	}
	sort.Strings(boxes)

	return fmt.Sprintf(utils.Texts["results"]["jackpot"],
		reference,
		winAmounts[selectedNumber].Item,
		FormatToMZN(tax_deducted_amount),
	)
}

func (s *LuckyNumberService) ResultDisplay(selectedNumber string, winAmounts map[string]WinAmount, freeBet int64, reference string) (string, error) {
	// Create a slice of keys to sort
	keys := make([]string, 0, len(winAmounts))
	for k := range winAmounts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build ordered map
	ordered := make(map[string]WinAmount, len(winAmounts))
	for _, k := range keys {
		ordered[k] = winAmounts[k]
	}

	// Marshal to JSON
	resultJSON, err := json.Marshal(ordered)
	if err != nil {
		return "", err
	}

	// Convert []byte to string
	return string(resultJSON), nil
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
	err = s.sendsms(msisdn, message)
	if err != nil {
		return fmt.Errorf("failed to insert failed SMS: %w", err)
	}

	log.Printf("Failed SMS queued for %s with reference: %s", msisdn, ref)
	return nil
}

var fastRng = mathrand.New(mathrand.NewSource(time.Now().UnixNano()))

func fastRandFloat() float64 {
	return fastRng.Float64()
}
func biasedHighFloat(min, max float64) float64 {
	// u ∈ [0,1)
	u := fastRandFloat()

	// Bias towards HIGH (smaller exponent = stronger bias)
	bias := 0.2 // 0.2 = very high bias, 0.5 = moderate
	u = math.Pow(u, bias)

	return min + u*(max-min)
}

// Utility function
func round(value float64) float64 {
	return float64(int(value + 0.5))
}

func (s *LuckyNumberService) PlaceBetSpin(
	session_id string, session_start string,
	player map[string]interface{},
	gameCatID, msisdn string,
	amount float64,
	channel, mode string) (SpinResponse, error) {
	// s.mu.Lock()
	// defer s.mu.Unlock()
	ctx := context.Background()
	gameID := "SPIN_" + s.randomString(10)
	symbols := []string{"0", "1", "2", "3"}
	//----------------------------------------------------
	// LOAD SETTINGS
	//----------------------------------------------------
	data, err := s.loadSpinData(ctx, gameCatID, msisdn)
	if err != nil {
		return SpinResponse{}, err
	}
	basket := data.Basket
	setting := data.Setting
	kpi := data.KPI
	game := data.Game
	// player := data.Player
	//----------------------------------------------------
	// EXTRACT PARAMS
	//----------------------------------------------------
	basketid := utils.ToString(basket["pool_id"])

	num := basket["balance_minor"].(pgtype.Numeric)

	f, _ := num.Float64Value()
	basketValue := f.Float64

	logrus.Infof("basketValue : %s", basketValue)
	defaultRTP := utils.ToFloat64(setting["default_rtp"])
	qadjustRTP := utils.ToFloat64(setting["adjustmentable_rtp"])
	minAdjustRTP := utils.ToFloat64(setting["adjustdynamic"])
	adjustRTP := biasedHighFloat(minAdjustRTP, qadjustRTP)
	minMul := utils.ToFloat64(setting["min_win_multipier"])
	maxMul := utils.ToFloat64(setting["max_win_multipier"])
	// minLoss := utils.ToInt64(setting["min_loss_count"])

	vig := utils.ToFloat64(setting["vig_percentage"])

	overload := utils.ToFloat64(setting["rtp_overload"])
	withholding := utils.ToFloat64(setting["withholding"])
	jackpotspin := utils.ToFloat64(setting["jackpot_percentage"])
	playerLost := utils.ToInt(player["lost_count"])
	playerPayout := utils.ToFloat64(player["payout"])
	playerBet := utils.ToFloat64(player["total_bets"])
	playerTotalBets := player["total_bets"].(float64)
	playerID := utils.ToInt64(player["player_id"])
	playerLostCount := int64(0)
	if lost, ok := player["lost_count"].(int32); ok {
		playerLostCount = int64(lost)
	} else if lost, ok := player["lost_count"].(int64); ok {
		playerLostCount = lost
	}
	playerRTP := 0.0
	if playerBet > 0 {
		playerRTP = (playerPayout / playerBet) * 100
	}
	gameExposure := utils.ToFloat64(game["max_exposure"])
	kpiPay := utils.ToFloat64(kpi["payout"])
	BetAmount := amount
	kpiBet := utils.ToFloat64(kpi["bet"]) + BetAmount

	globalRTP := defaultRTP + qadjustRTP
	//----------------------------------------------------
	// HELPER: FORCE LOSS
	//----------------------------------------------------
	hardLoss := func() (SpinResponse, error) {
		row := randomNonMatchingRow(symbols)
		err = s.db.UpdateLuckyBetAndLoss(ctx, utils.ToString(row), gameID, amount, playerID)
		if err != nil {
			return SpinResponse{}, fmt.Errorf("failed to handle loss: %w", err)
		}
		logrus.Info(row)
		return SpinResponse{
			Row:       row,
			Win:       false,
			WinAmount: 0,
			GameID:    gameID,
		}, nil
	}
	//----------------------------------------------------
	// TAX CALC
	//----------------------------------------------------
	calcTax := func(amount float64) (tax, net float64) {
		tax = (withholding / 100) * amount
		return tax, amount - tax
	}
	//----------------------------------------------------
	// RNG HELPERS
	//----------------------------------------------------
	forcedMatch := func() []string {
		return forcedMatchingRow(symbols)
	}
	p := playerLostCount
	logrus.Infof(utils.ToString(p))
	//----------------------------------------------------
	// UPDATE PLAYER BET + TAX FIRST
	//----------------------------------------------------
	// exciseTax := round(setting["excise_duty"].(float64) / 100 * BetAmount)
	if err := s.bet(ctx, gameID, playerID, playerTotalBets, BetAmount); err != nil {
		return SpinResponse{}, err
	}
	basket_value := BetAmount * (globalRTP / 100)
	houseValue := (setting["vig_percentage"].(float64) / 100) * BetAmount
	jackpotValue := (setting["jackpot_percentage"].(float64) / 100) * BetAmount

	var exciseTaxAmount = 0.00
	if setting["excise_duty"].(float64) > 0 {
		exciseTaxAmount = (setting["excise_duty"].(float64) / 100) * BetAmount
	}
	var withholdTaxJackpot = 0.00
	if setting["withholding"].(float64) > 0 {
		withholdTaxJackpot = (setting["withholding"].(float64) / 100) * jackpotValue
	}
	// batch async DB tasks
	gross := BetAmount

	// logrus.Infof("maxMul : %s", utils.ToString(basket_value))

	statenet := BetAmount - jackpotValue - exciseTaxAmount
	_ = s.db.CreateBet(ctx, basketid, basket_value, utils.ToInt64(player["player_id"]), withholdTaxJackpot,
		exciseTaxAmount, utils.ToInt64(session_id), session_start, gross, statenet, houseValue, jackpotValue, "0", BetAmount, "",
		gameID, "Pending", "normal", gameCatID, utils.ToString(game["name"]), fmt.Sprintf("%.2f added to the basket:- game id %s", basket_value, gameID), channel)

	//----------------------------------------------------
	// RTP CALC
	//----------------------------------------------------
	//----------------------------------------------------
	// Calculate min/max win
	minWin := BetAmount * minMul
	maxWin := math.Min(BetAmount*maxMul, gameExposure)

	// Apply basket cap (80%)

	logrus.Infof("maxMul : %s", basketValue)

	maxWin = math.Min(maxWin, basketValue*0.80)

	// Generate potential win amount
	winAmt := cryptoRandFloatRange(minWin, maxWin)
	// Calculate current RTP day
	currentRTPDay := 0.0
	if kpiBet > 0 {
		currentRTPDay = ((kpiPay + winAmt) / kpiBet) * 100
	}

	// Define RTP limits
	rtpLimit := defaultRTP + adjustRTP + jackpotspin
	tooHigh := currentRTPDay > rtpLimit || playerRTP > (rtpLimit+vig+overload)

	// Hard loss conditions
	minLossCount := cryptoRandIndex(int(setting["min_loss_count"].(float64)))

	// if forceWin {
	cherries_three := BetAmount * 50
	apple_three := BetAmount * 20
	oranges_three := BetAmount * 15
	grapes_three := BetAmount * 5
	cherries_two := BetAmount * 40
	apple_two := BetAmount * 10
	oranges_two := BetAmount * 5
	type payoutOption struct {
		amount float64
		match  int // 2 or 3 symbols match
		symbol int // 0=cherries, 1=apple, 2=oranges, 3=grapes
	}
	forcedPayouts := []payoutOption{
		{cherries_three, 3, 0},
		{apple_three, 3, 1},
		{oranges_three, 3, 2},
		{grapes_three, 3, 3},
		{cherries_two, 2, 0},
		{apple_two, 2, 1},
		{oranges_two, 2, 2},
	}

	logrus.Infof("playerLost : %s", playerLost)
	logrus.Infof("minLossCount : %s", minLossCount)

	if playerLost >= minLossCount {
		if maxWin < minWin {
			return hardLoss() // cannot afford a win
		}
		//----------------------------------------------------
		// 100% RANDOM FORCED WIN USING CRYPTO RNG
		// Also must respect game exposure and basket limits
		absoluteMax := maxWin // maxWin already includes exposure & basket caps
		// logrus.Infof("minLossCount : %s", minLossCount)
		if absoluteMax <= 0 {
			return hardLoss()
		}
		// ------------------------------------------------------------
		// FULL-RANDOM: forcedAmount anywhere between 0 and absoluteMax
		// ------------------------------------------------------------
		// Filter allowed payouts based on basket & absolute max
		allowedPayouts := make([]payoutOption, 0)

		for _, val := range forcedPayouts {
			if val.amount <= basketValue && val.amount <= absoluteMax {
				allowedPayouts = append(allowedPayouts, val)
			}
		}
		// No valid payouts → hard loss
		if len(allowedPayouts) == 0 {
			return hardLoss()
		}
		// Pick a random allowed payout
		idx := cryptoRandIndex(len(allowedPayouts))
		chosen := allowedPayouts[idx]
		forcedAmount := chosen.amount
		symbolIndex := chosen.symbol // <- now you know which symbol to force
		matchSymbol := chosen.match
		// Compute new RTP
		if kpiBet > 0 {
			currentRTPDay = ((kpiPay + forcedAmount) / kpiBet) * 100.0
		}
		logrus.Infof("kpiBet : %s", kpiBet)
		logrus.Infof("kpiPay : %s", kpiPay)
		// Compute remaining RTP headroom
		maxWinAllowed := maxAllowedWin((kpiPay + forcedAmount), kpiBet, rtpLimit)
		logrus.Infof("[FORCE-WIN COMPLETE] Forced currentRTPDay=%.2f, symbolIndex=%d, adjustable_rtp=%.2f, rtpLimit=%.2f, forcedAmount=%.2f, maxWinAllowed=%.2f",
			currentRTPDay, symbolIndex, adjustRTP, rtpLimit, forcedAmount, maxWinAllowed)
		//if RTP too high → try smaller payouts
		if currentRTPDay > rtpLimit {
			sorted := make([]payoutOption, len(allowedPayouts))
			copy(sorted, allowedPayouts)

			sort.Slice(sorted, func(i, j int) bool {
				return sorted[i].amount > sorted[j].amount // DESC
			})
			for _, p := range sorted {
				currentRTPDay = ((kpiPay + p.amount) / kpiBet) * 100.0
				logrus.Infof("[FORCE-WIN  currentRTPDay=%.2f, bet=%.2f, rtpLimit=%.2f, kpipay=%.2f, forcedAmount=%.2f, maxWinAllowed=%.2f",
					currentRTPDay, kpiBet, rtpLimit, kpiPay, p.amount, maxWinAllowed)
				if ((kpiPay+p.amount)/kpiBet)*100.0 <= rtpLimit {
					forcedAmount = p.amount
					symbolIndex = p.symbol
					matchSymbol = p.match
					currentRTPDay = ((kpiPay + forcedAmount) / kpiBet) * 100.0
					logrus.Infof("[currentRTPDay] n=%.2f", currentRTPDay)
					break
				}
			}
			// Still too high → hard loss
			if currentRTPDay > rtpLimit {
				logrus.Infof("[FORCE-WIN COMPLETE] Forced currentRTPDay=%.2f, symbolIndex=%d, adjustable_rtp=%.2f, rtpLimit=%.2f, forcedAmount=%.2f, maxWinAllowed=%.2f",
					currentRTPDay, symbolIndex, adjustRTP, rtpLimit, forcedAmount, maxWinAllowed)
				return hardLoss()
			}
		}
		// Final log
		// Check basket coverage
		if forcedAmount > basketValue || forcedAmount < 1 {
			return hardLoss()
		}
		// Assign final forced win
		amount := forcedAmount
		logrus.Infof("[FORCE-WIN COMPLETE] Forced win=%.2f, adjustable_rtp=%.2f, target_rtp=%.2f, basket=%.2f",
			amount, kpiPay, rtpLimit, amount)
		// amount = 100
		if basketValue > amount {
			tax, net := calcTax(amount)
			// Force a matching row (3 symbols match)
			row := forcedMatchFromLeft(symbols, symbolIndex, matchSymbol)
			logrus.Infof("minLossCount : %s", amount)
			logrus.Infof("minLossCount : %s", net)
			narrative := fmt.Sprintf("%.2f deducted from the basket:- game id %s", amount, gameID)
			// Record win without adjusting RTP
			err := s.db.CreateWin(ctx, utils.ToInt64(basketid), playerID, tax, net, utils.ToInt64(session_id), session_start, amount, utils.ToString(row), gameID, "won", "normal", gameCatID, utils.ToString(game["name"]), narrative, channel)
			if err != nil {
				logrus.Infof("playerLost : %s", err)

				return hardLoss()
			}
			// response
			return SpinResponse{
				Row:       row,
				Win:       true,
				WinAmount: net,
				GameID:    gameID,
			}, nil
		} else {
			return hardLoss()
		}
	} else {
		if winAmt > basketValue || tooHigh {
			return hardLoss()
		}
		// ------------------------------
		// NORMAL WIN (if allowed by RTP)
		// ------------------------------
		tax, net := calcTax(winAmt)
		row := forcedMatch() // matching row
		narrative := fmt.Sprintf("%.2f deducted from the basket:- game id %s", amount, gameID)
		s.db.CreateWin(ctx, utils.ToInt64(basketid), playerID, tax, net, utils.ToInt64(session_id), session_start, amount, utils.ToString(row), gameID, "won", "normal", gameCatID, utils.ToString(game["name"]), narrative, channel)
		return SpinResponse{
			Row:       row,
			Win:       true,
			WinAmount: winAmt,
			GameID:    gameID,
		}, nil
	}
}

func maxAllowedWin(kpiPay, kpiBet, rtpLimit float64) float64 {
	if kpiBet <= 0 {
		return 0
	}
	return ((rtpLimit / 100.0) * kpiBet) - kpiPay
}
func forcedMatchFromLeft(symbols []string, symbolIndex int, matchSymbols int) []string {
	row := make([]string, 3)
	switch matchSymbols {
	case 3:
		// All three same
		row[0], row[1], row[2] = symbols[symbolIndex], symbols[symbolIndex], symbols[symbolIndex]
	case 2:
		// First two same, last one different
		row[0], row[1] = symbols[symbolIndex], symbols[symbolIndex]
		for {
			r := cryptoRandIndex(len(symbols))
			if r != symbolIndex {
				row[2] = symbols[r]
				break
			}
		}
	default:
		// fully random
		for i := 0; i < 3; i++ {
			row[i] = symbols[cryptoRandIndex(len(symbols))]
		}
	}
	return row
}

func forcedMatchingRow(symbols []string) []string {
	s := symbols[cryptoRandIndex(len(symbols))]
	return []string{s, s, s}
}

func randomNonMatchingRow(symbols []string) []string {
	if len(symbols) < 2 {
		panic("need at least 2 symbols")
	}
	row := make([]string, 3)
	// Pick first symbol
	firstIdx := cryptoRandIndex(len(symbols))
	first := symbols[firstIdx]
	row[0] = first

	// Build allowed indices (everything except first)
	allowed := make([]string, 0, len(symbols)-1)
	for i, s := range symbols {
		if i != firstIdx {
			allowed = append(allowed, s)
		}
	}

	// Pick remaining symbols from allowed set (no retries)
	row[1] = allowed[cryptoRandIndex(len(allowed))]
	row[2] = allowed[cryptoRandIndex(len(allowed))]

	return row
}

// // SecureFloat returns random float64 in [min, max]
// func SecureFloat(min, max float64) (float64, error) {
// 	// random 63-bit integer
// 	nBig, err := rand.Int(rand.Reader, big.NewInt(1<<62))
// 	if err != nil {
// 		return 0, err
// 	}

// 	r := float64(nBig.Int64()) / float64(1<<62) // normalize to [0,1]
// 	return min + r*(max-min), nil
// }

// row[i] = symbols[cryptoRandIndex(len(symbols))]

// idx, _ := SecureInt(int64(len(symbols)))
// row[i] = symbols[idx]

// func forceLoss(resp *SpinResponse) (*SpinResponse, error) {
// 	resp.Win = false
// 	resp.WinAmount = 0
// 	return resp, nil
// }

// win records a win for a player

func (s *LuckyNumberService) winSpin(ctx context.Context, playerID int64, payout, bets float64, winItem string, withholdTax, taxDeductedAmount, amount float64, msisdn, reference string) error {
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
					_, err := s.db.InsertCustomerLogsPawaBoxKe(ctx, amountNew, "withdraw", utils.ToString(playerID), "customer withdrawal: spin&win", reference)
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

type SpinPrerequisites struct {
	Basket  map[string]interface{}
	Setting map[string]interface{}
	KPI     map[string]interface{}
	Game    map[string]interface{}
	Player  map[string]interface{}
}

func (s *LuckyNumberService) loadSpinData(ctx context.Context, gameCatID, msisdn string) (*SpinPrerequisites, error) {

	var (
		r     SpinPrerequisites
		wg    sync.WaitGroup
		errCh = make(chan error, 4)
	)
	wg.Add(4)
	go func() {
		defer wg.Done()
		v, err := s.db.CheckBasketLucky(ctx)
		if err != nil {
			errCh <- err
			return
		}
		r.Basket = v
	}()
	go func() {
		defer wg.Done()
		v, err := s.db.CheckSetting(ctx)
		if err != nil {
			errCh <- err
			return
		}
		r.Setting = v
	}()
	go func() {
		defer wg.Done()
		v, err := s.db.CheckSettingKPI(ctx)
		if err != nil {
			errCh <- err
			return
		}
		r.KPI = v
	}()
	go func() {
		defer wg.Done()
		v, err := s.db.CheckGamePlay(ctx, gameCatID)
		if err != nil {
			errCh <- err
			return
		}
		r.Game = v
	}()
	wg.Wait()
	close(errCh)

	for e := range errCh {
		return nil, e
	}
	return &r, nil
}
