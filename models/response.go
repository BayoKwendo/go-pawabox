package models

import (
	"time"

	"gorm.io/gorm"
)

type H map[string]interface{}

type BaseResponse struct {
	Status        int         `json:"Status"`
	StatusCode    int         `json:"StatusCode"`
	StatusMessage interface{} `json:"StatusMessage"`
}

func NewSuccess(status, statusCode int, msg interface{}) H {
	return H{"Status": status, "StatusCode": statusCode, "StatusMessage": msg}
}

func NewSuccessWithData(status, statusCode int, data interface{}) H {
	return H{"Status": status, "StatusCode": statusCode, "StatusMessage": data}
}

func NewErrorResponse(status, statusCode int, msg interface{}) H {
	return H{"Status": status, "StatusCode": statusCode, "StatusMessage": msg}
}

type User struct {
	ID                     int64      `json:"id" db:"id"`
	PlayerID               string     `json:"player_id" db:"player_id"`
	Msisdn                 string     `json:"msisdn" db:"msisdn"`
	Carrier                *string    `json:"carrier,omitempty" db:"carrier"`
	Name                   *string    `json:"name,omitempty" db:"name"`
	Monetary               float64    `json:"monetary" db:"monetary"`
	Frequency              int        `json:"frequency" db:"frequency"`
	Recency                *int64     `json:"recency,omitempty" db:"recency"`
	LostCount              int        `json:"lost_count" db:"lost_count"`
	Payout                 float64    `json:"payout" db:"payout"`
	TotalLosses            float64    `json:"total_losses" db:"total_losses"`
	TotalBets              float64    `json:"total_bets" db:"total_bets"`
	LastTransactionTime    *time.Time `json:"last_transaction_time,omitempty" db:"last_transaction_time"`
	TenureDays             int64      `json:"tenure_days" db:"tenure_days"`
	LoyaltyPoint           string     `json:"loyalty_point" db:"loyalty_point"`
	FreeBet                float64    `json:"free_bet" db:"free_bet"`
	Bonus                  float64    `json:"bonus" db:"bonus"`
	BonusExpiry            *time.Time `json:"bonus_expiry,omitempty" db:"bonus_expiry"`
	FreebetExpiry          *time.Time `json:"freebet_expiry,omitempty" db:"freebet_expiry"`
	FreebetCount           int64      `json:"freebet_count" db:"freebet_count"`
	BonusTurnIntoRealMoney float64    `json:"bonus_turn_into_real_money" db:"bonus_turn_into_real_money"`
	FreeTurnIntoRealMoney  float64    `json:"free_turn_into_real_money" db:"free_turn_into_real_money"`
	LastStakeAmount        float64    `json:"last_stake_amount" db:"last_stake_amount"`
	TotalLossCount         float64    `json:"total_loss_count" db:"total_loss_count"`
	RtpPlayer              float64    `json:"rtp_player" db:"rtp_player"`
	Session                *string    `json:"session,omitempty" db:"session"`
	FreeBetCount           float64    `json:"free_bet_count" db:"free_bet_count"`
	IsFree                 string     `json:"is_free" db:"is_free"`
	Channel                string     `json:"channel" db:"channel"`
	DateCreated            time.Time  `json:"date_created" db:"date_created"`
	LastUpdatedOn          time.Time  `json:"last_updated_on" db:"last_updated_on"`
	JackpotAmount          float64    `json:"jackpot_amount" db:"jackpot_amount"`
}

type Symbol struct {
	ID     string `json:"id" gorm:"primaryKey"`
	Name   string `json:"name"`
	Weight int    `json:"weight"`
	Payout int    `json:"payout"`
	Image  string `json:"image"`
}

type GameConfig struct {
	Symbols      []Symbol `json:"symbols" gorm:"-"`
	DrumsCount   int      `json:"drums_count" gorm:"-"`
	MinStake     int      `json:"min_stake" gorm:"-"`
	MaxStake     int      `json:"max_stake" gorm:"-"`
	QuickStakes  []int    `json:"quick_stakes" gorm:"-"`
	MaxAutoSpins int      `json:"max_auto_spins" gorm:"-"`
	DefaultStake int      `json:"default_stake" gorm:"-"`
}

type SpinRequest struct {
	Stake     int    `json:"stake"`
	AutoSpin  bool   `json:"auto_spin"`
	AutoSpins int    `json:"auto_spins"`
	StopRules []bool `json:"stop_rules"`
	PlayerID  string `json:"-"`
}

type Position struct {
	Drum int `json:"drum"`
	Row  int `json:"row"`
}

type WinLine struct {
	gorm.Model
	SpinResultID uint   `json:"-"`
	LineNumber   int    `json:"line_number"`
	Symbol       string `json:"symbol"`
	Count        int    `json:"count"`
	Payout       int    `json:"payout"`
	Positions    []byte `json:"-" gorm:"type:jsonb"` // Store positions as JSON
}

type SpinResult struct {
	gorm.Model
	PlayerID  string    `json:"player_id" gorm:"index"`
	SpinID    string    `json:"spin_id" gorm:"uniqueIndex"`
	Drums     []byte    `json:"-" gorm:"type:jsonb"` // Store drums as JSON
	WinAmount int       `json:"win_amount"`
	Balance   int       `json:"balance"`
	Stake     int       `json:"stake"`
	IsWin     bool      `json:"is_win"`
	Timestamp time.Time `json:"timestamp"`
	WinLines  []WinLine `json:"win_lines" gorm:"foreignKey:SpinResultID"`
}

type GameSession struct {
	gorm.Model
	PlayerID      string     `json:"player_id" gorm:"uniqueIndex"`
	Balance       int        `json:"balance"`
	Stake         int        `json:"stake"`
	AutoSpinsLeft int        `json:"auto_spins_left"`
	IsAutoMode    bool       `json:"is_auto_mode"`
	TotalSpins    int        `json:"total_spins"`
	TotalWins     int        `json:"total_wins"`
	TotalWagered  int        `json:"total_wagered"`
	TotalWon      int        `json:"total_won"`
	LastSpinTime  *time.Time `json:"last_spin_time"`
}
