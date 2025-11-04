package models

import "time"

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
