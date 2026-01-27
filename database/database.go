package database

import (
	"context"
	"errors"
	"fiberapp/utils"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// VerificationCode represents one row from verification
type VerificationCode struct {
	ID      int64
	Msisdn  string
	Code    string
	Expired int64 // unix seconds
	Created int64 // unix seconds
	Status  int
}

type Config struct {
	Production struct {
		Postgres struct {
			Connection struct {
				Host     string `yaml:"host"`
				User     string `yaml:"username"`
				Password string `yaml:"password"`
				DBName   string `yaml:"database"`
				Port     int    `yaml:"port"`
			} `yaml:"connection"`
		} `yaml:"postgres"`
	} `yaml:"production"`
}

var (
	// Global pool instance - renamed from DB to avoid conflict
	globalPool *pgxpool.Pool
	dbOnce     sync.Once
	dbMux      sync.Mutex
	isClosed   bool
)

// Database struct to hold the connection pool
type Database struct {
	pool *pgxpool.Pool
}

// NewDatabase creates a new Database instance using the global pool
func NewDatabase() *Database {
	if globalPool == nil {
		log.Fatal("Database not initialized. Call ConnectPostgres first.")
	}
	return &Database{pool: globalPool}
}

// NewDatabaseWithPool creates a new Database instance with a custom pool
func NewDatabaseWithPool(pool *pgxpool.Pool) *Database {
	return &Database{pool: pool}
}

// Load YAML config
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf(":%s", err)
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Printf("Unmarshal error: %v", err)
		return nil, err
	}

	// Log the parsed config to verify
	// configJSON, _ := json.MarshalIndent(cfg, "", "  ")
	// log.Printf("Parsed config: %s", string(configJSON))

	return &cfg, nil
}

// Build DSN
func dsnFromConfig(cfg *Config) string {

	// log.Println("Raw YAML content:\n%s", cfg)
	conn := cfg.Production.Postgres.Connection

	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable&pool_max_conns=50&pool_min_conns=5",
		conn.User, conn.Password, conn.Host, conn.Port, conn.DBName)
}

// ConnectPostgres initializes the global pool once
func ConnectPostgres(configPath string) error {
	var connErr error
	dbOnce.Do(func() {
		cfg, err := loadConfig(configPath)
		if err != nil {
			connErr = err
			return
		}
		dsn := dsnFromConfig(cfg)

		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		// Parse DSN into config to customize pool settings
		poolConfig, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			connErr = fmt.Errorf("failed to parse DSN: %w", err)
			return
		}

		// OPTIMIZE CONNECTION POOL SETTINGS
		poolConfig.MaxConns = 100                  // Default is 4 - too low for web apps!
		poolConfig.MinConns = 5                    // Keep some connections ready
		poolConfig.MaxConnLifetime = 1 * time.Hour // Recycle connections periodically
		poolConfig.MaxConnIdleTime = 30 * time.Minute
		poolConfig.HealthCheckPeriod = 1 * time.Minute

		// Configure connection timeouts
		poolConfig.ConnConfig.ConnectTimeout = 10 * time.Second
		poolConfig.ConnConfig.RuntimeParams["statement_timeout"] = "10000" // 10 seconds

		log.Printf("🔄 Initializing database pool with %d max connections", poolConfig.MaxConns)

		pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			connErr = fmt.Errorf("failed to create connection pool: %w", err)
			return
		}

		if err := pool.Ping(ctx); err != nil {
			connErr = fmt.Errorf("failed to ping database: %w", err)
			pool.Close()
			return
		}

		globalPool = pool
		// log.Println("✅ PostgreSQL connection pool established with optimized settings")

		// Log initial pool stats
		stats := pool.Stat()
		log.Printf("📊 Initial Pool Stats - Max: %d, Total: %d, Idle: %d",
			poolConfig.MaxConns, stats.TotalConns(), stats.IdleConns())
	})
	return connErr
}

// GetPool returns the global connection pool
func GetPool() *pgxpool.Pool {
	return globalPool
}

// Acquire connection safely from global pool
func Acquire() (*pgxpool.Conn, error) {
	if globalPool == nil {
		return nil, errors.New("database not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return globalPool.Acquire(ctx)
}

// Release connection
func Release(conn *pgxpool.Conn) {
	if conn != nil {
		conn.Release()
	}
}

// Close DB pool
func Close() {
	dbMux.Lock()
	defer dbMux.Unlock()

	if !isClosed && globalPool != nil {
		globalPool.Close()
		isClosed = true
		log.Println("✅ PostgreSQL pool closed")
	}
}

// Helper function to scan row to map
func (db *Database) scanRowsToSingleMap(rows pgx.Rows) (map[string]interface{}, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error reading rows: %w", err)
		}
		return nil, nil // No rows
	}

	values, err := rows.Values()
	if err != nil {
		return nil, fmt.Errorf("failed to get row values: %w", err)
	}

	fieldDescriptions := rows.FieldDescriptions()
	result := make(map[string]interface{})
	for i, fd := range fieldDescriptions {
		result[string(fd.Name)] = values[i]
	}

	log.Printf("Acquiring connection for : %v", result)

	return result, nil
}

// Helper function to scan rows to map slice
func (db *Database) scanRowsToMap(rows pgx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()

	var results []map[string]interface{}
	fieldDescriptions := rows.FieldDescriptions()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("failed to get row values: %w", err)
		}

		result := make(map[string]interface{})
		for i, fd := range fieldDescriptions {
			result[string(fd.Name)] = values[i]
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// Database methods implementation...

// CheckUserAttempted checks if user exists in Attempted_Players table
func (db *Database) CheckUserAttempted(ctx context.Context, msisdn string) (map[string]interface{}, error) {
	query := `SELECT * FROM "Attempted_Players" WHERE new_msisdn = $1 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, msisdn)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToSingleMap(rows)
}

func (db *Database) CheckSelfExclusion(ctx context.Context, msisdn string) (map[string]interface{}, error) {
	query := `SELECT * FROM self_exlusion_request WHERE msisdn = $1  AND status = 'pending' order by id DESC LIMIT 1`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, msisdn)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToSingleMap(rows)
}
func (db *Database) CheckPromoCode(ctx context.Context, promo string) (map[string]interface{}, error) {

	query := `SELECT * FROM "promocode" WHERE promocode = $1 AND expire = 'NO'`

	logrus.Infof("promo already : %s", promo)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, promo)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToSingleMap(rows)
}

func (db *Database) CheckTransaction(ctx context.Context, transactionID string) (map[string]interface{}, error) {

	query := `SELECT * FROM "deposit_requests" WHERE transaction_id = $1 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, transactionID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToSingleMap(rows)
}

func (db *Database) UpdateUserAviatorBalInfoLucky(ctx context.Context, amount float64, msisdn, name string) (int64, error) {
	query := `UPDATE "Player" 
              SET name = $1,
				  monetary = monetary + $2,
				  balance = balance + $3
              WHERE msisdn = $4`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, name, amount, amount, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update user %s: %w", msisdn, err)
	}

	return result.RowsAffected(), nil
}

func (db *Database) UpdateUserInfo(ctx context.Context, msisdn, name string) (int64, error) {
	query := `UPDATE "Player" 
              SET name = $1
              WHERE msisdn = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, name, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update user %s: %w", msisdn, err)
	}

	return result.RowsAffected(), nil
}

func (db *Database) UpdateUserMsisdn(ctx context.Context, msisdn, newmsisdn string) (int64, error) {
	query := `UPDATE "Player" 
              SET msisdn = $1
              WHERE msisdn = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, newmsisdn, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update user %s: %w", msisdn, err)
	}

	return result.RowsAffected(), nil
}

func (db *Database) UpdatePlayerSelf(ctx context.Context, msisdn string, hrs string) error {
	query := `UPDATE "Player"
						SET
							self_exclusion = 'YES',
							self_exclusion_expiry = NOW() + ($1 * INTERVAL '1 hour')
						WHERE msisdn = $2;`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}

	defer conn.Release()
	_, err = conn.Exec(ctx, query, hrs, msisdn)
	if err != nil {
		return fmt.Errorf("failed to update user %s: %w", msisdn, err)
	}

	return nil
}

func (db *Database) UpdateSelfExclusion(ctx context.Context, msisdn string) error {
	query := `UPDATE self_exlusion_request 
              SET status = 'processed'
              WHERE msisdn = $1 AND status = 'pending'`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, query, msisdn)
	if err != nil {
		return fmt.Errorf("failed to update user %s: %w", msisdn, err)
	}

	return nil
}

func (db *Database) UpdateUserWinStatus(ctx context.Context, msisdn, show_win string) (int64, error) {
	query := `UPDATE "Player" 
              SET show_win = $1
              WHERE msisdn = $2`
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()
	result, err := conn.Exec(ctx, query, show_win, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update user %s: %w", msisdn, err)
	}
	return result.RowsAffected(), nil
}

func (db *Database) UpdateUserProfilePic(ctx context.Context, msisdn, filename string) (int64, error) {
	query := `UPDATE "Player" 
              SET profile_url = $1
              WHERE msisdn = $2`
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()
	result, err := conn.Exec(ctx, query, filename, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update user %s: %w", msisdn, err)
	}
	return result.RowsAffected(), nil
}

func (db *Database) DeleteUserInfo(ctx context.Context, msisdn string) (int64, error) {
	query := `UPDATE "Player" 
              SET active_status = 'inactive'
              WHERE msisdn = $1`
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()
	result, err := conn.Exec(ctx, query, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update user %s: %w", msisdn, err)
	}
	return result.RowsAffected(), nil
}
func (db *Database) CheckDepositRequestLucky(ctx context.Context, reference string) (map[string]interface{}, error) {
	query := `SELECT * FROM "deposit_requests" 
              WHERE reference = $1 
              AND transaction_id IS NULL 
              AND status IS NULL 
              `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, reference)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToSingleMap(rows)
}

// CheckUser checks if user exists in Player table
func (db *Database) CheckUser(ctx context.Context, msisdn string) (map[string]interface{}, error) {

	query := `SELECT * FROM "Player" WHERE msisdn = $1 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, msisdn)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// bet History
func (db *Database) CheckHistory(ctx context.Context, msisdn string, startDate, endDate *string) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	args = append(args, msisdn) // $1 for msisdn

	// Log for debugging

	if startDate != nil && endDate != nil {
		logrus.Infof("GetGames request: %+v", startDate)
		// Filter by date range
		query = `SELECT * 
		         FROM "Bets" 
		         WHERE msisdn = $1 
		           AND date_created BETWEEN $2 AND $3
		         ORDER BY id DESC LIMIT 100`
		args = append(args, *startDate, *endDate) // $2, $3
	} else {
		// No date filter
		query = `SELECT * 
		         FROM "Bets" 
		         WHERE msisdn = $1 
		         ORDER BY id DESC LIMIT 10`
	}

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}

// bet History
func (db *Database) CheckGameHistory(ctx context.Context, msisdn string, startDate, endDate *string, offset string, page_size string) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	args = append(args, msisdn) // $1 for msisdn

	
	// Log for debugging
	if startDate != nil && endDate != nil {
		logrus.Infof("GetGames request: %+v", startDate)
		// Filter by date range
		query = `SELECT c.*, p.msisdn  
					FROM "CustomerLogs" c 
					INNER JOIN "Player" p ON c.customer_id = p.id::text  
					WHERE p.msisdn = $1 
					AND c.date_created BETWEEN $2 AND $3
					ORDER BY c.id DESC LIMIT $5 OFFSET $4;`

		logrus.Infof("GetGames request: %+v", offset)
		logrus.Infof("GetGames request: %+v", page_size)

		args = append(args, *startDate, *endDate, offset, page_size) // $2, $3
	} else {
		// No date filter
		query = `SELECT 
					c.*, 
					p.msisdn  
				FROM "CustomerLogs" c 
				INNER JOIN "Player" p ON c.customer_id = p.id::text  
				WHERE p.msisdn = $1 
				ORDER BY c.id DESC 
				LIMIT $2 OFFSET $3;`
			logrus.Infof("GetGames request: %+v", query)
		logrus.Infof("GetGames request: %+v", page_size)
		logrus.Infof("GetGames request: %+v", offset)

			args = append(args, offset, page_size) // $2, $3

	}

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}



func (db *Database) CheckWithdrawal(ctx context.Context, msisdn string, startDate, endDate *string) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	args = append(args, msisdn) // $1 for msisdn

	// Log for debugging

	if startDate != nil && endDate != nil {
		logrus.Infof("GetGames request: %+v", startDate)
		// Filter by date range
		query = `SELECT * 
		         FROM "withdrawals" 
		         WHERE msisdn = $1 
		           AND date_created BETWEEN $2 AND $3
		         ORDER BY id DESC LIMIT 100`
		args = append(args, *startDate, *endDate) // $2, $3
	} else {
		// No date filter
		query = `SELECT * 
		         FROM "withdrawals" 
		         WHERE msisdn = $1 
		         ORDER BY id DESC LIMIT 10`
	}

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}

func (db *Database) GetWinners(ctx context.Context) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	// No date filter
	query = `SELECT DISTINCT ON (msisdn) *
		FROM withdrawals 
		ORDER BY msisdn, id DESC 
		LIMIT 10;`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}

func (db *Database) GetOnlineUsers(ctx context.Context) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	// No date filter
	query = `SELECT 
				COUNT(DISTINCT c.customer_id) AS online_users
			FROM "CustomerLogs" c
			INNER JOIN "Player" p 
				ON c.customer_id = p.id::text
			WHERE  
				c.date_created >= NOW() - INTERVAL '1 hour';`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}

func (db *Database) CheckDeposits(ctx context.Context, msisdn string, startDate, endDate *string) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	args = append(args, msisdn) // $1 for msisdn

	// Log for debugging

	if startDate != nil && endDate != nil {
		logrus.Infof("GetGames request: %+v", startDate)
		// Filter by date range
		query = `SELECT * 
		         FROM "deposit" 
		         WHERE msisdn = $1 
		           AND date_created BETWEEN $2 AND $3
		         ORDER BY id DESC LIMIT 100`
		args = append(args, *startDate, *endDate) // $2, $3
	} else {
		// No date filter
		query = `SELECT * 
		         FROM "deposit" 
		         WHERE msisdn = $1 
		         ORDER BY id DESC LIMIT 10`
	}

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}

// CheckBets gets user's bets
func (db *Database) CheckBets(ctx context.Context, msisdn string) ([]map[string]interface{}, error) {
	query := `SELECT * FROM "Bets" WHERE msisdn = $1 ORDER BY id DESC `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, msisdn)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}

// CheckBettoBet checks recent bets within 1 minute
func (db *Database) CheckBettoBet(ctx context.Context, msisdn string) ([]map[string]interface{}, error) {
	query := `SELECT * FROM "Bets" WHERE msisdn = $1 AND date_created >= NOW() - INTERVAL '1 minute' ORDER BY id DESC `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, msisdn)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}

// CheckJackpotWinnerKitty gets recent jackpot winners
func (db *Database) CheckJackpotWinnerKitty(ctx context.Context, msisdn string) ([]map[string]interface{}, error) {
	query := `SELECT msisdn FROM "jackpot_winners" ORDER BY id DESC LIMIT 3`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}

// UpdateKPI updates or inserts KPI record
func (db *Database) UpdateKPI(ctx context.Context) (int64, error) {
	checkQuery := `SELECT id FROM "kpi" WHERE DATE(created_on) = CURRENT_DATE`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var existingID int64
	err = tx.QueryRow(ctx, checkQuery).Scan(&existingID)

	if errors.Is(err, pgx.ErrNoRows) {
		insertQuery := `INSERT INTO "kpi" (date, handle, payout, ggr) 
					   SELECT CURRENT_DATE, 0, 0, 0 FROM "HouseIncome"`
		result, err := tx.Exec(ctx, insertQuery)
		if err != nil {
			return 0, fmt.Errorf("failed to insert kpi: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("failed to commit transaction: %w", err)
		}

		return result.RowsAffected(), nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to check existing kpi: %w", err)
	}

	return 0, nil
}

// UpdateJackpotKit updates jackpot kitty
func (db *Database) UpdateJackpotKit(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "jackpot_kitty"
			 SET kitty = kitty + ($1 * (pct_slice / 100)),
				 pct_to_target = ((kitty + ($1 * (pct_slice / 100))) / cost) * 100 
			 WHERE LENGTH(name_init) = 0`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		return 0, fmt.Errorf("failed to update jackpot kitty: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateJackpotKitNameInit updates jackpot kitty with name_init
func (db *Database) UpdateJackpotKitNameInit(ctx context.Context, mvalue float64, nameInit string) (int64, error) {
	query := `UPDATE "jackpot_kitty"
			 SET kitty = kitty + ($1 * (pct_slice / 100)),
				 pct_to_target = ((kitty + ($1 * (pct_slice / 100))) / cost) * 100 
			 WHERE LENGTH(name_init) > 0 AND name_init = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, nameInit)
	if err != nil {
		return 0, fmt.Errorf("failed to update jackpot kitty with name_init: %w", err)
	}

	return result.RowsAffected(), nil
}

func (db *Database) UpdateJackpotKity(ctx context.Context, id int) (int64, error) {
	query := `UPDATE "jackpot_kitty"
			 SET is_locked = 0 ,kitty = kitty-cost, win_count = win_count+ 1
			 WHERE id = $1`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update jackpot kitty with name_init: %w", err)
	}

	return result.RowsAffected(), nil
}

func (db *Database) UpdatePlayerRestLossJackpot(ctx context.Context, cost float64, id int) (int64, error) {
	query := `UPDATE "Player"
			 SET jackpot_amount = jackpot_amount + $1, lost_count = 0
			 WHERE id = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, cost, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update jackpot kitty with name_init: %w", err)
	}

	return result.RowsAffected(), nil
}

func (db *Database) UpdateJackpotKitUpdate(ctx context.Context, id int) (int64, error) {
	query := `UPDATE "jackpot_kitty"
			 SET is_locked = 1 
			 WHERE id = $1`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update jackpot kitty with name_init: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateKPIHandle updates KPI handle
func (db *Database) UpdateKPIHandle(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "kpi"
             SET bet_count = bet_count + 1,
                 bet = bet + $1,
                 rtp = ((payout / CASE WHEN bet + $1 = 0 THEN 1 ELSE bet + $1 END) * 100)
             WHERE DATE(created_on) = CURRENT_DATE`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Only pass mvalue once since all placeholders are $1
	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		return 0, fmt.Errorf("failed to update kpi handle: %w", err)
	}

	return result.RowsAffected(), nil
}

// CheckSettingKPI gets KPI settings
func (db *Database) CheckSettingKPI(ctx context.Context) (map[string]interface{}, error) {
	query := `SELECT rtp, payout, bet FROM "kpi" WHERE DATE(created_on) = CURRENT_DATE `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// UpdateKPIPayouts updates KPI payouts
func (db *Database) UpdateKPIPayouts(ctx context.Context, mvalue, withTaxAmount, exciseTaxAmount float64) (int64, error) {
	query := `UPDATE "kpi"  
			 SET withholding_tax_amount = withholding_tax_amount + $1, 
				 excise_duty_tax_amount = excise_duty_tax_amount + $2, 
				 rtp = (((payout + $3) / CASE WHEN bet = 0 THEN 1 ELSE bet END) * 100), 
				 ggr = handle - (payout + $4), 
				 payout = payout + $5 
			 WHERE DATE(created_on) = CURRENT_DATE`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, withTaxAmount, exciseTaxAmount, mvalue, mvalue, mvalue)
	if err != nil {
		return 0, fmt.Errorf("failed to update kpi payouts: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateKPIPayouts updates KPI payouts
func (db *Database) UpdateKPIPayoutSPIN(ctx context.Context, exciseTaxAmount float64) (int64, error) {
	query := `UPDATE "kpi"  
			 SET 
				 excise_duty_tax_amount = excise_duty_tax_amount + $1, 
				 rtp = (((payout) / CASE WHEN bet = 0 THEN 1 ELSE bet END) * 100), 
				 ggr = handle - (payout)
			 WHERE DATE(created_on) = CURRENT_DATE`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, exciseTaxAmount)
	if err != nil {
		return 0, fmt.Errorf("failed to update kpi payouts: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateKPIRTP updates KPI RTP
func (db *Database) UpdateKPIRTP(ctx context.Context) (int64, error) {
	query := `UPDATE "kpi" 
			 SET rtp = ((payout / CASE WHEN bet = 0 THEN 1 ELSE bet END) * 100) 
			 WHERE DATE(created_on) = CURRENT_DATE`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to update kpi rtp: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateKPIVIG updates KPI VIG
func (db *Database) UpdateKPIVIG(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "kpi" SET vig = vig + $1 WHERE DATE(created_on) = CURRENT_DATE`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		return 0, fmt.Errorf("failed to update kpi vig: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateKPIDeposit updates KPI deposit
func (db *Database) UpdateKPIDeposit(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "kpi" 
			 SET handle = handle + $1, 
				 ggr = handle - payout 
			 WHERE DATE(created_on) = CURRENT_DATE`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		return 0, fmt.Errorf("failed to update kpi deposit: %w", err)
	}

	return result.RowsAffected(), nil
}

// CheckGames gets active USSD games
func (db *Database) CheckGames(ctx context.Context, category string) ([]map[string]interface{}, error) {
	baseQuery := `SELECT id, name, title, category, name_init, description, bet_amount, boxes, max_win
                  FROM "Games"
                  WHERE status = 'active'`

	var args []interface{}
	if category != "" && category != "all" {
		baseQuery += " AND category = $1"
		args = append(args, category)
	}

	baseQuery += ` ORDER BY CASE id 
                 WHEN 10 THEN 1
                 WHEN 17 THEN 2
                 WHEN 16 THEN 3 
                 WHEN 8 THEN 4
                 WHEN 9 THEN 5
                 WHEN 12 THEN 6
                 WHEN 13 THEN 7
                 WHEN 11 THEN 8
                 WHEN 14 THEN 9
                 WHEN 15 THEN 10
                 ELSE 11
             END`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, baseQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToMap(rows)
}

// CheckGameONE gets a specific game by ID
func (db *Database) CheckGameONE(ctx context.Context, catID string) (map[string]interface{}, error) {
	query := `SELECT * FROM "Games" WHERE id = $1 AND status = 'active' `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, catID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// CheckGamePlay gets a game by ID
func (db *Database) CheckGamePlay(ctx context.Context, catID string) (map[string]interface{}, error) {
	query := `SELECT * FROM "Games" WHERE id = $1 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, catID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// CheckSetting gets settings
func (db *Database) CheckSetting(ctx context.Context) (map[string]interface{}, error) {
	query := `SELECT * FROM "PawaBox_KeSettings" `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)
}

// UpdateUserLucky updates user lucky count
func (db *Database) UpdateUserLucky(ctx context.Context, msisdn string) (int64, error) {
	query := `UPDATE "Player" 
			 SET free_bet_count = free_bet_count + 1,  
				 freebet_count = freebet_count + 1, 
				 free_bet = free_bet - 1 
			 WHERE msisdn = $1 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update user lucky: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateUserLuckyFree updates user free status
func (db *Database) UpdateUserLuckyFree(ctx context.Context, msisdn string) (int64, error) {
	query := `UPDATE "Player" SET is_free = 'NO' WHERE msisdn = $1 RETURNING id`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update user free status: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateUser updates user field
func (db *Database) UpdateUser(ctx context.Context, name string, mvalue interface{}, id int64) (int64, error) {
	// Use parameterized query to prevent SQL injection
	query := fmt.Sprintf(`UPDATE "Player" SET %s = $1 WHERE id = $2 `, name)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update user: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateUserRTP updates user RTP
func (db *Database) UpdateUserRTP(ctx context.Context, amount float64, id int64) (int64, error) {
	query := `UPDATE "Player" 
			 SET is_free = 'NO', balance = balance - $1, rtp_player = (payout / CASE WHEN total_bets = 0 THEN 1 ELSE total_bets END) * 100 
			 WHERE id = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, amount, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update user rtp: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateUserLossCount updates user loss count
func (db *Database) UpdateUserLossCount(ctx context.Context, mvalue float64, id int64) (int64, error) {
	query := `UPDATE "Player" 
			 SET lost_count = lost_count + 1,
				 total_loss_count = total_loss_count + 1, 
				 rtp_player = (payout / CASE WHEN total_bets = 0 THEN 1 ELSE total_bets END) * 100,
				 total_losses = total_losses + $1 
			 WHERE id = $2 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update user loss count: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateUserBet updates user bet information
func (db *Database) UpdateUserBet(ctx context.Context, mvalue float64, id int64) (int64, error) {
	query := `UPDATE "Player" 
			 SET monetary = monetary + $1, 
				 frequency = frequency + 1, 
				 last_transaction_time = NOW(),
				 total_bets = total_bets + $2,
				 recency = EXTRACT(DAY FROM (NOW() - last_transaction_time)),
				 last_stake_amount = $3 
			 WHERE id = $4`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, mvalue, mvalue, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update user bet: %w", err)
	}

	return result.RowsAffected(), nil
}

// CreateBet creates a new bet
func (db *Database) CreateBet(ctx context.Context, msisdn, selectedChoice string, amount float64, result, reference, betStatus, betType, gameCatID, gameName, channel string) (int64, error) {
	query := `INSERT INTO "Bets" 
			 (game_cat_id, game_name,channel, bet_type, result_status, results, reference, amount, msisdn, selected_number) 
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,$10)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	resultExec, err := conn.Exec(ctx, query, gameCatID, gameName, channel, betType, betStatus, result, reference, amount, msisdn, selectedChoice)
	if err != nil {
		return 0, fmt.Errorf("failed to create bet: %w", err)
	}

	return resultExec.RowsAffected(), nil
}

// InsertVerification inserts a new verification code
func (db *Database) InsertVerification(ctx context.Context, msisdn, code string, expired int64, created int64) (int64, error) {
	query := `
		INSERT INTO verification 
		(msisdn, code, expired, created)
		VALUES ($1, $2, $3, $4)
	`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	resultExec, err := conn.Exec(ctx, query, msisdn, code, expired, created)
	if err != nil {
		return 0, fmt.Errorf("failed to insert verification code: %w", err)
	}

	return resultExec.RowsAffected(), nil
}

// RequestSelfExlusion inserts a new verification code
func (db *Database) RequestSelfExlusion(ctx context.Context, msisdn string, hrs int) (int64, error) {
	query := `
		INSERT INTO self_exlusion_request 
		(msisdn, value_hrs)
		VALUES ($1, $2)
	`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	resultExec, err := conn.Exec(ctx, query, msisdn, hrs)
	if err != nil {
		return 0, fmt.Errorf("failed to insert self_exlusion_request code: %w", err)
	}

	return resultExec.RowsAffected(), nil
}

// UpdateLuckyBet updates bet result
func (db *Database) UpdateLuckyBet(ctx context.Context, result, game, reference, betStatus string) (int64, error) {
	query := `UPDATE "Bets" 
			 SET result_status = $1, 
				 status = 'processed', 
				 results = $2 ,
				 game = $3
			 WHERE reference = $4 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	resultExec, err := conn.Exec(ctx, query, betStatus, result, game, reference)
	if err != nil {
		return 0, fmt.Errorf("failed to update lucky bet: %w", err)
	}

	return resultExec.RowsAffected(), nil
}

// UpdateLuckyBetWin updates bet with win amount
func (db *Database) UpdateLuckyBetWin(ctx context.Context, result, game, reference string, winAmount float64, betStatus string) (int64, error) {
	query := `UPDATE "Bets" 
			 SET result_status = $1, 
				 status = 'processed', 
				 win_amount = $2, 
				 results = $3,
				 game=$4
			 WHERE reference = $5 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	resultExec, err := conn.Exec(ctx, query, betStatus, winAmount, result, game, reference)
	if err != nil {
		return 0, fmt.Errorf("failed to update lucky bet win: %w", err)
	}

	return resultExec.RowsAffected(), nil
}

// CreateUser creates a new user
func (db *Database) CreateUser(ctx context.Context, carrier, msisdn string, name string, my_promocode string, promocode string) (int64, error) {
	query := `INSERT INTO "Player" (carrier, msisdn, name, promocode, my_promocode) VALUES ($1, $2, $3, $4, $5)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, carrier, msisdn, name, promocode, my_promocode)
	if err != nil {
		return 0, fmt.Errorf("failed to create user: %w", err)
	}

	return result.RowsAffected(), nil
}

func (db *Database) CreatePromo(ctx context.Context, msisdn string, promocode string) (int64, error) {
	query := `INSERT INTO "promocode" (promocode, msisdn) VALUES ($1, $2)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, promocode, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to promocode user: %w", err)
	}

	return result.RowsAffected(), nil
}

// CreateUserAttempted creates a new attempted user
func (db *Database) CreateUserAttempted(ctx context.Context, msisdn string, new_msisdn string) (int64, error) {
	query := `INSERT INTO "Attempted_Players" ( msisdn, new_msisdn) VALUES ($1,$2) 
	ON CONFLICT DO NOTHING`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, msisdn, new_msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to create attempted user: %w", err)
	}

	return result.RowsAffected(), nil
}

// DeleteUserAttempted deletes attempted user
func (db *Database) DeleteUserAttempted(ctx context.Context, msisdn string) (int64, error) {
	query := `DELETE FROM "Attempted_Players" WHERE msisdn = $1`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to delete attempted user: %w", err)
	}

	return result.RowsAffected(), nil
}

// CheckJackpotWinner checks for available jackpot winners
func (db *Database) CheckJackpotWinner(ctx context.Context) (map[string]interface{}, error) {
	query := `SELECT * FROM "jackpot_kitty" 
			 WHERE is_locked = 0 AND kitty > 0 AND kitty >= cost AND release_jackpot = 'yes' 
			 ORDER BY RANDOM() `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// GetOTPVerified returns a single verification row that hasn't expired (expired > now)
func (db *Database) GetOTPVerified(ctx context.Context, msisdn, code string, now int64) (map[string]interface{}, error) {
	query := `
		SELECT *
		FROM verification
		WHERE msisdn = $1 AND code = $2 AND expired > $3
		LIMIT 1
	`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, msisdn, code, now)
	if err != nil {
		return nil, fmt.Errorf("failed to execute GetOTPVerified query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToSingleMap(rows)
}

// GetOTPChecked returns a single verification row with status = 0 (unused)
func (db *Database) GetOTPChecked(ctx context.Context, msisdn, code string) (map[string]interface{}, error) {
	query := `
		SELECT *
		FROM verification
		WHERE status = 0 AND msisdn = $1 AND code = $2
		LIMIT 1
	`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, msisdn, code)
	if err != nil {
		return nil, fmt.Errorf("failed to execute GetOTPChecked query: %w", err)
	}
	defer rows.Close()

	return db.scanRowsToSingleMap(rows)
}

// UpdateIntoVerification marks the verification as used (status = 1) and returns affected rows
func (db *Database) UpdateIntoVerification(ctx context.Context, id int32) (int64, error) {
	query := `UPDATE verification SET status = 1 WHERE id = $1`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	res, err := conn.Exec(ctx, query, id)
	if err != nil {
		return 0, fmt.Errorf("failed to execute UpdateIntoVerification: %w", err)
	}

	return res.RowsAffected(), nil
}

func (db *Database) UpdateIntoVerificationOld(ctx context.Context, msisdn string) (int64, error) {
	query := `UPDATE verification SET status = 1 WHERE status = 0 and msisdn = $1`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	res, err := conn.Exec(ctx, query, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to execute UpdateIntoVerification: %w", err)
	}

	return res.RowsAffected(), nil
}

// CheckBasketLucky checks basket
func (db *Database) CheckBasketLucky(ctx context.Context) (map[string]interface{}, error) {
	query := `SELECT * FROM "Basket" `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// CheckAwardsLuckyRandom gets a random active award by name_init
func (db *Database) CheckAwardsLuckyRandom(ctx context.Context, nameInit string) (map[string]interface{}, error) {
	query := `SELECT * FROM "awards" WHERE name_init = $1 AND status = 'active' ORDER BY RANDOM() `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, nameInit)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// CheckAwardsLucky gets awards by name_init with value less than win amount
func (db *Database) CheckAwardsLucky(ctx context.Context, winAmount float64, nameInit string) (map[string]interface{}, error) {
	query := `SELECT * FROM "awards" WHERE name_init = $1 AND value < $2 AND status = 'active' `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, nameInit, winAmount)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)
}

// InsertHouseBasketLogs inserts basket logs
func (db *Database) InsertHouseBasketLogs(ctx context.Context, credit, debit, mvalue float64, narrative string) (int64, error) {
	query := `INSERT INTO "BasketLogs" (credit, debit, amount, narrative) VALUES ($1, $2, $3, $4)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, credit, debit, mvalue, narrative)
	if err != nil {
		return 0, fmt.Errorf("failed to insert basket logs: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateHouseAviatorHouse updates aviator house income
func (db *Database) UpdateHouseAviatorHouse(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "Aviator"."HouseIncome" SET house_income = house_income + $1`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		return 0, fmt.Errorf("failed to update aviator house income: %w", err)
	}

	return result.RowsAffected(), nil
}

// CheckHousePawaBoxKe gets house income data
func (db *Database) CheckHousePawaBoxKe(ctx context.Context) (map[string]interface{}, error) {
	query := `SELECT * FROM "HouseIncome" `

	log.Printf("Fetching house income data")

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for house income: %v", err)
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		log.Printf("Error querying house income: %v", err)
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error reading rows: %w", err)
		}
		return nil, nil // No rows found
	}

	values, err := rows.Values()
	if err != nil {
		return nil, fmt.Errorf("failed to get row values: %w", err)
	}

	fieldDescriptions := rows.FieldDescriptions()
	result := make(map[string]interface{})
	for i, fd := range fieldDescriptions {
		result[string(fd.Name)] = values[i]
	}

	log.Printf("House income data fetched successfully")
	return result, nil
}

// UpdateAviatorDepositRequestLucky updates deposit request to success status
func (db *Database) UpdateAviatorDepositRequestLucky(ctx context.Context, transactionID, reference, description string) (int64, error) {
	query := `UPDATE "deposit_requests" 
	SET status = 'success', transaction_id = $1, description = $2 
	WHERE reference = $3`

	log.Printf("Updating deposit request to success: ref=%s, transaction_id=%s", reference, transactionID)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for deposit update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, transactionID, description, reference)
	if err != nil {
		log.Printf("Error updating deposit request: %v", err)
		return 0, fmt.Errorf("failed to update deposit request: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("Deposit request updated to success: ref=%s, rows_affected=%d", reference, rowsAffected)

	return rowsAffected, nil
}

// InsertIntoDepositLuckyRequestBonus inserts bonus deposit request
func (db *Database) InsertIntoDepositLuckyRequestBonus(ctx context.Context, depositType, ussd, game, carrier string, gameCatID string, amount float64, msisdn, selectedBox, reference, channel string) (int64, error) {
	query := `INSERT INTO "deposit_requests" 
	(deposit_type, status, transaction_id, description, ussd, game, carrier, channel, game_cat_id, amount, msisdn, selected_box, reference) 
	VALUES ($1, 'success', $2, 'Free bets', $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	log.Printf("Inserting bonus deposit request: ref=%s, msisdn=%s, amount=%.2f, type=%s",
		reference, msisdn, amount, depositType)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for bonus deposit: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	params := []interface{}{depositType, reference, ussd, game, carrier, channel, gameCatID, amount, msisdn, selectedBox, reference}
	result, err := conn.Exec(ctx, query, params...)
	if err != nil {
		log.Printf("Error inserting bonus deposit request: %v", err)
		return 0, fmt.Errorf("failed to insert bonus deposit request: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("Bonus deposit request inserted: ref=%s, rows_affected=%d", reference, rowsAffected)

	return rowsAffected, nil
}

// InsertIntoDepositLuckyRequestComplete inserts a deposit request similar to the Python async version
func (db *Database) InsertIntoDepositLuckyRequestComplete(
	ctx context.Context,
	transactionID, description, game, carrier, channel, gameCatID string,
	amount float64,
	msisdn, selectedBox, reference string,
) (int64, error) {

	query := `INSERT INTO deposit_requests
        (gateway, status, transaction_id, description, game, carrier, channel, game_cat_id, amount, msisdn, selected_box, reference)
        VALUES ('direct deposit', 'success', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)` // equivalent to MySQL INSERT IGNORE

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	params := []interface{}{transactionID, description, game, carrier, channel, gameCatID, amount, msisdn, selectedBox, reference}

	result, err := conn.Exec(ctx, query, params...)
	if err != nil {
		log.Printf("Error inserting deposit request: %v", err)
		return 0, fmt.Errorf("failed to insert deposit request: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("Deposit request inserted: ref=%s, rows_affected=%d", reference, rowsAffected)

	return rowsAffected, nil
}

// CreateDepositRecordLucky creates a deposit record
func (db *Database) CreateDepositRecordLucky(ctx context.Context, msisdn string, amount float64, transactionID, shortcode, name, reference, depositType string) (int64, error) {
	query := `INSERT INTO "deposit" 
	(deposit_type, msisdn, amount, transaction_id, shortcode, name, mreference) 
	VALUES ($1, $2, $3, $4, $5, $6, $7)`

	log.Printf("Creating deposit record: ref=%s, msisdn=%s, amount=%.2f, type=%s",
		reference, msisdn, amount, depositType)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for deposit record: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	params := []interface{}{depositType, msisdn, amount, transactionID, shortcode, name, reference}
	result, err := conn.Exec(ctx, query, params...)
	if err != nil {
		log.Printf("Error creating deposit record: %v", err)
		return 0, fmt.Errorf("failed to create deposit record: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("Deposit record created: ref=%s, rows_affected=%d", reference, rowsAffected)

	return rowsAffected, nil
}

// UpdateHousePawaBoxKeBasket updates basket amount
func (db *Database) UpdateHousePawaBoxKeBasket(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "Basket" SET amount = amount + $1`

	log.Printf("Updating basket amount: +%.2f", mvalue)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for basket update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		log.Printf("Error updating basket: %v", err)
		return 0, fmt.Errorf("failed to update basket: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("Basket updated: +%.2f, rows_affected=%d", mvalue, rowsAffected)

	return rowsAffected, nil
}

// UpdateHousePawaBoxKeHouse updates house income
func (db *Database) UpdateHousePawaBoxKeHouse(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "HouseIncome" SET house_income = house_income + $1`

	log.Printf("Updating house income: +%.2f", mvalue)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for house income update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		log.Printf("Error updating house income: %v", err)
		return 0, fmt.Errorf("failed to update house income: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("House income updated: +%.2f, rows_affected=%d", mvalue, rowsAffected)

	return rowsAffected, nil
}

// UpdateHouseLucyNumberHouseCurrentRTP updates house current RTP
func (db *Database) UpdateHouseLucyNumberHouseCurrentRTP(ctx context.Context) (int64, error) {
	query := `UPDATE "HouseIncome" 
	SET current_rtp = (total_wins / CASE WHEN total_bets = 0 THEN 1 ELSE total_bets END) * 100`

	log.Printf("Updating house current RTP")

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for RTP update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query)
	if err != nil {
		log.Printf("Error updating house RTP: %v", err)
		return 0, fmt.Errorf("failed to update house RTP: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("House RTP updated, rows_affected=%d", rowsAffected)

	return rowsAffected, nil
}

// UpdateHousePawaBoxKeBets updates house total bets
func (db *Database) UpdateHousePawaBoxKeBets(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "HouseIncome" SET total_bets = total_bets + $1`

	log.Printf("Updating house total bets: +%.2f", mvalue)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for house bets update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		log.Printf("Error updating house bets: %v", err)
		return 0, fmt.Errorf("failed to update house bets: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("House bets updated: +%.2f, rows_affected=%d", mvalue, rowsAffected)

	return rowsAffected, nil
}

// UpdateCustomerAviator updates aviator customer field
func (db *Database) UpdateCustomerAviator(ctx context.Context, fieldName string, mvalue interface{}, id int64) (int64, error) {
	query := fmt.Sprintf(`UPDATE "Aviator"."Customer" SET %s = $1 WHERE id = $2`, fieldName)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update aviator customer: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateCustomerAviatorBets updates aviator customer bets
func (db *Database) UpdateCustomerAviatorBets(ctx context.Context, mvalue float64, id int64) (int64, error) {
	query := `UPDATE "Aviator"."Customer" 
			 SET last_stake_time = NOW(), 
				 bets = bets + 1, 
				 total_bets = total_bets + $1 
			 WHERE id = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update aviator customer bets: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateCustomerAviatorWins updates aviator customer wins
func (db *Database) UpdateCustomerAviatorWins(ctx context.Context, mvalue float64, id int64) (int64, error) {
	query := `UPDATE "Aviator"."Customer" 
			 SET win_count = win_count + 1, 
				 loss_count = 0, 
				 wins = wins + 1, 
				 total_wins = total_wins + $1 
			 WHERE id = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update aviator customer wins: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateCustomerAviatorLosses updates aviator customer losses
func (db *Database) UpdateCustomerAviatorLosses(ctx context.Context, mvalue float64, id int64) (int64, error) {
	query := `UPDATE "Aviator"."Customer" 
			 SET loss_count = loss_count + 1, 
				 win_count = 0, 
				 losses = losses + 1, 
				 total_losses = total_losses + $1 
			 WHERE id = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update aviator customer losses: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateCustomerWallet updates customer wallet balance
func (db *Database) UpdateCustomerWallet(ctx context.Context, id int64, mvalue float64) (int64, error) {
	query := `UPDATE "Aviator"."Customer" SET balance = $1 WHERE id = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, id)
	if err != nil {
		return 0, fmt.Errorf("failed to update customer wallet: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateCustomerWallet2 updates customer total withdrawal
func (db *Database) UpdateCustomerWallet2(ctx context.Context, msisdn string, mvalue float64) (int64, error) {
	query := `UPDATE "Aviator"."Customer" SET total_withdrawal = total_withdrawal + $1 WHERE msisdn = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update customer wallet2: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertCustomerLogsAviator inserts aviator customer logs
func (db *Database) InsertCustomerLogsAviator(ctx context.Context, amount float64, logType string, customerID int64, narrative string) (int64, error) {
	query := `INSERT INTO "Aviator"."CustomerLogs" (customer_id, type, narrative, amount) VALUES ($1, $2, $3, $4)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, customerID, logType, narrative, amount)
	if err != nil {
		return 0, fmt.Errorf("failed to insert aviator customer logs: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertCustomerLogsAviatorGame inserts aviator customer logs with game ID
func (db *Database) InsertCustomerLogsAviatorGame(ctx context.Context, gameID int64, amount float64, logType string, customerID int64, narrative string) (int64, error) {
	query := `INSERT INTO "Aviator"."CustomerLogs" (game_id, customer_id, type, narrative, amount) VALUES ($1, $2, $3, $4, $5)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, gameID, customerID, logType, narrative, amount)
	if err != nil {
		return 0, fmt.Errorf("failed to insert aviator customer logs with game: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertUSSDSession inserts USSD session
func (db *Database) InsertUSSDSession(ctx context.Context, data map[string]string) (int64, error) {
	query := `INSERT INTO "Aviator"."ussd_session" (sessionId, serviceCode, msisdn, ussdString) VALUES ($1, $2, $3, $4)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, data["sessionId"], data["serviceCode"], data["msisdn"], data["ussdString"])
	if err != nil {
		return 0, fmt.Errorf("failed to insert USSD session: %w", err)
	}

	return result.RowsAffected(), nil
}

// DisburseWithdrawalAviator inserts into withdrawal queue for aviator
func (db *Database) DisburseWithdrawalAviator(ctx context.Context, amount float64, msisdn, reference string) (int64, error) {
	query := `INSERT INTO "luckynumber"."withdrawal_queue_ke" 
			 (msisdn, amount, client, callback, reference) 
			 VALUES ($1, $2, 'aviator', 'https://gameapi.strikebet.co.ke/', $3)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, msisdn, amount, reference)
	if err != nil {
		return 0, fmt.Errorf("failed to disburse aviator withdrawal: %w", err)
	}

	return result.RowsAffected(), nil
}

// DisburseWithdrawals inserts into mpesa disburse
func (db *Database) DisburseWithdrawals(ctx context.Context, amount float64, msisdn, reference string) (int64, error) {
	query := `INSERT INTO "mpesa_disburse" 
			 (transaction_id, reference, amount, msisdn) 
			 VALUES (uuid(), $1, $2, $3)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, reference, amount, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to disburse withdrawals: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertIntoWithdrawals inserts into aviator withdrawals
func (db *Database) InsertIntoWithdrawals(ctx context.Context, amount float64, msisdn, reference string) (int64, error) {
	query := `INSERT INTO "Aviator"."withdrawals" (reference, amount, msisdn) VALUES ($1, $2, $3)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, reference, amount, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into withdrawals: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertIntoWithdrawalsLucky inserts into pawa box withdrawals
func (db *Database) InsertIntoWithdrawalsLucky(ctx context.Context, nonAmount, amount, withholdTax float64, items string, msisdn, reference string) (int64, error) {
	query := `INSERT INTO "withdrawals" 
			 (non_roundoff_amount, tax_amount, items, game_id, reference, amount, msisdn) 
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, nonAmount, withholdTax, items, reference, reference, amount, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into lucky withdrawals: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertIntoJackPotWinners inserts jackpot winners
func (db *Database) InsertIntoJackPotWinners(ctx context.Context, taxAmount float64, items string, gameID string, gameName, jackpotCategory string, kittyID string, amount float64, msisdn string) (int64, error) {
	query := `INSERT INTO "jackpot_winners" 
			 (tax_amount, items, game_id, game_name, jackpot_category, kitty_id, amount, msisdn, awarded) 
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'yes')`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, taxAmount, items, gameID, gameName, jackpotCategory, kittyID, amount, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to insert jackpot winners: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertIntoPendingWithdrawalsLucky inserts into pending withdrawals
func (db *Database) InsertIntoPendingWithdrawalsLucky(ctx context.Context, amount, taxAmount float64, items, msisdn, reference string) (int64, error) {
	query := `INSERT INTO "pending_withdrawals" 
			 (tax_amount, items, reference, amount, msisdn) 
			 VALUES ($1, $2, $3, $4, $5)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, taxAmount, items, reference, amount, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to insert pending withdrawals: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertIntoDepositRequest inserts into deposit requests
func (db *Database) InsertIntoDepositRequest(ctx context.Context, amount float64, msisdn, reference string) (int64, error) {
	query := `INSERT INTO "Aviator"."deposit_requests" (amount, msisdn, reference) VALUES ($1, $2, $3)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, amount, msisdn, reference)
	if err != nil {
		return 0, fmt.Errorf("failed to insert deposit request: %w", err)
	}

	return result.RowsAffected(), nil
}

// CheckWithdrawalsPawaBoxKe checks pending withdrawals
func (db *Database) CheckWithdrawalsPawaBoxKe(ctx context.Context, reference string) (map[string]interface{}, error) {
	query := `SELECT * FROM "withdrawals" WHERE status = 'pending' AND reference = $1 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, reference)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// CheckUSSDSession checks USSD session
func (db *Database) CheckUSSDSession(ctx context.Context, sessionID string) (map[string]interface{}, error) {
	query := `SELECT * FROM "Aviator"."ussd_session" WHERE sessionId = $1 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, sessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// InsertHouseLogsAviator inserts aviator house logs
func (db *Database) InsertHouseLogsAviator(ctx context.Context, fieldName, msisdn string, mvalue float64) (int64, error) {
	query := fmt.Sprintf(`INSERT INTO "Aviator"."HouseIncomeLogs" (msisdn, %s) VALUES ($1, $2)`, fieldName)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, msisdn, mvalue)
	if err != nil {
		return 0, fmt.Errorf("failed to insert aviator house logs: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertHouseLogsAviatorGameID inserts aviator house logs with game ID
func (db *Database) InsertHouseLogsAviatorGameID(ctx context.Context, gameID int64, fieldName, msisdn string, mvalue float64) (int64, error) {
	query := fmt.Sprintf(`INSERT INTO "Aviator"."HouseIncomeLogs" (game_id, msisdn, %s) VALUES ($1, $2, $3)`, fieldName)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, gameID, msisdn, mvalue)
	if err != nil {
		return 0, fmt.Errorf("failed to insert aviator house logs with game: %w", err)
	}

	return result.RowsAffected(), nil
}

// CheckUSSDSessionLogs checks USSD session logs
func (db *Database) CheckUSSDSessionLogs(ctx context.Context, msisdn string) (map[string]interface{}, error) {
	query := `SELECT payload, msisdn FROM "Aviator"."ussd_logs" 
			 WHERE msisdn = $1 
			 AND (status = 'continue' OR status IS NULL)
			 AND (payload IS NOT NULL AND payload <> '')
			 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, msisdn)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// CheckDepositRequestLuckyFailed checks if a deposit request exists by reference
func (db *Database) CheckDepositRequestLuckyFailed(ctx context.Context, reference string) (map[string]interface{}, error) {
	query := `SELECT * FROM "deposit_requests" WHERE reference = $1 `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, reference)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error reading rows: %w", err)
		}
		return nil, nil // No rows found
	}

	values, err := rows.Values()
	if err != nil {
		return nil, fmt.Errorf("failed to get row values: %w", err)
	}

	fieldDescriptions := rows.FieldDescriptions()
	result := make(map[string]interface{})
	for i, fd := range fieldDescriptions {
		result[string(fd.Name)] = values[i]
	}

	return result, nil
}

// CheckDepositRequests checks deposit requests
func (db *Database) CheckDepositRequests(ctx context.Context, reference string) (map[string]interface{}, error) {
	query := `SELECT * FROM "Aviator"."deposit_requests" WHERE reference = $1 AND status = 'pending' `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, reference)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

// CheckWithdrawalRequests checks withdrawal requests
func (db *Database) CheckWithdrawalRequests(ctx context.Context, reference string) (map[string]interface{}, error) {
	query := `SELECT * FROM "Aviator"."withdrawals" WHERE reference = $1 AND status = 'pending' `

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, reference)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Now use scanRowsToSingleMap which works with pgx.Rows
	return db.scanRowsToSingleMap(rows)

}

func (db *Database) InsertIntoDepositLuckyRequest(
	ctx context.Context,
	ussd, game, carrier string,
	gameCatID string,
	amount float64,
	msisdn, selectedBox, reference, channel string,
) (int64, error) {

	query := `INSERT INTO "deposit_requests" 
          (ussd, game, carrier, channel, game_cat_id, amount, msisdn, selected_box, reference) 
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	// Acquire connection from pool
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release() // Automatically release connection when function exits

	// Execute the query
	_, err = conn.Exec(
		ctx,
		query,
		ussd, game, carrier, channel, gameCatID, amount, utils.ToString(msisdn), utils.ToInt(selectedBox), reference,
	)
	if err != nil {
		fmt.Errorf("Failed to insert deposit request: %v", err)
		return 0, fmt.Errorf("failed to insert deposit request: %w", err)
	}

	// If you need the last inserted ID instead of rows affected:
	// Note: This depends on your database driver. For PostgreSQL with pgx:
	var lastInsertID int64
	err = conn.QueryRow(ctx, "SELECT LASTVAL()").Scan(&lastInsertID)
	if err != nil {
		fmt.Errorf("Failed to get last insert ID: %v", err)
		return 0, fmt.Errorf("failed to get last insert ID: %w", err)
	}

	fmt.Printf("Successfully inserted deposit request with ID: %d", lastInsertID)
	return lastInsertID, nil
}

// InsertSTK inserts into STK queue
func (db *Database) InsertSTK(ctx context.Context, game, carrier, reference, msisdn string, amount float64, shortcode string) (int64, error) {
	query := `INSERT INTO "stk_queue_ke" 
			 (game, carrier, reference, msisdn, amount, account, shortcode) 
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, game, carrier, reference, msisdn, amount, reference, shortcode)
	if err != nil {
		return 0, fmt.Errorf("failed to insert STK: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertWithdrawalQueue inserts into withdrawal queue
func (db *Database) InsertWithdrawalQueue(ctx context.Context, reference, msisdn string, amount float64, callback string) (int64, error) {
	query := `INSERT INTO "withdrawal_queue_ke" 
			 (reference, msisdn, amount, callback) 
			 VALUES ($1, $2, $3, $4)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, reference, msisdn, amount, callback)
	if err != nil {
		return 0, fmt.Errorf("failed to insert withdrawal queue: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertCustomerLogsPawaBoxKeWithID inserts customer logs and returns the ID
func (db *Database) InsertCustomerLogsPawaBoxKeWithID(ctx context.Context, amount float64, logType string, customerID int64, narrative, reference string) (int64, error) {
	query := `INSERT INTO "CustomerLogs" 
	(customer_id, type, narrative, amount, game_id) 
	VALUES ($1, $2, $3, $4, $5) 
	RETURNING id`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var insertedID int64
	err = conn.QueryRow(ctx, query, customerID, logType, narrative, amount, reference).Scan(&insertedID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert customer logs: %w", err)
	}

	return insertedID, nil
}

// UpdateHouseLuckyWins updates house total wins
func (db *Database) UpdateHouseLuckyWins(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "HouseIncome" 
	SET total_wins = total_wins + $1`

	log.Printf("Updating house wins: +%.2f", mvalue)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for house wins update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		log.Printf("Error updating house wins: %v", err)
		return 0, fmt.Errorf("failed to update house wins: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("House wins updated successfully: +%.2f, rows_affected=%d", mvalue, rowsAffected)

	return rowsAffected, nil
}

// UpdateHouseLuckyBasketWins deducts amount from basket for wins
func (db *Database) UpdateHouseLuckyBasketWins(ctx context.Context, mvalue float64) (bool, error) {
	query := `UPDATE "Basket" 
	SET amount = amount - $1 
	WHERE amount >= $1` // Ensure we don't go negative

	log.Printf("Deducting from basket for wins: -%.2f", mvalue)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for basket update: %v", err)
		return false, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		log.Printf("Error updating basket: %v", err)
		return false, fmt.Errorf("failed to update basket: %w", err)
	}

	rowsAffected := result.RowsAffected()
	success := rowsAffected > 0

	if success {
		log.Printf("Basket updated successfully: -%.2f, rows_affected=%d", mvalue, rowsAffected)
	} else {
		log.Printf("No basket record updated (insufficient funds or no record): amount=%.2f", mvalue)
	}

	return success, nil
}

// UpdateRESTLossUser updates player payout and resets loss count
func (db *Database) UpdateRESTLossUser(ctx context.Context, payout float64, id int64) (int64, error) {
	query := `UPDATE "Player" 
	SET payout = payout + $1, lost_count = 0 
	WHERE id = $2`

	log.Printf("Updating player loss reset: id=%d, payout=+%.2f", id, payout)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for player update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, payout, id)
	if err != nil {
		log.Printf("Error updating player loss reset: %v", err)
		return 0, fmt.Errorf("failed to update player: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("Player loss reset updated: id=%d, rows_affected=%d", id, rowsAffected)

	return rowsAffected, nil
}

// InsertTaxQueueWithID inserts tax record and returns the ID
func (db *Database) InsertTaxQueue(ctx context.Context, gameID string, amount, taxAmount, taxDeductedAmount float64, taxType, msisdn string) (int64, error) {
	query := `INSERT INTO "tax_record" 
	(game_id, amount, tax_amount, after_tax, tax_type, msisdn) 
	VALUES ($1, $2, $3, $4, $5, $6) 
	ON CONFLICT DO NOTHING
	RETURNING id`

	log.Printf("Inserting tax record: game_id=%d, amount=%.2f, tax=%.2f, type=%s, msisdn=%s",
		gameID, amount, taxAmount, taxType, msisdn)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for tax record: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var insertedID int64
	err = conn.QueryRow(ctx, query, gameID, amount, taxAmount, taxDeductedAmount, taxType, msisdn).Scan(&insertedID)
	if err != nil {
		// Check if it's a no-rows error (conflict)
		if err == pgx.ErrNoRows {
			log.Printf("Tax record already exists or conflict occurred")
			return 0, nil
		}
		log.Printf("Error inserting tax record: %v", err)
		return 0, fmt.Errorf("failed to insert tax record: %w", err)
	}

	log.Printf("Tax record inserted successfully, ID: %d", insertedID)
	return insertedID, nil
}

// UpdatePawaBoxKeWithdrawalRequest updates withdrawal request status to processed
func (db *Database) UpdatePawaBoxKeWithdrawalRequest(ctx context.Context, reference string) (int64, error) {
	query := `UPDATE "withdrawals" 
	SET status = 'processed' 
	WHERE reference = $1`

	log.Printf("Updating withdrawal request status to processed: ref=%s", reference)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for withdrawal update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, reference)
	if err != nil {
		log.Printf("Error updating withdrawal request: %v", err)
		return 0, fmt.Errorf("failed to update withdrawal request: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("Withdrawal request updated to processed: ref=%s, rows_affected=%d", reference, rowsAffected)

	return rowsAffected, nil
}

// UpdateHouseLuckyHouseLosses updates house losses
func (db *Database) UpdateHouseLuckyHouseLosses(ctx context.Context, mvalue float64) (int64, error) {
	query := `UPDATE "HouseIncome" 
	SET total_losses = total_losses + $1`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, mvalue)
	if err != nil {
		return 0, fmt.Errorf("failed to update house losses: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdatePawaBoxKeWithdrawalB2BDisburse updates B2B withdrawal disbursement status
func (db *Database) UpdatePawaBoxKeWithdrawalB2BDisburse(ctx context.Context, transactionID, status, description, reference string) (bool, error) {
	query := `UPDATE "withdrawalsb2b" 
	SET transaction_id = $1, disburse = $2, description = $3 
	WHERE status = 'processed' AND reference = $4`

	log.Printf("Updating B2B withdrawal disburse: ref=%s, transaction_id=%s, status=%s",
		reference, transactionID, status)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for B2B withdrawal update: %v", err)
		return false, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, transactionID, status, description, reference)
	if err != nil {
		log.Printf("Error updating B2B withdrawal disburse: %v", err)
		return false, fmt.Errorf("failed to update B2B withdrawal disburse: %w", err)
	}

	rowsAffected := result.RowsAffected()
	success := rowsAffected > 0

	if success {
		log.Printf("B2B withdrawal disburse updated successfully: ref=%s, rows_affected=%d", reference, rowsAffected)
	} else {
		log.Printf("No B2B withdrawal found to update: ref=%s", reference)
	}

	return success, nil
}

// UpdatePawaBoxKeWithdrawalDisburseMotto updates LudoMotto withdrawal disbursement status
func (db *Database) UpdatePawaBoxKeWithdrawalDisburseMotto(ctx context.Context, transactionID, status, description, reference string) (bool, error) {
	query := `UPDATE "LudoMotto_Ke"."withdrawals" 
	SET transaction_id = $1, disburse = $2, description = $3 
	WHERE status = 'processed' AND reference = $4`

	log.Printf("Updating LudoMotto withdrawal disburse: ref=%s, transaction_id=%s, status=%s",
		reference, transactionID, status)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for LudoMotto withdrawal update: %v", err)
		return false, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, transactionID, status, description, reference)
	if err != nil {
		log.Printf("Error updating LudoMotto withdrawal disburse: %v", err)
		return false, fmt.Errorf("failed to update LudoMotto withdrawal disburse: %w", err)
	}

	rowsAffected := result.RowsAffected()
	success := rowsAffected > 0

	if success {
		log.Printf("LudoMotto withdrawal disburse updated successfully: ref=%s, rows_affected=%d", reference, rowsAffected)
	} else {
		log.Printf("No LudoMotto withdrawal found to update: ref=%s", reference)
	}

	return success, nil
}

// UpdatePawaBoxKeWithdrawalDisburse updates regular withdrawal disbursement status
func (db *Database) UpdatePawaBoxKeWithdrawalDisburse(ctx context.Context, transactionID, status, description, reference string) (bool, error) {
	query := `UPDATE "withdrawals" 
	SET transaction_id = $1, disburse = $2, description = $3 
	WHERE status = 'processed' AND reference = $4`

	log.Printf("Updating withdrawal disburse: ref=%s, transaction_id=%s, status=%s",
		reference, transactionID, status)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for withdrawal update: %v", err)
		return false, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, transactionID, status, description, reference)
	if err != nil {
		log.Printf("Error updating withdrawal disburse: %v", err)
		return false, fmt.Errorf("failed to update withdrawal disburse: %w", err)
	}

	rowsAffected := result.RowsAffected()
	success := rowsAffected > 0

	if success {
		log.Printf("Withdrawal disburse updated successfully: ref=%s, rows_affected=%d", reference, rowsAffected)
	} else {
		log.Printf("No withdrawal found to update: ref=%s", reference)
	}

	return success, nil
}

// UpdateAviatorDepositFailRequestLucky updates deposit request to failed status
func (db *Database) UpdateAviatorDepositFailRequestLucky(ctx context.Context, reference, description string) (int64, error) {
	query := `UPDATE "deposit_requests" 
	SET status = 'fail', description = $1 
	WHERE reference = $2`

	log.Printf("Updating deposit request to failed: ref=%s, description=%s", reference, description)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for deposit fail update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, description, reference)
	if err != nil {
		log.Printf("Error updating deposit request to failed: %v", err)
		return 0, fmt.Errorf("failed to update deposit request: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("Deposit request updated to failed: ref=%s, rows_affected=%d", reference, rowsAffected)

	return rowsAffected, nil
}

// UpdateAviatorDepositFailRequestLuckySTK updates STK results to failed status
func (db *Database) UpdateAviatorDepositFailRequestLuckySTK(ctx context.Context, reference, description string) (int64, error) {
	query := `UPDATE "stk_results" 
	SET status = 'fail', description = $1 
	WHERE reference = $2`

	log.Printf("Updating STK result to failed: ref=%s, description=%s", reference, description)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for STK fail update: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, description, reference)
	if err != nil {
		log.Printf("Error updating STK result to failed: %v", err)
		return 0, fmt.Errorf("failed to update STK result: %w", err)
	}

	rowsAffected := result.RowsAffected()
	log.Printf("STK result updated to failed: ref=%s, rows_affected=%d", reference, rowsAffected)

	return rowsAffected, nil
}

// InsertIntoSMSQueueWithID inserts a message into the SMS queue and returns the ID
func (db *Database) InsertIntoSMSQueue(ctx context.Context, msisdn, message, smscID, response string) (int64, error) {
	query := `INSERT INTO "dbQueue" ("Originator", "Destination", "Message",  "MessageDirection","MessageTimeStamp", "SMSCID", "command")
VALUES ($1, $2, $3, $4, NOW(), $5, $6) 
    RETURNING "RecordID"`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var insertedID int64
	params := []interface{}{"LuckyNumber", msisdn, message, "OUT", smscID, response}
	err = conn.QueryRow(ctx, query, params...).Scan(&insertedID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert into SMS queue: %w", err)
	}

	logrus.Infof("✅ SMS queued successfully with ID: %d for %s", insertedID, msisdn)
	return insertedID, nil
}

// InsertCustomerLogsPawaBoxKeWithID inserts customer logs and returns the ID
func (db *Database) InsertCustomerLogsPawaBoxKe(ctx context.Context, amount float64, logType string, customerID string, narrative, reference string) (int64, error) {
	query := `INSERT INTO "CustomerLogs" 
	(customer_id, type, narrative, amount, game_id) 
	VALUES ($1, $2, $3, $4, $5) 
	RETURNING id`

	log.Printf("Inserting customer log: customer_id=%d, type=%s, amount=%.2f", customerID, logType, amount)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for customer log: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var insertedID int64
	err = conn.QueryRow(ctx, query, customerID, logType, narrative, amount, reference).Scan(&insertedID)
	if err != nil {
		log.Printf("Error inserting customer log: %v", err)
		return 0, fmt.Errorf("failed to insert customer log: %w", err)
	}

	log.Printf("Customer log inserted successfully, ID: %d", insertedID)
	return insertedID, nil
}

// InsertHouseLogsPawaBoxKeGameIDWithID inserts house income logs and returns the ID
func (db *Database) InsertHouseLogsPawaBoxKeGameID(ctx context.Context, gameID string, fieldName, msisdn string, mvalue float64) (int64, error) {
	// Validate field name
	validFields := map[string]bool{
		"total_bets": true, "total_wins": true, "total_losses": true,
		"house_income": true, "total_payout": true, "total_profit": true,
		"amount": true, "credit": true, "debit": true,
	}

	if !validFields[fieldName] {
		return 0, fmt.Errorf("invalid field name: %s", fieldName)
	}

	query := fmt.Sprintf(`INSERT INTO "HouseIncomeLogs" 
	(game_id, msisdn, %s) 
	VALUES ($1, $2, $3) 
	RETURNING id`, fieldName)

	log.Printf("Inserting house income log: game_id=%d, msisdn=%s, %s=%.2f", gameID, msisdn, fieldName, mvalue)

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		log.Printf("Error acquiring connection for house income log: %v", err)
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var insertedID int64
	err = conn.QueryRow(ctx, query, gameID, msisdn, mvalue).Scan(&insertedID)
	if err != nil {
		log.Printf("Error inserting house income log: %v", err)
		return 0, fmt.Errorf("failed to insert house income log: %w", err)
	}

	log.Printf("House income log inserted successfully, ID: %d", insertedID)
	return insertedID, nil
}

// InsertB2BWithdrawalB2B inserts into B2B withdrawal processing
func (db *Database) InsertB2BWithdrawalB2B(ctx context.Context, reference, msisdn string, amount float64, betStatus string) (int64, error) {
	query := `INSERT INTO "withdrawal_b2b_to_process" 
			 (reference, msisdn, amount, bet_status) 
			 VALUES ($1, $2, $3, $4)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, reference, msisdn, amount, betStatus)
	if err != nil {
		return 0, fmt.Errorf("failed to insert B2B withdrawal: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertUSSDLogs inserts USSD session logs
func (db *Database) InsertUSSDLogs(ctx context.Context, msisdn, sessionID, serviceCode, ussdString string) (int64, error) {
	query := `INSERT INTO "ussd_session" 
	(msisdn, sessionId, serviceCode, ussdString) 
	VALUES ($1, $2, $3, $4) 
	ON CONFLICT DO NOTHING` // PostgreSQL equivalent of INSERT IGNORE

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	println(msisdn)
	// println(msisdn, sessionID, serviceCode, ussdString)
	result, err := conn.Exec(ctx, query, msisdn, sessionID, serviceCode, ussdString)
	if err != nil {

		println(fmt.Errorf("failed to execute query: %w", err))
		return 0, fmt.Errorf("failed to insert USSD logs: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertAviatorUssdLogs inserts aviator USSD logs
func (db *Database) InsertAviatorUssdLogs(ctx context.Context, gameID int64, sessionID, msisdn, payload string) (int64, error) {
	query := `INSERT INTO "Aviator"."ussd_logs" (game_id, sessionid, msisdn, payload) VALUES ($1, $2, $3, $4)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, gameID, sessionID, msisdn, payload)
	if err != nil {
		return 0, fmt.Errorf("failed to insert aviator USSD logs: %w", err)
	}

	return result.RowsAffected(), nil
}

// InsertAviatorUssdInputLogs inserts aviator USSD input logs
func (db *Database) InsertAviatorUssdInputLogs(ctx context.Context, sessionCode, sessionID, msisdn, ussdMenu string) (int64, error) {
	query := `INSERT INTO "Aviator"."ussd_log_inputs" (ussd_code, sessionid, msisdn, ussd_menu) VALUES ($1, $2, $3, $4)`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, sessionCode, sessionID, msisdn, ussdMenu)
	if err != nil {
		return 0, fmt.Errorf("failed to insert aviator USSD input logs: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateAviatorUssdLogs updates aviator USSD logs
func (db *Database) UpdateAviatorUssdLogs(ctx context.Context, sessionID, msisdn string, gameID int64, payload string) (int64, error) {
	query := `UPDATE "Aviator"."ussd_logs" 
			 SET sessionid = $1, payload = $2 
			 WHERE game_id = $3 AND msisdn = $4`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, sessionID, payload, gameID, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update aviator USSD logs: %w", err)
	}

	return result.RowsAffected(), nil
}

// UpdateAviatorUssdLogsStatus updates aviator USSD logs status
func (db *Database) UpdateAviatorUssdLogsStatus(ctx context.Context, gameID int64, msisdn string) (int64, error) {
	query := `UPDATE "Aviator"."ussd_logs" SET status = 'closed' WHERE game_id = $1 AND msisdn = $2`

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	result, err := conn.Exec(ctx, query, gameID, msisdn)
	if err != nil {
		return 0, fmt.Errorf("failed to update aviator USSD logs status: %w", err)
	}

	return result.RowsAffected(), nil
}

// Additional methods can be added following the same pattern...

// Close closes the database connection pool
func (db *Database) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}
