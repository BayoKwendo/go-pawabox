package utils

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

var userLocks = struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}{m: make(map[string]*sync.Mutex)}

// GetLockForUser returns a mutex for the given msisdn (creates if absent)
func GetLockForUser(msisdn string) *sync.Mutex {
	userLocks.mu.Lock()
	defer userLocks.mu.Unlock()
	if l, ok := userLocks.m[msisdn]; ok {
		return l
	}
	l := &sync.Mutex{}
	userLocks.m[msisdn] = l
	return l
}

// toFloat64 safely converts any value to float64
func ToFloat64(value interface{}) float64 {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	case int16:
		return float64(v)
	case int8:
		return float64(v)
	case uint:
		return float64(v)
	case uint64:
		return float64(v)
	case uint32:
		return float64(v)
	case uint16:
		return float64(v)
	case uint8:
		return float64(v)
	case string:
		if v == "" {
			return 0
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		return 0
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		strVal := fmt.Sprintf("%v", value)
		if f, err := strconv.ParseFloat(strVal, 64); err == nil {
			return f
		}
		return 0
	}
}

// ToInt64 safely converts any value to int64
func ToInt64(value interface{}) int64 {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case uint64:
		return int64(v)
	case uint32:
		return int64(v)
	case uint:
		return int64(v)
	case uint16:
		return int64(v)
	case uint8:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case string:
		if v == "" {
			return 0
		}
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		return 0
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		strVal := fmt.Sprintf("%v", value)
		if i, err := strconv.ParseInt(strVal, 10, 64); err == nil {
			return i
		}
		return 0
	}
}

// ToInt safely converts any value to int
func ToInt(value interface{}) int {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case int32:
		return int(v)
	case int16:
		return int(v)
	case int8:
		return int(v)
	case uint:
		return int(v)
	case uint64:
		return int(v)
	case uint32:
		return int(v)
	case uint16:
		return int(v)
	case uint8:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	case string:
		if v == "" {
			return 0
		}
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
		return 0
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		strVal := fmt.Sprintf("%v", value)
		if i, err := strconv.Atoi(strVal); err == nil {
			return i
		}
		return 0
	}
}

// ToString safely converts any value to string
func ToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int, int64, int32, int16, int8:
		return fmt.Sprintf("%d", v)
	case uint, uint64, uint32, uint16, uint8:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%.0f", v) // ✅ zero decimal places
	case float32:
		return fmt.Sprintf("%.0f", v) // ✅ zero decimal places
	case bool:
		return strconv.FormatBool(v)
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ToBool safely converts any value to bool
func ToBool(value interface{}) bool {
	if value == nil {
		return false
	}

	switch v := value.(type) {
	case bool:
		return v
	case int, int64, int32, int16, int8:
		return ToInt64(v) != 0
	case uint, uint64, uint32, uint16, uint8:
		return ToInt64(v) != 0
	case float64, float32:
		return ToFloat64(v) != 0
	case string:
		if v == "" {
			return false
		}
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		lower := strings.ToLower(v)
		return lower == "true" || lower == "yes" || lower == "1" || lower == "on"
	default:
		strVal := fmt.Sprintf("%v", value)
		return ToBool(strVal)
	}
}

// JWTMiddleware verifies the JWT and sets claims in c.Locals("user")
func JWTMiddleware() fiber.Handler {
	secret := JWT_SECRET

	return func(c *fiber.Ctx) error {
		authHeader := c.Get("x-access-token")
		if authHeader == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"Status":        false,
				"StatusCode":    1,
				"StatusMessage": "missing Authorization header",
			})
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"Status":        false,
				"StatusCode":    1,
				"StatusMessage": "invalid Authorization header format",
			})
		}

		tokenString := parts[1]

		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			// ensure token method is HMAC
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return []byte(secret), nil
		})
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"Status":        false,
				"StatusCode":    1,
				"StatusMessage": "invalid token",
			})
		}

		if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
			c.Locals("user", claims) // store claims for handlers
			return c.Next()
		}

		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"Status":        false,
			"StatusCode":    1,
			"StatusMessage": "invalid or expired token",
		})
	}
}

// ToSQLFloat converts to float64 suitable for SQL (handles NaN, Inf)
func ToSQLFloat(value interface{}) interface{} {
	f := ToFloat64(value)

	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}

	return f
}

var JWT_SECRET = "sdjffjjf83488fdfnfnbbnfbnmd20304483e@u3rhnfhfsu8@##UJjdjdjjJHDDHDHJSMWHSHHHD@*EUDHFHH"

var Texts = map[string]map[string]string{
	"results": {
		"win": `Congratulations!! UMESHINDA
-
Ulichagua %s. UMESHINDA: %s
-
%s
-
Free Bet - %d
-
Cheza Tena *463#
-
game-id: %s
-
Help: 0703012550`,

		"jackpot": `CONGRATULATIONS! ID:%s IMESHINDA %s YENYE THAMANI KES %s

KIASI HIKI UTATUMIWA KWENYE ACCOUNT YAKO

Cheza Tena *463#

Help: 0703012550`,

		"loss": `Samahani, Jaribu tena
-
Ulichagua: %s
-
%s
-
Free Bet - %d
-
Cheza Tena *463#
-
game-id: %s
-
Help: 0703012550`,

		"cancelled": `Hapa chini ni muundo rahisi wa hatua za kucheza Bado Kidogo Ushinde!:

Bonyeza *148*33#

Chagua boksi lako (Boxi unalotaka kuchezea)

Ingiza nambari yako ya siri

Bonyeza 1 kuthibitisha na kuanza mchezo`,
	},
}

// Extract the core token verification logic from your middleware
func VerifyJWTToken(tokenString string) (jwt.MapClaims, error) {
	secret := JWT_SECRET

	if tokenString == "" {
		return nil, fmt.Errorf("missing token")
	}

	// Handle Bearer token format
	// Remove "Bearer " prefix if present
	if len(tokenString) > 7 && strings.HasPrefix(strings.ToLower(tokenString), "bearer ") {
		tokenString = tokenString[7:] // Remove "Bearer " prefix
	}

	// Trim any whitespace
	tokenString = strings.TrimSpace(tokenString)

	if tokenString == "" {
		return nil, fmt.Errorf("missing token after Bearer prefix")
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// ensure token method is HMAC
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secret), nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid token: %v", err)
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, fmt.Errorf("invalid or expired token")
}

// Helper function to extract token from various sources
func RandomFloat(min, max float64) float64 {
	if max <= min {
		return min
	}
	return min + rand.Float64()*(max-min)
}
