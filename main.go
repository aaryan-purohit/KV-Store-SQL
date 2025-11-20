package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type KeyValue struct {
	Key       string                 `json:"key,omitempty" binding:"required"`
	Value     map[string]interface{} `json:"value,omitempty" binding:"required"`
	ExpiredAt *time.Time             `json:"expired_at,omitempty"`
}

var Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

// function to connect to DB
func connectDB(database, password string) *sql.DB {
	dsn := fmt.Sprintf("root:%s@tcp(127.0.0.1:3306)/%s?parseTime=true", database, password)
	Logger.Info(dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		Logger.Error("Failed to open DB connection: %v", "error", err)
		panic(err)
	}

	if err := db.Ping(); err != nil {
		Logger.Error("Failed to ping database: %v", "error", err)
		panic(err)
	}

	return db
}

// index handler
func indexHanlder(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "Hello, Welcome to KV Store API Index Page",
	})
}

// get all values from the db
func GetAllKV(db *sql.DB, tableName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		Logger.Info("Fetching all the items from Db")
		query := fmt.Sprintf("SELECT `key`, `value`, `expired_at` FROM %s", tableName)
		rows, err := db.Query(query)
		if err != nil {
			Logger.Error("failed to query the db", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to query database",
			})
			return
		}
		defer rows.Close()

		var results []KeyValue
		for rows.Next() {
			var item KeyValue
			var rawJson []byte
			var expiredAt sql.NullTime

			err := rows.Scan(&item.Key, &rawJson, &expiredAt)
			if err != nil {
				Logger.Error("failed to scan row", "error", err)
				continue
			}

			if (len(rawJson)) > 0 {
				if err := json.Unmarshal(rawJson, &item.Value); err != nil {
					Logger.Error("failed to unmarshal json", "error", err)
					continue
				}
			}
			// Assign to pointer
			if expiredAt.Valid {
				item.ExpiredAt = &expiredAt.Time
			} else {
				item.ExpiredAt = nil
			}
			results = append(results, item)
		}

		if len(results) == 0 {
			Logger.Warn("No items found in DB")
			c.JSON(http.StatusNotFound, gin.H{
				"message": "No records found",
				"items":   []KeyValue{},
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"items": results,
		})
	}
}

// Put Key value in DB
func PutKV(db *sql.DB, tableName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		Logger.Info("Inserting Given KV pair into DB")
		var item KeyValue
		if err := c.ShouldBindBodyWithJSON(&item); err != nil {
			Logger.Error("payload validation failed", "error", err)
			c.JSON(http.StatusBadRequest, gin.H{
				"message": "Invalid json payload",
				"error":   err,
			})
			return
		}

		loc, err := time.LoadLocation("Asia/Kolkata")
		if err != nil {
			Logger.Error("failed to load timezone", "error", err)
		}
		ttl := 10 * time.Minute
		expiredAt := time.Now().In(loc).Add(ttl)
		Logger.Info("time", "expired_at", expiredAt)

		// conver value to json
		valueJson, err := json.Marshal(&item.Value)
		if err != nil {
			Logger.Error("failed to convert value to json", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to encode json value",
			})
			return
		}

		query := fmt.Sprintf(`
			INSERT INTO %s (`+"`key`"+`, `+"`value`"+`, expired_at)
			VALUES (?, ?, ?)
			ON DUPLICATE KEY UPDATE 
				`+"`value`"+` = VALUES(`+"`value`"+`),
				expired_at = VALUES(expired_at)
			`, tableName)

		_, err = db.Exec(query, item.Key, valueJson, expiredAt.Format("2006-01-02 15:04:05"))
		if err != nil {
			Logger.Error("failed to insert item into DB", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"message": "Operation failed",
				"error":   err,
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"message": "Operation successfull",
		})

	}
}

func GetKV(db *sql.DB, tableName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.Param("key") // get key from URL
		if key == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Key is required"})
			return
		}
		Logger.Info("Fetching given key values", "key", key)

		query := fmt.Sprintf("SELECT `key`, `value`, `expired_at` FROM %s WHERE `key` = ? AND expired_at > NOW()", tableName)
		row := db.QueryRow(query, key)

		var item KeyValue
		var rawJson []byte
		var expiredAt sql.NullTime

		err := row.Scan(&item.Key, &rawJson, &expiredAt)
		if err != nil {
			if err == sql.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
			} else {
				Logger.Error("failed to scan row", "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			}
			return
		}

		// Parse JSON value
		if len(rawJson) > 0 {
			if err := json.Unmarshal(rawJson, &item.Value); err != nil {
				Logger.Error("failed to unmarshal json", "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode value"})
				return
			}
		}

		// Assign ExpiredAt pointer
		if expiredAt.Valid {
			item.ExpiredAt = &expiredAt.Time
		} else {
			item.ExpiredAt = nil
		}

		c.JSON(http.StatusOK, gin.H{
			"item": item,
		})
	}
}

// Delete given key if exists
func DeleteKV(db *sql.DB, tableName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.Param("key") // get key from URL
		if key == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Key is required"})
			return
		}
		Logger.Info("Soft Deleting given key values", "key", key)

		// so that we know it was soft deleted
		softDeleteTime := "0001-01-01 00:00:00"
		query := fmt.Sprintf(
			"UPDATE %s SET expired_at = ? WHERE `key` = ? AND expired_at > NOW()",
			tableName,
		)

		results, err := db.Exec(query, softDeleteTime, key)
		if err != nil {
			Logger.Error("failed to soft delete given key", "key", key, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"message": "failed to delete given key",
				"error":   err,
			})
			return
		}

		rowsAffected, _ := results.RowsAffected()
		if rowsAffected == 0 {
			Logger.Error("given key does not exist", "key", key)
			c.JSON(http.StatusNotFound, gin.H{
				"message": "given key does not exist. Operation failed.",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "Successfully deleted given key",
		})

	}
}

func main() {

	err := godotenv.Load()
	if err != nil {
		Logger.Error("failed to load env varialbes", "error", err)
		panic(err)
	}

	password := os.Getenv("PASSWORD")
	database := os.Getenv("DATABASE")
	tableName := os.Getenv("TABLE_NAME")

	db := connectDB(password, database)

	router := gin.Default()
	router.GET("/", indexHanlder)
	router.GET("/kv/all", GetAllKV(db, tableName))
	router.GET("kv/:key", GetKV(db, tableName))
	router.POST("/kv", PutKV(db, tableName))
	router.DELETE("/kv/:key", DeleteKV(db, tableName))
	router.Run("localhost:8080")

}
