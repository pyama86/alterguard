package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Client interface {
	GetTableRowCount(table string) (int64, error)
	ExecuteAlter(alterStatement string) error
	ExecuteAlterWithDryRun(alterStatement string, dryRun bool) error
	CheckMetadataLock(table string, thresholdSeconds int) (bool, error)
	Close() error
}

func IsDuplicateError(err error) bool {
	if mysqlErr, ok := err.(*mysql.MySQLError); ok {
		return mysqlErr.Number == 1062 || // Duplicate entry
			mysqlErr.Number == 1061 || // Duplicate key name
			mysqlErr.Number == 1050 // Table already exists
	}
	return false
}

type MySQLClient struct {
	db *sqlx.DB
}

func NewMySQLClient(dsn string) (*MySQLClient, error) {
	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &MySQLClient{db: db}, nil
}

func (c *MySQLClient) GetTableRowCount(table string) (int64, error) {
	var count int64
	query := `
		SELECT table_rows
		FROM information_schema.TABLES
		WHERE table_schema = DATABASE() AND table_name = ?
	`

	err := c.db.Get(&count, query, table)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("table not found: %s", table)
		}
		return 0, fmt.Errorf("failed to get table row count for %s: %w", table, err)
	}

	return count, nil
}

func (c *MySQLClient) ExecuteAlter(alterStatement string) error {
	_, err := c.db.Exec(alterStatement)
	if err != nil {
		return fmt.Errorf("failed to execute ALTER statement [%s]: %w", alterStatement, err)
	}
	return nil
}

func (c *MySQLClient) ExecuteAlterWithDryRun(alterStatement string, dryRun bool) error {
	if dryRun {
		return nil
	}
	return c.ExecuteAlter(alterStatement)
}

func (c *MySQLClient) CheckMetadataLock(table string, thresholdSeconds int) (bool, error) {
	var lockCount int
	query := `
		SELECT COUNT(*)
		FROM performance_schema.metadata_locks
		WHERE object_name = ?
		AND lock_duration = 'TRANSACTION'
		AND lock_status = 'PENDING'
	`

	start := time.Now()
	for time.Since(start) < time.Duration(thresholdSeconds)*time.Second {
		err := c.db.Get(&lockCount, query, table)
		if err != nil {
			return false, fmt.Errorf("failed to check metadata lock for %s: %w", table, err)
		}

		if lockCount == 0 {
			return false, nil
		}

		time.Sleep(1 * time.Second)
	}

	return true, nil
}

func (c *MySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

type DBExecutor interface {
	Get(dest any, query string, args ...any) error
	Exec(query string, args ...any) (sql.Result, error)
}

func (c *MySQLClient) getTableRowCountWithDB(db DBExecutor, table string) (int64, error) {
	var count int64
	query := `
		SELECT table_rows
		FROM information_schema.TABLES
		WHERE table_schema = DATABASE() AND table_name = ?
	`

	err := db.Get(&count, query, table)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("table not found: %s", table)
		}
		return 0, fmt.Errorf("failed to get table row count for %s: %w", table, err)
	}

	return count, nil
}

func (c *MySQLClient) executeAlterWithDB(db DBExecutor, alterStatement string) error {
	_, err := db.Exec(alterStatement)
	if err != nil {
		return fmt.Errorf("failed to execute ALTER statement [%s]: %w", alterStatement, err)
	}
	return nil
}
