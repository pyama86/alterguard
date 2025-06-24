package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

type Client interface {
	GetTableRowCount(table string) (int64, error)
	ExecuteAlter(alterStatement string) error
	ExecuteAlterWithDryRun(alterStatement string, dryRun bool) error
	CheckMetadataLock(table string, thresholdSeconds int) (bool, error)
	SetSessionConfig(lockWaitTimeout, innodbLockWaitTimeout int) error
	TableExists(tableName string) (bool, error)
	HasOtherActiveConnections() (bool, string, error)
	GetCurrentUser() (string, error)
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
	db     *sqlx.DB
	logger *logrus.Logger
}

func NewMySQLClient(dsn string, logger *logrus.Logger) (*MySQLClient, error) {
	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &MySQLClient{db: db, logger: logger}, nil
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
	c.logger.Infof("Executing SQL: %s", alterStatement)
	start := time.Now()

	_, err := c.db.Exec(alterStatement)
	duration := time.Since(start)

	if err != nil {
		c.logger.Errorf("SQL execution failed (duration: %v): %s - Error: %v", duration, alterStatement, err)
		return fmt.Errorf("failed to execute ALTER statement [%s]: %w", alterStatement, err)
	}

	c.logger.Infof("SQL execution completed (duration: %v): %s", duration, alterStatement)
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

func (c *MySQLClient) SetSessionConfig(lockWaitTimeout, innodbLockWaitTimeout int) error {
	if lockWaitTimeout > 0 {
		query := fmt.Sprintf("SET SESSION lock_wait_timeout = %d", lockWaitTimeout)
		c.logger.Infof("Executing SQL: %s", query)
		start := time.Now()

		if _, err := c.db.Exec(query); err != nil {
			duration := time.Since(start)
			c.logger.Errorf("SQL execution failed (duration: %v): %s - Error: %v", duration, query, err)
			return fmt.Errorf("failed to set lock_wait_timeout: %w", err)
		}

		duration := time.Since(start)
		c.logger.Infof("SQL execution completed (duration: %v): %s", duration, query)
	}

	if innodbLockWaitTimeout > 0 {
		query := fmt.Sprintf("SET SESSION innodb_lock_wait_timeout = %d", innodbLockWaitTimeout)
		c.logger.Infof("Executing SQL: %s", query)
		start := time.Now()

		if _, err := c.db.Exec(query); err != nil {
			duration := time.Since(start)
			c.logger.Errorf("SQL execution failed (duration: %v): %s - Error: %v", duration, query, err)
			return fmt.Errorf("failed to set innodb_lock_wait_timeout: %w", err)
		}

		duration := time.Since(start)
		c.logger.Infof("SQL execution completed (duration: %v): %s", duration, query)
	}

	return nil
}

func (c *MySQLClient) TableExists(tableName string) (bool, error) {
	var count int
	query := `
		SELECT COUNT(*)
		FROM information_schema.TABLES
		WHERE table_schema = DATABASE() AND table_name = ?
	`

	err := c.db.Get(&count, query, tableName)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence for %s: %w", tableName, err)
	}

	return count > 0, nil
}

func (c *MySQLClient) HasOtherActiveConnections() (bool, string, error) {
	currentUser, err := c.GetCurrentUser()
	if err != nil {
		return false, "", fmt.Errorf("failed to get current user: %w", err)
	}

	var currentConnectionID int64
	err = c.db.Get(&currentConnectionID, "SELECT CONNECTION_ID()")
	if err != nil {
		return false, currentUser, fmt.Errorf("failed to get current connection ID: %w", err)
	}

	var otherConnections int
	query := `
		SELECT COUNT(*)
		FROM information_schema.PROCESSLIST
		WHERE USER = ? AND ID != ?
	`

	err = c.db.Get(&otherConnections, query, currentUser, currentConnectionID)
	if err != nil {
		return false, currentUser, fmt.Errorf("failed to check other active connections: %w", err)
	}

	return otherConnections > 0, currentUser, nil
}

func (c *MySQLClient) GetCurrentUser() (string, error) {
	var user string
	err := c.db.Get(&user, "SELECT USER()")
	if err != nil {
		return "", fmt.Errorf("failed to get current user: %w", err)
	}

	// USER()は'user@host'形式で返すので、@より前の部分を取得
	if idx := strings.Index(user, "@"); idx != -1 {
		user = user[:idx]
	}

	return user, nil
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
