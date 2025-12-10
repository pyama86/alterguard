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
	GetNewTableRowCount(tableName string) (int64, error)
	GetTableRowCountForSwap(table string) (int64, error)
	GetNewTableRowCountForSwap(tableName string) (int64, error)
	ExecuteAlter(alterStatement string) error
	ExecuteAlterWithDryRun(alterStatement string, dryRun bool) error
	SetSessionConfig(lockWaitTimeout, innodbLockWaitTimeout int) error
	TableExists(tableName string) (bool, error)
	CheckNewTableExists(tableName string) (bool, error)
	HasOtherActiveConnections() (bool, string, error)
	GetCurrentUser() (string, error)
	AnalyzeTable(tableName string) error
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
	var usedMethod string

	// 第一選択: INNODB_SYS_TABLESTATS (MySQL 5.7)
	query := `
		SELECT NUM_ROWS
		FROM information_schema.INNODB_SYS_TABLESTATS
		WHERE NAME = CONCAT(DATABASE(), '/', ?)
	`

	err := c.db.Get(&count, query, table)
	if err != nil {
		// 第二選択: INNODB_TABLESTATS (MySQL 8.0+)
		c.logger.Debugf("Failed to get row count from INNODB_SYS_TABLESTATS for %s, trying INNODB_TABLESTATS: %v", table, err)
		query = `
			SELECT NUM_ROWS
			FROM information_schema.INNODB_TABLESTATS
			WHERE NAME = CONCAT(DATABASE(), '/', ?)
		`
		err = c.db.Get(&count, query, table)
		if err != nil {
			// 第三選択: information_schema.TABLES
			c.logger.Debugf("Failed to get row count from INNODB_TABLESTATS for %s, trying information_schema.TABLES: %v", table, err)
			query = `
				SELECT TABLE_ROWS
				FROM information_schema.TABLES
				WHERE table_schema = DATABASE() AND table_name = ?
			`
			err = c.db.Get(&count, query, table)
			if err != nil {
				// フォールバック: COUNT(*)
				c.logger.Warnf("Failed to get row count from all stats tables for %s, falling back to COUNT(*): %v", table, err)

				countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
				err = c.db.Get(&count, countQuery)
				if err != nil {
					return 0, fmt.Errorf("failed to get table row count for %s: %w", table, err)
				}
				c.logger.Infof("Used COUNT(*) for table %s: %d rows", table, count)
				return count, nil
			}
			usedMethod = "information_schema.TABLES"
			c.logger.Debugf("Used information_schema.TABLES for table %s: %d rows", table, count)
		} else {
			usedMethod = "INNODB_TABLESTATS"
			c.logger.Debugf("Used INNODB_TABLESTATS for table %s: %d rows", table, count)
		}
	} else {
		usedMethod = "INNODB_SYS_TABLESTATS"
		c.logger.Debugf("Used INNODB_SYS_TABLESTATS for table %s: %d rows", table, count)
	}

	// 統計情報が0件の場合は、COUNT(*)で正確な件数を確認
	if count == 0 {
		c.logger.Infof("Stats show 0 rows for table %s (from %s), verifying with COUNT(*)", table, usedMethod)
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
		var actualCount int64
		err = c.db.Get(&actualCount, countQuery)
		if err != nil {
			return 0, fmt.Errorf("failed to verify table row count with COUNT(*) for %s: %w", table, err)
		}
		c.logger.Infof("Verified with COUNT(*) for table %s: %d rows", table, actualCount)
		return actualCount, nil
	}

	return count, nil
}

func (c *MySQLClient) GetNewTableRowCount(tableName string) (int64, error) {
	newTableName := fmt.Sprintf("_%s_new", tableName)
	return c.GetTableRowCount(newTableName)
}

func (c *MySQLClient) GetTableRowCountForSwap(table string) (int64, error) {
	var count int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)

	c.logger.Infof("Getting exact row count for swap using COUNT(*): %s", table)

	err := c.db.Get(&count, countQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to get exact table row count for swap %s: %w", table, err)
	}

	c.logger.Infof("Exact row count for swap table %s: %d rows", table, count)
	return count, nil
}

func (c *MySQLClient) GetNewTableRowCountForSwap(tableName string) (int64, error) {
	newTableName := fmt.Sprintf("_%s_new", tableName)
	return c.GetTableRowCountForSwap(newTableName)
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

func (c *MySQLClient) CheckNewTableExists(tableName string) (bool, error) {
	newTableName := fmt.Sprintf("_%s_new", tableName)
	return c.TableExists(newTableName)
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

func (c *MySQLClient) AnalyzeTable(tableName string) error {
	analyzeSQL := fmt.Sprintf("ANALYZE TABLE `%s`", tableName)
	c.logger.Infof("Executing ANALYZE TABLE: %s", analyzeSQL)
	start := time.Now()

	_, err := c.db.Exec(analyzeSQL)
	duration := time.Since(start)

	if err != nil {
		c.logger.Errorf("ANALYZE TABLE failed (duration: %v): %s - Error: %v", duration, analyzeSQL, err)
		return fmt.Errorf("failed to execute ANALYZE TABLE [%s]: %w", tableName, err)
	}

	c.logger.Infof("ANALYZE TABLE completed (duration: %v): %s", duration, analyzeSQL)
	return nil
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
	var usedMethod string

	// 第一選択: INNODB_SYS_TABLESTATS (MySQL 5.7)
	query := `
		SELECT NUM_ROWS
		FROM information_schema.INNODB_SYS_TABLESTATS
		WHERE NAME = CONCAT(DATABASE(), '/', ?)
	`

	err := db.Get(&count, query, table)
	if err != nil {
		// 第二選択: INNODB_TABLESTATS (MySQL 8.0+)
		c.logger.Debugf("Failed to get row count from INNODB_SYS_TABLESTATS for %s, trying INNODB_TABLESTATS: %v", table, err)
		query = `
			SELECT NUM_ROWS
			FROM information_schema.INNODB_TABLESTATS
			WHERE NAME = CONCAT(DATABASE(), '/', ?)
		`
		err = db.Get(&count, query, table)
		if err != nil {
			// 第三選択: information_schema.TABLES
			c.logger.Debugf("Failed to get row count from INNODB_TABLESTATS for %s, trying information_schema.TABLES: %v", table, err)
			query = `
				SELECT TABLE_ROWS
				FROM information_schema.TABLES
				WHERE table_schema = DATABASE() AND table_name = ?
			`
			err = db.Get(&count, query, table)
			if err != nil {
				// フォールバック: COUNT(*)
				c.logger.Warnf("Failed to get row count from all stats tables for %s, falling back to COUNT(*): %v", table, err)

				countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
				err = db.Get(&count, countQuery)
				if err != nil {
					return 0, fmt.Errorf("failed to get table row count for %s: %w", table, err)
				}
				c.logger.Infof("Used COUNT(*) for table %s: %d rows", table, count)
				return count, nil
			}
			usedMethod = "information_schema.TABLES"
			c.logger.Debugf("Used information_schema.TABLES for table %s: %d rows", table, count)
		} else {
			usedMethod = "INNODB_TABLESTATS"
			c.logger.Debugf("Used INNODB_TABLESTATS for table %s: %d rows", table, count)
		}
	} else {
		usedMethod = "INNODB_SYS_TABLESTATS"
		c.logger.Debugf("Used INNODB_SYS_TABLESTATS for table %s: %d rows", table, count)
	}

	// 統計情報が0件の場合は、COUNT(*)で正確な件数を確認
	if count == 0 {
		c.logger.Infof("Stats show 0 rows for table %s (from %s), verifying with COUNT(*)", table, usedMethod)
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
		var actualCount int64
		err = db.Get(&actualCount, countQuery)
		if err != nil {
			return 0, fmt.Errorf("failed to verify table row count with COUNT(*) for %s: %w", table, err)
		}
		c.logger.Infof("Verified with COUNT(*) for table %s: %d rows", table, actualCount)
		return actualCount, nil
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

func (c *MySQLClient) getTableRowCountForSwapWithDB(db DBExecutor, table string) (int64, error) {
	var count int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)

	c.logger.Infof("Getting exact row count for swap using COUNT(*): %s", table)

	err := db.Get(&count, countQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to get exact table row count for swap %s: %w", table, err)
	}

	c.logger.Infof("Exact row count for swap table %s: %d rows", table, count)
	return count, nil
}
