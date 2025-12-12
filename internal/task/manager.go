package task

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/pyama86/alterguard/internal/database"
	"github.com/pyama86/alterguard/internal/ptarchiver"
	"github.com/pyama86/alterguard/internal/ptosc"
	"github.com/pyama86/alterguard/internal/slack"
	"github.com/sirupsen/logrus"
)

type Manager struct {
	db         database.Client
	ptosc      ptosc.Executor
	ptarchiver ptarchiver.Executor
	slack      slack.Notifier
	logger     *logrus.Logger
	config     *config.Config
	dryRun     bool
}

type QueryResult struct {
	Query    string
	Duration time.Duration
	Success  bool
	Error    error
}

type QueryInfo struct {
	Query     string
	QueryType string
	TableName string
}

type TableGroup struct {
	TableName    string
	AlterParts   []string
	OtherQueries []QueryInfo
	RowCount     int64
}

func NewManager(db database.Client, ptoscExec ptosc.Executor, ptarchiverExec ptarchiver.Executor, slackNotifier slack.Notifier, logger *logrus.Logger, cfg *config.Config, dryRun bool) *Manager {
	return &Manager{
		db:         db,
		ptosc:      ptoscExec,
		ptarchiver: ptarchiverExec,
		slack:      slackNotifier,
		logger:     logger,
		config:     cfg,
		dryRun:     dryRun,
	}
}

func (m *Manager) extractDatabaseNameFromDSN() (string, error) {
	dsn := m.config.DSN
	parts := strings.Split(dsn, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid DSN format: %s", dsn)
	}

	dbPart := parts[len(parts)-1]

	if strings.Contains(dbPart, "?") {
		dbPart = strings.Split(dbPart, "?")[0]
	}

	if dbPart == "" {
		return "", fmt.Errorf("database name not found in DSN: %s", dsn)
	}

	return dbPart, nil
}

func (m *Manager) ExecuteAllTasks() error {
	m.logger.Infof("Starting execution of %d queries", len(m.config.Queries))

	queries, err := m.parseQueries(m.config.Queries)
	if err != nil {
		return fmt.Errorf("failed to parse queries: %w", err)
	}

	tableGroups := m.groupQueriesByTable(queries)

	for tableName, group := range tableGroups {
		if err := m.executeTableGroup(tableName, group); err != nil {
			return fmt.Errorf("failed to execute queries for table %s: %w", tableName, err)
		}
	}

	// テーブル指定がないクエリを実行する
	for _, query := range queries {
		if query.TableName == "" {
			cleanedQuery := strings.ReplaceAll(query.Query, "`", "")
			quotedQuery := fmt.Sprintf("`%s`", cleanedQuery)
			taskName := "non-table-query"
			if m.dryRun {
				taskName = "non-table-query (DRY RUN)"
			}
			if err := m.slack.NotifyStartWithQuery(taskName, query.TableName, quotedQuery, 0); err != nil {
				m.logger.Errorf("Failed to send start notification: %v", err)
			}

			start := time.Now()
			if err := m.executeQuery(&query, "non-table-query"); err != nil {
				if slackErr := m.slack.NotifyFailureWithQuery(taskName, query.TableName, quotedQuery, 0, err); slackErr != nil {
					m.logger.Errorf("Failed to send failure notification: %v", slackErr)
				}
				return fmt.Errorf("failed to execute query: %w", err)
			}

			duration := time.Since(start)
			if err := m.slack.NotifySuccessWithQuery(taskName, query.TableName, quotedQuery, 0, duration); err != nil {
				m.logger.Errorf("Failed to send success notification: %v", err)
			}
		}
	}

	m.logger.Info("All queries completed successfully")
	return nil
}

func (m *Manager) groupQueriesByTable(queries []QueryInfo) map[string]*TableGroup {
	groups := make(map[string]*TableGroup)

	for _, query := range queries {
		if query.TableName == "" {
			continue
		}

		if _, exists := groups[query.TableName]; !exists {
			groups[query.TableName] = &TableGroup{
				TableName:    query.TableName,
				AlterParts:   []string{},
				OtherQueries: []QueryInfo{},
			}
		}

		if query.QueryType == "ALTER" {
			alterPart := m.extractAlterStatement(query.Query)
			if alterPart != "" {
				groups[query.TableName].AlterParts = append(groups[query.TableName].AlterParts, alterPart)
			}
		} else {
			groups[query.TableName].OtherQueries = append(groups[query.TableName].OtherQueries, query)
		}
	}

	return groups
}

func (m *Manager) executeTableGroup(tableName string, group *TableGroup) error {
	m.logger.Infof("Processing table: %s", tableName)

	if err := m.executeSmallQueries(group.OtherQueries); err != nil {
		return err
	}

	if len(group.AlterParts) == 0 {
		return nil
	}

	rowCount, err := m.db.GetTableRowCount(tableName)
	if err != nil {
		m.logger.Warnf("Failed to get row count for table %s, treating as small query: %v", tableName, err)
		return m.executeAlterPartsAsSmallQueries(tableName, group.AlterParts)
	}

	threshold := m.config.Common.PtOscThreshold
	m.logger.Infof("Table %s has %d rows (threshold: %d)", tableName, rowCount, threshold)

	if rowCount <= threshold {
		return m.executeAlterPartsAsSmallQueries(tableName, group.AlterParts)
	} else {
		return m.executeLargeAlterQuery(tableName, group.AlterParts, rowCount)
	}
}

func (m *Manager) executeAlterPartsAsSmallQueries(tableName string, alterParts []string) error {
	taskName := "alter-table"
	if m.dryRun {
		taskName = "alter-table (DRY RUN)"
	}

	if err := m.checkOtherActiveConnections(taskName, tableName); err != nil {
		return err
	}

	rowCount, err := m.db.GetTableRowCount(tableName)
	if err != nil {
		m.logger.Warnf("Failed to get row count for table %s: %v", tableName, err)
		rowCount = 0
	}

	cleanedQuery := strings.ReplaceAll(fmt.Sprintf("ALTER TABLE %s %s", tableName, strings.Join(alterParts, ", ")), "`", "")
	combinedQuery := fmt.Sprintf("`%s`", cleanedQuery)

	if err := m.slack.NotifyStartWithQuery(taskName, tableName, combinedQuery, rowCount); err != nil {
		m.logger.Errorf("Failed to send start notification: %v", err)
	}

	start := time.Now()
	for _, alterPart := range alterParts {
		query := fmt.Sprintf("ALTER TABLE %s %s", tableName, alterPart)
		queryInfo := QueryInfo{
			Query:     query,
			QueryType: "ALTER",
			TableName: tableName,
		}
		if err := m.executeQuery(&queryInfo, "alter-table"); err != nil {
			if slackErr := m.slack.NotifyFailureWithQuery(taskName, tableName, combinedQuery, rowCount, err); slackErr != nil {
				m.logger.Errorf("Failed to send failure notification: %v", slackErr)
			}
			return err
		}
	}

	duration := time.Since(start)
	if err := m.slack.NotifySuccessWithQuery(taskName, tableName, combinedQuery, rowCount, duration); err != nil {
		m.logger.Errorf("Failed to send success notification: %v", err)
	}

	return nil
}

func (m *Manager) executeLargeAlterQuery(tableName string, alterParts []string, rowCount int64) error {
	taskName := "pt-osc"
	if m.dryRun {
		taskName = "pt-osc (DRY RUN)"
	}

	if err := m.checkOtherActiveConnections(taskName, tableName); err != nil {
		return err
	}

	if err := m.checkNewTableExists(taskName, tableName); err != nil {
		return err
	}

	combinedAlter := strings.Join(alterParts, ", ")
	cleanedAlterQuery := strings.ReplaceAll(fmt.Sprintf("ALTER TABLE %s %s", tableName, combinedAlter), "`", "")
	alterQuery := fmt.Sprintf("`%s`", cleanedAlterQuery)

	// Build detailed pt-osc command with actual parameters
	var ptOscCommand string
	if ptOscExecutor, ok := m.ptosc.(*ptosc.PtOscExecutor); ok {
		ptOscArgs, _, err := ptOscExecutor.BuildArgsWithPassword(tableName, combinedAlter, m.config.Common.PtOsc, m.config.DSN, m.dryRun)
		if err != nil {
			m.logger.Warnf("Failed to build pt-osc args for notification: %v", err)
			cleanedPtOscCommand := strings.ReplaceAll(fmt.Sprintf("pt-online-schema-change --alter='%s' --execute", combinedAlter), "`", "")
			ptOscCommand = fmt.Sprintf("`%s`", cleanedPtOscCommand)
		} else {
			cleanedPtOscCommand := strings.ReplaceAll(fmt.Sprintf("pt-online-schema-change %s", strings.Join(ptOscArgs, " ")), "`", "")
			ptOscCommand = fmt.Sprintf("`%s`", cleanedPtOscCommand)
		}
	} else {
		// For testing or other implementations
		cleanedPtOscCommand := strings.ReplaceAll(fmt.Sprintf("pt-online-schema-change --alter='%s' --execute", combinedAlter), "`", "")
		ptOscCommand = fmt.Sprintf("`%s`", cleanedPtOscCommand)
	}

	queryInfo := fmt.Sprintf("ALTER: %s\npt-osc: %s", alterQuery, ptOscCommand)

	m.logger.Infof("Executing pt-online-schema-change for table %s (rows: %d)", tableName, rowCount)

	if err := m.slack.NotifyStartWithQuery(taskName, tableName, queryInfo, rowCount); err != nil {
		m.logger.Errorf("Failed to send start notification: %v", err)
	}

	start := time.Now()

	if m.dryRun {
		dryRunResult, err := m.ptosc.ExecuteAlterWithDryRunResult(tableName, combinedAlter, m.config.Common.PtOsc, m.config.DSN, m.dryRun)
		if err != nil {
			if slackErr := m.slack.NotifyFailureWithQuery(taskName, tableName, queryInfo, rowCount, err); slackErr != nil {
				m.logger.Errorf("Failed to send failure notification: %v", slackErr)
			}
			return fmt.Errorf("pt-online-schema-change dry run failed: %w", err)
		}

		duration := time.Since(start)
		if dryRunResult != nil {
			slackDryRunResult := &slack.DryRunResult{
				EstimatedTime:    dryRunResult.EstimatedTime,
				AffectedRows:     dryRunResult.AffectedRows,
				ChunkCount:       dryRunResult.ChunkCount,
				ValidationResult: dryRunResult.ValidationResult,
				Warnings:         dryRunResult.Warnings,
				Summary:          dryRunResult.Summary,
			}
			if err := m.slack.NotifyDryRunResult(taskName, tableName, slackDryRunResult, duration); err != nil {
				m.logger.Errorf("Failed to send dry run result notification: %v", err)
			}
		} else {
			if err := m.slack.NotifySuccessWithQuery(taskName, tableName, queryInfo, rowCount, duration); err != nil {
				m.logger.Errorf("Failed to send success notification: %v", err)
			}
		}
	} else {
		if err := m.ptosc.ExecuteAlter(tableName, combinedAlter, m.config.Common.PtOsc, m.config.DSN, m.dryRun); err != nil {
			var ptOscLog string
			if ptOscExecutor, ok := m.ptosc.(*ptosc.PtOscExecutor); ok {
				ptOscLog = ptOscExecutor.GetOutputSummary()
			}
			if slackErr := m.slack.NotifyFailureWithQueryAndLog(taskName, tableName, queryInfo, rowCount, err, ptOscLog); slackErr != nil {
				m.logger.Errorf("Failed to send failure notification: %v", slackErr)
			}
			return fmt.Errorf("pt-online-schema-change failed: %w", err)
		}

		duration := time.Since(start)
		var ptOscLog string
		if ptOscExecutor, ok := m.ptosc.(*ptosc.PtOscExecutor); ok {
			ptOscLog = ptOscExecutor.GetOutputSummary()
		}

		newRowCount, err := m.db.GetNewTableRowCount(tableName)
		if err != nil {
			m.logger.Warnf("Failed to get new table row count for %s: %v", tableName, err)
			if slackErr := m.slack.NotifySuccessWithQueryAndLog(taskName, tableName, queryInfo, rowCount, duration, ptOscLog); slackErr != nil {
				m.logger.Errorf("Failed to send success notification: %v", slackErr)
			}
		} else {
			m.logger.Infof("pt-osc completed for table %s: original=%d, new=%d", tableName, rowCount, newRowCount)
			if err := m.slack.NotifyPtOscCompletionWithNewTableCount(taskName, tableName, rowCount, newRowCount, duration, ptOscLog); err != nil {
				m.logger.Errorf("Failed to send completion notification: %v", err)
			}
		}
	}

	return nil
}

func (m *Manager) executeSmallQueries(queries []QueryInfo) error {
	for _, queryInfo := range queries {
		m.logger.Infof("Executing query: %s", queryInfo.Query)

		var rowCount int64 = 0
		if queryInfo.TableName != "" {
			if count, err := m.db.GetTableRowCount(queryInfo.TableName); err == nil {
				rowCount = count
			}
		}

		cleanedQuery := strings.ReplaceAll(queryInfo.Query, "`", "")
		quotedQuery := fmt.Sprintf("`%s`", cleanedQuery)
		taskName := "small-query"
		if m.dryRun {
			taskName = "small-query (DRY RUN)"
		}
		if err := m.slack.NotifyStartWithQuery(taskName, queryInfo.TableName, quotedQuery, rowCount); err != nil {
			m.logger.Errorf("Failed to send start notification: %v", err)
		}

		start := time.Now()
		if err := m.executeQuery(&queryInfo, "small-query"); err != nil {
			if slackErr := m.slack.NotifyFailureWithQuery(taskName, queryInfo.TableName, quotedQuery, rowCount, err); slackErr != nil {
				m.logger.Errorf("Failed to send failure notification: %v", slackErr)
			}
			return err
		}

		duration := time.Since(start)
		if err := m.slack.NotifySuccessWithQuery(taskName, queryInfo.TableName, quotedQuery, rowCount, duration); err != nil {
			m.logger.Errorf("Failed to send success notification: %v", err)
		}
	}
	return nil
}

func (m *Manager) executeQuery(queryInfo *QueryInfo, taskName string) error {
	if m.dryRun {
		m.logger.Infof("[DRY RUN] Would execute SQL: %s", queryInfo.Query)
		return nil
	}

	if err := m.db.ExecuteAlter(queryInfo.Query); err != nil {
		if database.IsDuplicateError(err) {
			warning := fmt.Sprintf("Duplicate detected in %s: %s (query: %s)", taskName, err.Error(), queryInfo.Query)
			m.logger.Warn(warning)

			if slackErr := m.slack.NotifyWarning(taskName, queryInfo.TableName, warning); slackErr != nil {
				m.logger.Errorf("Failed to send warning notification: %v", slackErr)
			}

			return nil
		}
		return err
	}
	return nil
}

func (m *Manager) parseQueries(queries []string) ([]QueryInfo, error) {
	var result []QueryInfo
	for _, query := range queries {
		queryType, err := m.getQueryType(query)
		if err != nil {
			return nil, err
		}

		queryInfo := QueryInfo{
			Query:     strings.TrimSpace(query),
			TableName: m.extractTableName(query),
			QueryType: queryType,
		}
		result = append(result, queryInfo)
	}

	return result, nil
}

func (m *Manager) getQueryType(query string) (string, error) {
	query = strings.TrimSpace(strings.ToUpper(query))
	if strings.HasPrefix(query, "CREATE") {
		return "CREATE", nil
	} else if strings.HasPrefix(query, "ALTER") {
		return "ALTER", nil
	} else if strings.HasPrefix(query, "DROP") {
		return "DROP", nil
	}
	return "", fmt.Errorf("unsupported type query:%s", query)
}

func (m *Manager) extractTableName(query string) string {
	query = strings.TrimSpace(query)

	createTableRe := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + "`" + `?([^` + "`" + `\s]+)` + "`" + `?`)
	if matches := createTableRe.FindStringSubmatch(query); len(matches) > 1 {
		return strings.Trim(matches[1], "`")
	}

	alterTableRe := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + "`" + `?([^` + "`" + `\s]+)` + "`" + `?`)
	if matches := alterTableRe.FindStringSubmatch(query); len(matches) > 1 {
		return strings.Trim(matches[1], "`")
	}

	dropTableRe := regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?` + "`" + `?([^` + "`" + `\s]+)` + "`" + `?`)
	if matches := dropTableRe.FindStringSubmatch(query); len(matches) > 1 {
		return strings.Trim(matches[1], "`")
	}

	return ""
}

func (m *Manager) extractAlterStatement(query string) string {
	alterTableRe := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + "`" + `?[^` + "`" + `\s]+` + "`" + `?\s+(.+)`)
	if matches := alterTableRe.FindStringSubmatch(query); len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func (m *Manager) SwapTable(tableName string) error {
	m.logger.Infof("Starting table swap for %s", tableName)

	taskName := "swap"
	if m.dryRun {
		taskName = "swap (DRY RUN)"
	}

	if err := m.checkOtherActiveConnections(taskName, tableName); err != nil {
		return err
	}

	originalTableExists, err := m.db.TableExists(tableName)
	if err != nil {
		m.logger.Errorf("Failed to check original table existence: %v", err)
		return fmt.Errorf("failed to check original table existence: %w", err)
	}
	if !originalTableExists {
		return fmt.Errorf("original table %s does not exist", tableName)
	}

	newTableName := fmt.Sprintf("_%s_new", tableName)
	newTableExists, err := m.db.TableExists(newTableName)
	if err != nil {
		m.logger.Errorf("Failed to check new table existence: %v", err)
		return fmt.Errorf("failed to check new table existence: %w", err)
	}
	if !newTableExists {
		return fmt.Errorf("new table %s does not exist", newTableName)
	}

	m.logger.Infof("Both tables exist: %s and %s", tableName, newTableName)

	// レコード件数チェック（5%の閾値でハードコーディング）
	if err := m.checkRowCountDifference(tableName); err != nil {
		return err
	}

	// swap前にnewテーブルに対してANALYZE TABLEを実行
	if !m.config.Common.DisableAnalyzeTable {
		newTableName := fmt.Sprintf("_%s_new", tableName)
		if m.dryRun {
			m.logger.Infof("[DRY RUN] Would execute ANALYZE TABLE for %s before swap", newTableName)
		} else {
			m.logger.Infof("Executing ANALYZE TABLE for %s before swap", newTableName)
			if err := m.db.AnalyzeTable(newTableName); err != nil {
				m.logger.Warnf("ANALYZE TABLE failed for %s: %v", newTableName, err)
			}
		}
	}

	swapSQL := fmt.Sprintf("RENAME TABLE %s TO %s_old, _%s_new TO %s",
		tableName, tableName, tableName, tableName)
	cleanedQuery := strings.ReplaceAll(swapSQL, "`", "")
	quotedQuery := fmt.Sprintf("`%s`", cleanedQuery)

	if err := m.slack.NotifyStartWithQuery(taskName, tableName, quotedQuery, 0); err != nil {
		m.logger.Errorf("Failed to send start notification: %v", err)
	}

	start := time.Now()

	if err := m.db.SetSessionConfig(m.config.Common.SessionConfig.LockWaitTimeout, m.config.Common.SessionConfig.InnodbLockWaitTimeout); err != nil {
		m.logger.Errorf("Failed to set session config: %v", err)
		if slackErr := m.slack.NotifyFailureWithQuery(taskName, tableName, quotedQuery, 0, err); slackErr != nil {
			m.logger.Errorf("Failed to send failure notification: %v", slackErr)
		}
		return fmt.Errorf("failed to set session config: %w", err)
	}

	if m.dryRun {
		m.logger.Infof("[DRY RUN] Would execute SQL: %s", swapSQL)
		duration := time.Since(start)
		if err := m.slack.NotifySuccessWithQuery(taskName, tableName, quotedQuery, 0, duration); err != nil {
			m.logger.Errorf("Failed to send success notification: %v", err)
		}
		return nil
	}

	// Start concurrent execution time monitoring
	thresholdSeconds := m.config.Common.Alert.ExecutionTimeThresholdSeconds
	if thresholdSeconds > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			timer := time.NewTimer(time.Duration(thresholdSeconds) * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				warning := fmt.Sprintf("Long execution time detected in %s: operation is taking longer than %d seconds for query: %s",
					taskName, thresholdSeconds, quotedQuery)
				m.logger.Warn(warning)
				if slackErr := m.slack.NotifyWarning(taskName, tableName, warning); slackErr != nil {
					m.logger.Errorf("Failed to send execution time warning notification: %v", slackErr)
				}
			case <-ctx.Done():
				return
			}
		}()
	}

	if err := m.db.ExecuteAlter(swapSQL); err != nil {
		if slackErr := m.slack.NotifyFailureWithQuery(taskName, tableName, quotedQuery, 0, err); slackErr != nil {
			m.logger.Errorf("Failed to send failure notification: %v", slackErr)
		}
		return fmt.Errorf("table swap failed: %w", err)
	}

	duration := time.Since(start)

	if err := m.slack.NotifySuccessWithQuery(taskName, tableName, quotedQuery, 0, duration); err != nil {
		m.logger.Errorf("Failed to send success notification: %v", err)
	}

	m.logger.Infof("Table swap completed for %s", tableName)
	return nil
}

func (m *Manager) CleanupOldTable(tableName string) error {
	m.logger.Infof("Starting cleanup for table %s", tableName)

	// pt-archiverが有効な場合、DROP前にデータを削除
	if m.config.Common.PtArchiver.Enabled {
		oldTableName := fmt.Sprintf("%s_old", tableName)
		if err := m.PurgeOldTable(oldTableName); err != nil {
			return fmt.Errorf("failed to purge old table before cleanup: %w", err)
		}
	}

	// バッファプールサイズチェック（閾値が設定されている場合）
	if m.config.Common.BufferPoolSizeThresholdMB > 0 {
		dbName, err := m.extractDatabaseNameFromDSN()
		if err != nil {
			return fmt.Errorf("failed to extract database name from DSN: %w", err)
		}

		oldTableName := fmt.Sprintf("%s_old", tableName)
		bufferPoolSizeMB, err := m.db.GetTableBufferPoolSizeMB(dbName, oldTableName)
		if err != nil {
			m.logger.Warnf("Failed to get buffer pool size for table %s: %v", oldTableName, err)
		} else {
			m.logger.Infof("Buffer pool size for table %s: %.2f MB (threshold: %.2f MB)",
				oldTableName, bufferPoolSizeMB, m.config.Common.BufferPoolSizeThresholdMB)

			if bufferPoolSizeMB > m.config.Common.BufferPoolSizeThresholdMB {
				errMsg := fmt.Sprintf(
					"buffer pool size (%.2f MB) exceeds threshold (%.2f MB) for table %s",
					bufferPoolSizeMB, m.config.Common.BufferPoolSizeThresholdMB, oldTableName)
				m.logger.Errorf("Buffer pool size check failed: %s", errMsg)
				return fmt.Errorf("buffer pool size check failed: %s", errMsg)
			}
		}
	}

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s_old", tableName)
	cleanedQuery := strings.ReplaceAll(dropSQL, "`", "")
	quotedQuery := fmt.Sprintf("`%s`", cleanedQuery)

	taskName := "cleanup"
	if m.dryRun {
		taskName = "cleanup (DRY RUN)"
	}

	if err := m.slack.NotifyStartWithQuery(taskName, tableName, quotedQuery, 0); err != nil {
		m.logger.Errorf("Failed to send start notification: %v", err)
	}

	start := time.Now()

	if m.dryRun {
		m.logger.Infof("[DRY RUN] Would execute SQL: %s", dropSQL)
		duration := time.Since(start)
		if err := m.slack.NotifySuccessWithQuery(taskName, tableName, quotedQuery, 0, duration); err != nil {
			m.logger.Errorf("Failed to send success notification: %v", err)
		}
		return nil
	}

	if err := m.db.ExecuteAlter(dropSQL); err != nil {
		if slackErr := m.slack.NotifyFailureWithQuery(taskName, tableName, quotedQuery, 0, err); slackErr != nil {
			m.logger.Errorf("Failed to send failure notification: %v", slackErr)
		}
		return fmt.Errorf("failed to drop backup table: %w", err)
	}

	duration := time.Since(start)
	if err := m.slack.NotifySuccessWithQuery(taskName, tableName, quotedQuery, 0, duration); err != nil {
		m.logger.Errorf("Failed to send success notification: %v", err)
	}

	m.logger.Infof("Cleanup completed for table %s", tableName)
	return nil
}

func (m *Manager) PurgeOldTable(tableName string) error {
	m.logger.Infof("Starting purge for table %s using pt-archiver", tableName)

	taskName := "pt-archiver"
	if m.dryRun {
		taskName = "pt-archiver (DRY RUN)"
	}

	ptArchiverCommand := m.buildPtArchiverCommand(tableName)
	cleanedCommand := strings.ReplaceAll(ptArchiverCommand, "`", "")
	quotedCommand := fmt.Sprintf("`%s`", cleanedCommand)

	if err := m.slack.NotifyStartWithQuery(taskName, tableName, quotedCommand, 0); err != nil {
		m.logger.Errorf("Failed to send start notification: %v", err)
	}

	start := time.Now()

	if err := m.ptarchiver.ExecutePurge(tableName, m.config.Common.PtArchiver, m.config.DSN, m.dryRun); err != nil {
		if slackErr := m.slack.NotifyFailureWithQuery(taskName, tableName, quotedCommand, 0, err); slackErr != nil {
			m.logger.Errorf("Failed to send failure notification: %v", slackErr)
		}
		return fmt.Errorf("pt-archiver failed: %w", err)
	}

	duration := time.Since(start)

	var ptArchiverLog string
	if ptArchiverExecutor, ok := m.ptarchiver.(*ptarchiver.PtArchiverExecutor); ok {
		ptArchiverLog = ptArchiverExecutor.GetOutputSummary()
	}

	if ptArchiverLog != "" {
		if err := m.slack.NotifySuccessWithQueryAndLog(taskName, tableName, quotedCommand, 0, duration, ptArchiverLog); err != nil {
			m.logger.Errorf("Failed to send success notification: %v", err)
		}
	} else {
		if err := m.slack.NotifySuccessWithQuery(taskName, tableName, quotedCommand, 0, duration); err != nil {
			m.logger.Errorf("Failed to send success notification: %v", err)
		}
	}

	m.logger.Infof("Purge completed for table %s", tableName)
	return nil
}

func (m *Manager) buildPtArchiverCommand(tableName string) string {
	cfg := m.config.Common.PtArchiver
	var args []string

	args = append(args, "--source=h=HOST,P=PORT,D=DATABASE,t="+tableName)
	args = append(args, "--user=USER")

	if cfg.Where != "" {
		args = append(args, fmt.Sprintf("--where=%s", cfg.Where))
	} else {
		args = append(args, "--where=1=1")
	}

	if cfg.Limit > 0 {
		args = append(args, fmt.Sprintf("--limit=%d", cfg.Limit))
	}

	if cfg.CommitEach {
		args = append(args, "--commit-each")
	}

	args = append(args, "--purge")

	if cfg.Progress > 0 {
		args = append(args, fmt.Sprintf("--progress=%d", cfg.Progress))
	}

	if cfg.MaxLag > 0 {
		args = append(args, fmt.Sprintf("--max-lag=%f", cfg.MaxLag))
	}

	if cfg.NoCheckCharset {
		args = append(args, "--no-check-charset")
	}

	if cfg.BulkDelete {
		args = append(args, "--bulk-delete")
	}

	if cfg.PrimaryKeyOnly {
		args = append(args, "--primary-key-only")
	}

	if cfg.Statistics {
		args = append(args, "--statistics")
	}

	if m.dryRun {
		args = append(args, "--dry-run")
	}

	return "pt-archiver " + strings.Join(args, " ")
}

func (m *Manager) CleanupNewTable(tableName string) error {
	m.logger.Infof("Starting new table cleanup for table %s", tableName)

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS _%s_new", tableName)
	cleanedQuery := strings.ReplaceAll(dropSQL, "`", "")
	quotedQuery := fmt.Sprintf("`%s`", cleanedQuery)

	taskName := "new-table-cleanup"
	if m.dryRun {
		taskName = "new-table-cleanup (DRY RUN)"
	}

	if err := m.slack.NotifyStartWithQuery(taskName, tableName, quotedQuery, 0); err != nil {
		m.logger.Errorf("Failed to send start notification: %v", err)
	}

	start := time.Now()

	if m.dryRun {
		m.logger.Infof("[DRY RUN] Would execute SQL: %s", dropSQL)
		duration := time.Since(start)
		if err := m.slack.NotifySuccessWithQuery(taskName, tableName, quotedQuery, 0, duration); err != nil {
			m.logger.Errorf("Failed to send success notification: %v", err)
		}
		return nil
	}

	if err := m.db.ExecuteAlter(dropSQL); err != nil {
		if slackErr := m.slack.NotifyFailureWithQuery(taskName, tableName, quotedQuery, 0, err); slackErr != nil {
			m.logger.Errorf("Failed to send failure notification: %v", slackErr)
		}
		return fmt.Errorf("failed to drop new table: %w", err)
	}

	duration := time.Since(start)
	if err := m.slack.NotifySuccessWithQuery(taskName, tableName, quotedQuery, 0, duration); err != nil {
		m.logger.Errorf("Failed to send success notification: %v", err)
	}

	m.logger.Infof("New table cleanup completed for table %s", tableName)
	return nil
}

func (m *Manager) CleanupTriggers(tableName string) error {
	m.logger.Infof("Starting trigger cleanup for table %s", tableName)

	dbName, err := m.extractDatabaseNameFromDSN()
	if err != nil {
		return fmt.Errorf("failed to extract database name from DSN: %w", err)
	}

	triggers := []string{
		fmt.Sprintf("pt_osc_%s_%s_del", dbName, tableName),
		fmt.Sprintf("pt_osc_%s_%s_upd", dbName, tableName),
		fmt.Sprintf("pt_osc_%s_%s_ins", dbName, tableName),
	}

	taskName := "trigger-cleanup"
	if m.dryRun {
		taskName = "trigger-cleanup (DRY RUN)"
	}

	if err := m.slack.NotifyTriggerCleanupStart(taskName, tableName, triggers); err != nil {
		m.logger.Errorf("Failed to send trigger cleanup start notification: %v", err)
	}

	start := time.Now()
	var hasErrors bool

	for _, trigger := range triggers {
		dropSQL := fmt.Sprintf("DROP TRIGGER IF EXISTS %s", trigger)
		if m.dryRun {
			m.logger.Infof("[DRY RUN] Would execute SQL: %s", dropSQL)
			continue
		}
		if err := m.db.ExecuteAlter(dropSQL); err != nil {
			m.logger.Errorf("Failed to drop trigger %s: %v", trigger, err)
			hasErrors = true
		} else {
			m.logger.Infof("Dropped trigger %s", trigger)
		}
	}

	duration := time.Since(start)

	if hasErrors {
		err := fmt.Errorf("some triggers failed to drop")
		if slackErr := m.slack.NotifyTriggerCleanupFailure(taskName, tableName, triggers, err); slackErr != nil {
			m.logger.Errorf("Failed to send trigger cleanup failure notification: %v", slackErr)
		}
		return err
	}

	if err := m.slack.NotifyTriggerCleanupSuccess(taskName, tableName, triggers, duration); err != nil {
		m.logger.Errorf("Failed to send trigger cleanup success notification: %v", err)
	}

	m.logger.Infof("Trigger cleanup completed for table %s", tableName)
	return nil
}

func (m *Manager) checkOtherActiveConnections(taskName, tableName string) error {
	if !m.config.Common.ConnectionCheck.Enabled {
		return nil
	}

	hasOthers, username, err := m.db.HasOtherActiveConnections()
	if err != nil {
		return fmt.Errorf("failed to check active connections: %w", err)
	}

	if hasOthers {
		errMsg := fmt.Sprintf("detected other active connections for user '%s', stopping execution for safety", username)
		m.logger.Warn(errMsg)

		if slackErr := m.slack.NotifyConnectionCheckFailure(taskName, tableName, username); slackErr != nil {
			m.logger.Errorf("Failed to send connection check failure notification: %v", slackErr)
		}

		return fmt.Errorf("%s", errMsg)
	}

	return nil
}

func (m *Manager) checkNewTableExists(taskName, tableName string) error {
	exists, err := m.db.CheckNewTableExists(tableName)
	if err != nil {
		return fmt.Errorf("failed to check new table existence: %w", err)
	}

	if exists {
		errMsg := fmt.Sprintf("previous pt-osc execution failed, _%s_new table already exists", tableName)
		m.logger.Warn(errMsg)

		if slackErr := m.slack.NotifyPtOscPreCheckFailure(taskName, tableName); slackErr != nil {
			m.logger.Errorf("Failed to send pt-osc pre-check failure notification: %v", slackErr)
		}

		return fmt.Errorf("%s", errMsg)
	}

	return nil
}

func (m *Manager) checkRowCountDifference(tableName string) error {
	originalCount, err := m.db.GetTableRowCountForSwap(tableName)
	if err != nil {
		return fmt.Errorf("failed to get original table row count: %w", err)
	}

	newCount, err := m.db.GetNewTableRowCountForSwap(tableName)
	if err != nil {
		return fmt.Errorf("failed to get new table row count: %w", err)
	}

	m.logger.Infof("Row count comparison for %s: original=%d, new=%d", tableName, originalCount, newCount)

	var diffPercent float64

	if originalCount >= newCount {
		if originalCount > 0 {
			diffPercent = float64(originalCount-newCount) / float64(originalCount) * 100
		}
	} else {
		diffPercent = float64(newCount-originalCount) / float64(newCount) * 100
	}

	threshold := 5.0 // 5%の閾値をハードコーディング
	if diffPercent > threshold {
		errMsg := fmt.Sprintf("row count difference exceeds threshold: %.2f%% (threshold: %.2f%%), original=%d, new=%d",
			diffPercent, threshold, originalCount, newCount)

		m.logger.Errorf("Row count check failed for table %s: %s", tableName, errMsg)

		taskName := "swap-row-count-check"
		if m.dryRun {
			taskName = "swap-row-count-check (DRY RUN)"
		}

		if slackErr := m.slack.NotifyWarning(taskName, tableName, errMsg); slackErr != nil {
			m.logger.Errorf("Failed to send row count check warning notification: %v", slackErr)
		}

		return fmt.Errorf("row count check failed: %s", errMsg)
	}

	m.logger.Infof("Row count check passed for table %s: difference=%.2f%% (threshold: %.2f%%)",
		tableName, diffPercent, threshold)

	return nil
}
