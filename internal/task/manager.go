package task

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/pyama86/alterguard/internal/database"
	"github.com/pyama86/alterguard/internal/ptosc"
	"github.com/pyama86/alterguard/internal/slack"
	"github.com/sirupsen/logrus"
)

type Manager struct {
	db     database.Client
	ptosc  ptosc.Executor
	slack  slack.Notifier
	logger *logrus.Logger
	config *config.Config
	dryRun bool
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

func NewManager(db database.Client, ptoscExec ptosc.Executor, slackNotifier slack.Notifier, logger *logrus.Logger, cfg *config.Config, dryRun bool) *Manager {
	return &Manager{
		db:     db,
		ptosc:  ptoscExec,
		slack:  slackNotifier,
		logger: logger,
		config: cfg,
		dryRun: dryRun,
	}
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

func (m *Manager) CleanupTable(tableName string) error {
	m.logger.Infof("Starting cleanup for table %s", tableName)

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

func (m *Manager) CleanupTriggers(tableName string) error {
	m.logger.Infof("Starting trigger cleanup for table %s", tableName)

	triggers := []string{
		fmt.Sprintf("pt_osc_%s_del", tableName),
		fmt.Sprintf("pt_osc_%s_upd", tableName),
		fmt.Sprintf("pt_osc_%s_ins", tableName),
	}

	for _, trigger := range triggers {
		dropSQL := fmt.Sprintf("DROP TRIGGER IF EXISTS %s", trigger)
		if m.dryRun {
			m.logger.Infof("[DRY RUN] Would execute SQL: %s", dropSQL)
			continue
		}
		if err := m.db.ExecuteAlter(dropSQL); err != nil {
			m.logger.Errorf("Failed to drop trigger %s: %v", trigger, err)
		} else {
			m.logger.Infof("Dropped trigger %s", trigger)
		}
	}

	m.logger.Infof("Trigger cleanup completed for table %s", tableName)
	return nil
}

func (m *Manager) checkExecutionTimeThreshold(taskName, tableName, query string, duration time.Duration) {
	thresholdSeconds := m.config.Common.Alert.ExecutionTimeThresholdSeconds
	if thresholdSeconds <= 0 {
		return
	}

	threshold := time.Duration(thresholdSeconds) * time.Second
	if duration > threshold {
		warning := fmt.Sprintf("Long execution time detected in %s: %v (threshold: %v) for query: %s",
			taskName, duration, threshold, query)
		m.logger.Warn(warning)

		if slackErr := m.slack.NotifyWarning(taskName, tableName, warning); slackErr != nil {
			m.logger.Errorf("Failed to send execution time warning notification: %v", slackErr)
		}
	}
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
