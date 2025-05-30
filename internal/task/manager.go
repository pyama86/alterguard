package task

import (
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

type TaskResult struct {
	Task     config.Task
	Duration time.Duration
	Success  bool
	Error    error
}

type QueryInfo struct {
	Query     string
	QueryType string
	TableName string
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
	m.logger.Infof("Starting execution of %d tasks", len(m.config.Tasks))

	for _, task := range m.config.Tasks {
		if err := m.executeTask(task); err != nil {
			return fmt.Errorf("failed to execute task %s: %w", task.Name, err)
		}
	}

	m.logger.Info("All tasks completed successfully")
	return nil
}

func (m *Manager) executeTask(task config.Task) error {
	m.logger.Infof("Starting task: %s", task.Name)
	start := time.Now()

	threshold := m.getThreshold(task)
	queries, err := m.parseQueries(task.Queries)
	if err != nil {
		return err
	}

	smallQueries := []QueryInfo{}
	largeQueries := []QueryInfo{}

	for _, queryInfo := range queries {
		if queryInfo.QueryType != "ALTER" {
			smallQueries = append(smallQueries, queryInfo)
			continue
		}

		if queryInfo.TableName == "" {
			smallQueries = append(smallQueries, queryInfo)
			continue
		}

		rowCount, err := m.db.GetTableRowCount(queryInfo.TableName)
		if err != nil {
			m.logger.Warnf("Failed to get row count for table %s, treating as small query: %v", queryInfo.TableName, err)
			smallQueries = append(smallQueries, queryInfo)
			continue
		}

		m.logger.Infof("Table %s has %d rows (threshold: %d)", queryInfo.TableName, rowCount, threshold)

		if rowCount <= threshold {
			smallQueries = append(smallQueries, queryInfo)
		} else {
			largeQueries = append(largeQueries, queryInfo)
		}
	}

	if err := m.executeSmallQueries(task, smallQueries); err != nil {
		return err
	}

	if len(largeQueries) > 0 {
		if len(largeQueries) > 1 {
			m.logger.Warnf("Multiple large queries detected in task %s, only first will be executed with pt-osc", task.Name)
			for _, queryInfo := range largeQueries[1:] {
				warning := fmt.Sprintf("Skipping large query in task %s: %s", task.Name, queryInfo.Query)
				m.logger.Warn(warning)
				if slackErr := m.slack.NotifyWarning(task.Name, queryInfo.TableName, warning); slackErr != nil {
					m.logger.Errorf("Failed to send Slack notification: %v", slackErr)
				}
			}
		}

		if err := m.executeLargeQuery(task, largeQueries[0]); err != nil {
			return err
		}
	}

	duration := time.Since(start)
	m.logger.Infof("Task %s completed in %s", task.Name, duration)
	return nil
}

func (m *Manager) executeSmallQueries(task config.Task, queries []QueryInfo) error {
	for _, queryInfo := range queries {
		m.logger.Infof("Executing query: %s", queryInfo.Query)

		if err := m.executeQuery(&queryInfo, task.Name); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) executeLargeQuery(task config.Task, queryInfo QueryInfo) error {
	if queryInfo.QueryType != "ALTER" {
		return fmt.Errorf("pt-osc can only be used with ALTER statements, got: %s", queryInfo.QueryType)
	}

	rowCount, err := m.db.GetTableRowCount(queryInfo.TableName)
	if err != nil {
		return fmt.Errorf("failed to get row count: %w", err)
	}

	m.logger.Infof("Executing pt-online-schema-change for table %s (rows: %d)", queryInfo.TableName, rowCount)

	if err := m.slack.NotifyStart(task.Name, queryInfo.TableName, rowCount); err != nil {
		m.logger.Errorf("Failed to send start notification: %v", err)
	}

	start := time.Now()
	alterStatement := m.extractAlterStatement(queryInfo.Query)
	legacyTask := config.Task{
		Name:    task.Name,
		Queries: []string{fmt.Sprintf("ALTER TABLE %s %s", queryInfo.TableName, alterStatement)},
	}

	if err := m.ptosc.Execute(legacyTask, m.config.Common.PtOsc, m.config.DSN, m.dryRun); err != nil {
		if slackErr := m.slack.NotifyFailure(task.Name, queryInfo.TableName, rowCount, err); slackErr != nil {
			m.logger.Errorf("Failed to send failure notification: %v", slackErr)
		}
		return fmt.Errorf("pt-online-schema-change failed: %w", err)
	}

	duration := time.Since(start)
	if err := m.slack.NotifySuccess(task.Name, queryInfo.TableName, rowCount, duration); err != nil {
		m.logger.Errorf("Failed to send success notification: %v", err)
	}

	return nil
}

func (m *Manager) executeQuery(queryInfo *QueryInfo, taskName string) error {
	if m.dryRun {
		m.logger.Infof("[DRY RUN] Would execute query: %s", queryInfo.Query)
		return nil
	}

	if err := m.db.ExecuteAlter(queryInfo.Query); err != nil {
		if database.IsDuplicateError(err) {
			warning := fmt.Sprintf("Duplicate detected in task %s: %s (query: %s)", taskName, err.Error(), queryInfo.Query)
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

func (m *Manager) getThreshold(task config.Task) int64 {
	if task.Threshold != nil {
		return *task.Threshold
	}
	return m.config.Common.PtOscThreshold
}

func (m *Manager) SwapTable(tableName string) error {
	m.logger.Infof("Starting table swap for %s", tableName)

	lockDetected, err := m.db.CheckMetadataLock(tableName, m.config.Common.Alert.MetadataLockThresholdSeconds)
	if err != nil {
		return fmt.Errorf("failed to check metadata lock: %w", err)
	}

	if lockDetected {
		warning := fmt.Sprintf("Metadata lock detected for table %s (threshold: %d seconds)",
			tableName, m.config.Common.Alert.MetadataLockThresholdSeconds)
		m.logger.Warn(warning)

		if err := m.slack.NotifyWarning("swap", tableName, warning); err != nil {
			m.logger.Errorf("Failed to send warning notification: %v", err)
		}
	}

	swapSQL := fmt.Sprintf("RENAME TABLE %s TO %s_old, _%s_new TO %s",
		tableName, tableName, tableName, tableName)

	if m.dryRun {
		m.logger.Infof("[DRY RUN] Would execute swap: %s", swapSQL)
		return nil
	}

	if err := m.db.ExecuteAlter(swapSQL); err != nil {
		return fmt.Errorf("table swap failed: %w", err)
	}

	m.logger.Infof("Table swap completed for %s", tableName)
	return nil
}

func (m *Manager) CleanupTable(tableName string) error {
	m.logger.Infof("Starting cleanup for table %s", tableName)

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s_old", tableName)

	if m.dryRun {
		m.logger.Infof("[DRY RUN] Would execute cleanup: %s", dropSQL)
		return nil
	}

	if err := m.db.ExecuteAlter(dropSQL); err != nil {
		return fmt.Errorf("failed to drop backup table: %w", err)
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
			m.logger.Infof("[DRY RUN] Would execute trigger cleanup: %s", dropSQL)
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
