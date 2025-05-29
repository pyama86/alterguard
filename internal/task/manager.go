package task

import (
	"fmt"
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
	RowCount int64
	Duration time.Duration
	Method   string // "ALTER" or "PT-OSC"
	Success  bool
	Error    error
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

	smallTasks := []config.Task{}
	largeTasks := []config.Task{}

	// Classify tasks by row count
	for _, task := range m.config.Tasks {
		rowCount, err := m.db.GetTableRowCount(task.Table)
		if err != nil {
			return fmt.Errorf("failed to get row count for table %s: %w", task.Table, err)
		}

		m.logger.Infof("Table %s has %d rows (threshold: %d)", task.Table, rowCount, task.Threshold)

		if rowCount <= task.Threshold {
			smallTasks = append(smallTasks, task)
		} else {
			largeTasks = append(largeTasks, task)
		}
	}

	// Execute small tasks with ALTER TABLE
	for _, task := range smallTasks {
		if err := m.executeSmallTask(task); err != nil {
			return fmt.Errorf("failed to execute small task %s: %w", task.Name, err)
		}
	}

	// Check large tasks count
	if len(largeTasks) == 0 {
		m.logger.Info("All tasks completed successfully")
		return nil
	}

	if len(largeTasks) > 1 {
		taskNames := make([]string, len(largeTasks))
		for i, task := range largeTasks {
			taskNames[i] = task.Name
		}
		err := fmt.Errorf("multiple large tables detected: %v", taskNames)

		// Send Slack notification for multiple large tables
		for _, task := range largeTasks {
			rowCount, _ := m.db.GetTableRowCount(task.Table)
			if slackErr := m.slack.NotifyFailure(task.Name, task.Table, rowCount, err); slackErr != nil {
				m.logger.Errorf("Failed to send Slack notification: %v", slackErr)
			}
		}

		return err
	}

	// Execute single large task with pt-osc
	task := largeTasks[0]
	if err := m.executeLargeTask(task); err != nil {
		return fmt.Errorf("failed to execute large task %s: %w", task.Name, err)
	}

	m.logger.Info("All tasks completed successfully")
	return nil
}

func (m *Manager) executeSmallTask(task config.Task) error {
	start := time.Now()

	rowCount, err := m.db.GetTableRowCount(task.Table)
	if err != nil {
		return fmt.Errorf("failed to get row count: %w", err)
	}

	m.logger.Infof("Executing ALTER TABLE for task %s (table: %s, rows: %d)", task.Name, task.Table, rowCount)

	// Send start notification
	if err := m.slack.NotifyStart(task.Name, task.Table, rowCount); err != nil {
		m.logger.Errorf("Failed to send start notification: %v", err)
	}

	// Execute ALTER TABLE
	alterSQL := fmt.Sprintf("ALTER TABLE %s %s", task.Table, task.AlterStatement)
	if err := m.db.ExecuteAlter(alterSQL); err != nil {
		// Send failure notification
		if slackErr := m.slack.NotifyFailure(task.Name, task.Table, rowCount, err); slackErr != nil {
			m.logger.Errorf("Failed to send failure notification: %v", slackErr)
		}
		return fmt.Errorf("ALTER TABLE failed: %w", err)
	}

	duration := time.Since(start)
	m.logger.Infof("ALTER TABLE completed for task %s in %s", task.Name, duration)

	// Send success notification
	if err := m.slack.NotifySuccess(task.Name, task.Table, rowCount, duration); err != nil {
		m.logger.Errorf("Failed to send success notification: %v", err)
	}

	return nil
}

func (m *Manager) executeLargeTask(task config.Task) error {
	start := time.Now()

	rowCount, err := m.db.GetTableRowCount(task.Table)
	if err != nil {
		return fmt.Errorf("failed to get row count: %w", err)
	}

	m.logger.Infof("Executing pt-online-schema-change for task %s (table: %s, rows: %d)", task.Name, task.Table, rowCount)

	// Send start notification
	if err := m.slack.NotifyStart(task.Name, task.Table, rowCount); err != nil {
		m.logger.Errorf("Failed to send start notification: %v", err)
	}

	// Execute pt-osc
	if err := m.ptosc.Execute(task, m.config.Common.PtOsc, m.config.DSN, m.dryRun); err != nil {
		// Send failure notification
		if slackErr := m.slack.NotifyFailure(task.Name, task.Table, rowCount, err); slackErr != nil {
			m.logger.Errorf("Failed to send failure notification: %v", slackErr)
		}
		return fmt.Errorf("pt-online-schema-change failed: %w", err)
	}

	duration := time.Since(start)
	m.logger.Infof("pt-online-schema-change completed for task %s in %s", task.Name, duration)

	// Send success notification
	if err := m.slack.NotifySuccess(task.Name, task.Table, rowCount, duration); err != nil {
		m.logger.Errorf("Failed to send success notification: %v", err)
	}

	return nil
}

func (m *Manager) SwapTable(tableName string) error {
	m.logger.Infof("Starting table swap for %s", tableName)

	// Check for metadata locks
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

	// Execute table swap
	swapSQL := fmt.Sprintf("RENAME TABLE %s TO %s_old, _%s_new TO %s",
		tableName, tableName, tableName, tableName)

	if err := m.db.ExecuteAlter(swapSQL); err != nil {
		return fmt.Errorf("table swap failed: %w", err)
	}

	m.logger.Infof("Table swap completed for %s", tableName)
	return nil
}

func (m *Manager) CleanupTable(tableName string) error {
	m.logger.Infof("Starting cleanup for table %s", tableName)

	// Drop backup table
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s_old", tableName)
	if err := m.db.ExecuteAlter(dropSQL); err != nil {
		return fmt.Errorf("failed to drop backup table: %w", err)
	}

	m.logger.Infof("Cleanup completed for table %s", tableName)
	return nil
}

func (m *Manager) CleanupTriggers(tableName string) error {
	m.logger.Infof("Starting trigger cleanup for table %s", tableName)

	// Get trigger names
	triggers := []string{
		fmt.Sprintf("pt_osc_%s_del", tableName),
		fmt.Sprintf("pt_osc_%s_upd", tableName),
		fmt.Sprintf("pt_osc_%s_ins", tableName),
	}

	for _, trigger := range triggers {
		dropSQL := fmt.Sprintf("DROP TRIGGER IF EXISTS %s", trigger)
		if err := m.db.ExecuteAlter(dropSQL); err != nil {
			m.logger.Errorf("Failed to drop trigger %s: %v", trigger, err)
			// Continue with other triggers even if one fails
		} else {
			m.logger.Infof("Dropped trigger %s", trigger)
		}
	}

	m.logger.Infof("Trigger cleanup completed for table %s", tableName)
	return nil
}
