package slack

import (
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/slack-go/slack"
)

type Notifier interface {
	NotifyStart(taskName, tableName string, rowCount int64) error
	NotifySuccess(taskName, tableName string, rowCount int64, duration time.Duration) error
	NotifyFailure(taskName, tableName string, rowCount int64, err error) error
	NotifyWarning(taskName, tableName string, message string) error
	NotifyStartWithQuery(taskName, tableName, query string, rowCount int64) error
	NotifySuccessWithQuery(taskName, tableName, query string, rowCount int64, duration time.Duration) error
	NotifyFailureWithQuery(taskName, tableName, query string, rowCount int64, err error) error
	NotifySuccessWithQueryAndLog(taskName, tableName, query string, rowCount int64, duration time.Duration, ptOscLog string) error
	NotifyFailureWithQueryAndLog(taskName, tableName, query string, rowCount int64, err error, ptOscLog string) error
	NotifyPtOscCompletionWithNewTableCount(taskName, tableName string, originalRowCount, newRowCount int64, duration time.Duration, ptOscLog string) error
	NotifyDryRunResult(taskName, tableName string, result *DryRunResult, duration time.Duration) error
	NotifyConnectionCheckFailure(taskName, tableName, username string) error
	NotifyTriggerCleanupStart(taskName, tableName string, triggers []string) error
	NotifyTriggerCleanupSuccess(taskName, tableName string, triggers []string, duration time.Duration) error
	NotifyTriggerCleanupFailure(taskName, tableName string, triggers []string, err error) error
	NotifyPtOscPreCheckFailure(taskName, tableName string) error
	NotifyAllTasksStart(totalQueries int) error
	NotifyAllTasksSuccess(totalQueries int, duration time.Duration) error
	NotifyAllTasksFailure(totalQueries int, err error) error
}

type DryRunResult struct {
	EstimatedTime    string
	AffectedRows     int64
	ChunkCount       int
	ValidationResult string
	Warnings         []string
	Summary          string
}

type SlackNotifier struct {
	client      *slack.Client
	logger      *logrus.Logger
	environment string
}

func NewSlackNotifier(logger *logrus.Logger) (*SlackNotifier, error) {
	return NewSlackNotifierWithEnvironment(logger, "")
}

func NewSlackNotifierWithEnvironment(logger *logrus.Logger, environment string) (*SlackNotifier, error) {
	webhookURL := os.Getenv("SLACK_WEBHOOK_URL")
	var client *slack.Client
	if webhookURL == "" {
		logger.Info("SLACK_WEBHOOK_URL environment variable is not set, Slack notifications will be disabled")
	} else {
		client = slack.New("", slack.OptionAPIURL(webhookURL))
	}

	return &SlackNotifier{
		client:      client,
		logger:      logger,
		environment: environment,
	}, nil
}

func (n *SlackNotifier) formatTitle(title string) string {
	if n.environment != "" {
		return fmt.Sprintf("%s [%s]", title, n.environment)
	}
	return title
}

func (n *SlackNotifier) FormatTitle(title string) string {
	return n.formatTitle(title)
}

func (n *SlackNotifier) NotifyStart(taskName, tableName string, rowCount int64) error {
	title := n.formatTitle("🚀 Schema change started")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nRow count: %d",
		title, taskName, tableName, rowCount)

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifySuccess(taskName, tableName string, rowCount int64, duration time.Duration) error {
	title := n.formatTitle("✅ Schema change completed successfully")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nRow count: %d\nDuration: %s",
		title, taskName, tableName, rowCount, duration.String())

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifyFailure(taskName, tableName string, rowCount int64, err error) error {
	title := n.formatTitle("❌ Schema change failed")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nRow count: %d\nError: %s",
		title, taskName, tableName, rowCount, err.Error())

	return n.sendMessage(message, "danger")
}

func (n *SlackNotifier) NotifyWarning(taskName, tableName string, message string) error {
	title := n.formatTitle("⚠️ Schema change warning")
	msg := fmt.Sprintf("%s\nTask: %s\nTable: %s\nWarning: %s",
		title, taskName, tableName, message)

	return n.sendMessage(msg, "warning")
}

func (n *SlackNotifier) NotifyStartWithQuery(taskName, tableName, query string, rowCount int64) error {
	title := n.formatTitle("🚀 Schema change started")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nRow count: %d\nQuery: %s",
		title, taskName, tableName, rowCount, query)

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifySuccessWithQuery(taskName, tableName, query string, rowCount int64, duration time.Duration) error {
	title := n.formatTitle("✅ Schema change completed successfully")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nRow count: %d\nDuration: %s\nQuery: %s",
		title, taskName, tableName, rowCount, duration.String(), query)

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifyFailureWithQuery(taskName, tableName, query string, rowCount int64, err error) error {
	title := n.formatTitle("❌ Schema change failed")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nRow count: %d\nError: %s\nQuery: %s",
		title, taskName, tableName, rowCount, err.Error(), query)

	return n.sendMessage(message, "danger")
}

func (n *SlackNotifier) NotifySuccessWithQueryAndLog(taskName, tableName, query string, rowCount int64, duration time.Duration, ptOscLog string) error {
	title := n.formatTitle("✅ Schema change completed successfully")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nRow count: %d\nDuration: %s\nQuery: %s",
		title, taskName, tableName, rowCount, duration.String(), query)

	if ptOscLog != "" {
		message += "\n\n📋 pt-osc Output:\n```\n" + ptOscLog + "\n```"
	}

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifyFailureWithQueryAndLog(taskName, tableName, query string, rowCount int64, err error, ptOscLog string) error {
	title := n.formatTitle("❌ Schema change failed")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nRow count: %d\nError: %s\nQuery: %s",
		title, taskName, tableName, rowCount, err.Error(), query)

	if ptOscLog != "" {
		message += "\n\n📋 pt-osc Output:\n```\n" + ptOscLog + "\n```"
	}

	return n.sendMessage(message, "danger")
}

func (n *SlackNotifier) NotifyPtOscCompletionWithNewTableCount(taskName, tableName string, originalRowCount, newRowCount int64, duration time.Duration, ptOscLog string) error {
	title := n.formatTitle("✅ pt-osc completed successfully")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nOriginal row count: %d\nNew table row count: %d\nDuration: %s",
		title, taskName, tableName, originalRowCount, newRowCount, duration.String())

	if ptOscLog != "" {
		message += "\n\n📋 pt-osc Output:\n```\n" + ptOscLog + "\n```"
	}

	return n.sendMessage(message, "warning")
}

func (n *SlackNotifier) NotifyDryRunResult(taskName, tableName string, result *DryRunResult, duration time.Duration) error {
	title := n.formatTitle("🧪 Dry run completed")
	var message string

	if result.ValidationResult != "" {
		message = fmt.Sprintf("%s\nTask: %s\nTable: %s\nDuration: %s\nStatus: %s",
			title, taskName, tableName, duration.String(), result.ValidationResult)
	} else {
		message = fmt.Sprintf("%s\nTask: %s\nTable: %s\nDuration: %s",
			title, taskName, tableName, duration.String())
	}

	if result.Summary != "" {
		message += "\n\n📋 pt-osc Output:\n```\n" + result.Summary + "\n```"
	}

	color := "good"
	if len(result.Warnings) > 0 {
		color = "warning"
	}

	return n.sendMessage(message, color)
}

func (n *SlackNotifier) NotifyConnectionCheckFailure(taskName, tableName, username string) error {
	title := n.formatTitle("🛑 Schema change stopped - Other connections detected")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nUser: %s\nReason: Detected other active connections for the same user",
		title, taskName, tableName, username)

	return n.sendMessage(message, "warning")
}

func (n *SlackNotifier) NotifyTriggerCleanupStart(taskName, tableName string, triggers []string) error {
	title := n.formatTitle("🗑️ Trigger cleanup started")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nTriggers: %v",
		title, taskName, tableName, triggers)

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifyTriggerCleanupSuccess(taskName, tableName string, triggers []string, duration time.Duration) error {
	title := n.formatTitle("✅ Trigger cleanup completed successfully")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nTriggers: %v\nDuration: %s",
		title, taskName, tableName, triggers, duration.String())

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifyTriggerCleanupFailure(taskName, tableName string, triggers []string, err error) error {
	title := n.formatTitle("❌ Trigger cleanup failed")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nTriggers: %v\nError: %s",
		title, taskName, tableName, triggers, err.Error())

	return n.sendMessage(message, "danger")
}

func (n *SlackNotifier) NotifyPtOscPreCheckFailure(taskName, tableName string) error {
	title := n.formatTitle("⚠️ pt-osc pre-check failed")
	message := fmt.Sprintf("%s\nTask: %s\nTable: %s\nReason: Previous pt-osc execution failed, _%s_new table already exists\n\nTo resolve this issue, run the cleanup command:\n```\nalterguard cleanup %s --drop-new-table --drop-triggers\n```\n\nAfter cleanup, you can retry the pt-osc execution.",
		title, taskName, tableName, tableName, tableName)

	return n.sendMessage(message, "warning")
}

func (n *SlackNotifier) NotifyAllTasksStart(totalQueries int) error {
	title := n.formatTitle("🚀 All tasks started")
	message := fmt.Sprintf("%s\nTotal queries: %d", title, totalQueries)

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifyAllTasksSuccess(totalQueries int, duration time.Duration) error {
	title := n.formatTitle("✅ All tasks completed successfully")
	message := fmt.Sprintf("%s\nTotal queries: %d\nTotal duration: %s", title, totalQueries, duration.String())

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifyAllTasksFailure(totalQueries int, err error) error {
	title := n.formatTitle("❌ Tasks failed")
	message := fmt.Sprintf("%s\nTotal queries: %d\nError: %s", title, totalQueries, err.Error())

	return n.sendMessage(message, "danger")
}

func (n *SlackNotifier) sendMessage(text, color string) error {
	if n.client == nil {
		return nil
	}

	attachment := slack.Attachment{
		Color: color,
		Text:  text,
	}

	username := "alterguard"
	if n.environment != "" {
		username = fmt.Sprintf("[%s] %s", n.environment, username)
	}

	msg := &slack.WebhookMessage{
		Username:    username,
		IconEmoji:   ":gear:",
		Attachments: []slack.Attachment{attachment},
	}

	err := slack.PostWebhook(os.Getenv("SLACK_WEBHOOK_URL"), msg)
	if err != nil {
		n.logger.Errorf("Failed to send Slack notification: %v", err)
		return fmt.Errorf("failed to send Slack notification: %w", err)
	}

	n.logger.Debugf("Slack notification sent successfully: %s", text)
	return nil
}
