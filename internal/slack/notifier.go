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
}

type SlackNotifier struct {
	client *slack.Client
	logger *logrus.Logger
}

func NewSlackNotifier(logger *logrus.Logger) (*SlackNotifier, error) {
	webhookURL := os.Getenv("SLACK_WEBHOOK_URL")
	var client *slack.Client
	if webhookURL == "" {
		logger.Info("SLACK_WEBHOOK_URL environment variable is not set, Slack notifications will be disabled")
	} else {
		client = slack.New("", slack.OptionAPIURL(webhookURL))
	}

	return &SlackNotifier{
		client: client,
		logger: logger,
	}, nil
}

func (n *SlackNotifier) NotifyStart(taskName, tableName string, rowCount int64) error {
	message := fmt.Sprintf("🚀 Schema change started\nTask: %s\nTable: %s\nRow count: %d",
		taskName, tableName, rowCount)

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifySuccess(taskName, tableName string, rowCount int64, duration time.Duration) error {
	message := fmt.Sprintf("✅ Schema change completed successfully\nTask: %s\nTable: %s\nRow count: %d\nDuration: %s",
		taskName, tableName, rowCount, duration.String())

	return n.sendMessage(message, "good")
}

func (n *SlackNotifier) NotifyFailure(taskName, tableName string, rowCount int64, err error) error {
	message := fmt.Sprintf("❌ Schema change failed\nTask: %s\nTable: %s\nRow count: %d\nError: %s",
		taskName, tableName, rowCount, err.Error())

	return n.sendMessage(message, "danger")
}

func (n *SlackNotifier) NotifyWarning(taskName, tableName string, message string) error {
	msg := fmt.Sprintf("⚠️ Schema change warning\nTask: %s\nTable: %s\nWarning: %s",
		taskName, tableName, message)

	return n.sendMessage(msg, "warning")
}

func (n *SlackNotifier) sendMessage(text, color string) error {
	if n.client == nil {
		return nil
	}

	attachment := slack.Attachment{
		Color: color,
		Text:  text,
	}

	msg := &slack.WebhookMessage{
		Username:    "alterguard",
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
