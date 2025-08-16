package slack

import (
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestNewSlackNotifier(t *testing.T) {
	tests := []struct {
		name        string
		webhookURL  string
		expectError bool
	}{
		{
			name:        "valid webhook URL",
			webhookURL:  "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
			expectError: false,
		},
		{
			name:        "missing webhook URL",
			webhookURL:  "",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.webhookURL != "" {
				t.Setenv("SLACK_WEBHOOK_URL", tt.webhookURL)
			} else {
				t.Setenv("SLACK_WEBHOOK_URL", "")
			}

			logger := logrus.New()
			notifier, err := NewSlackNotifier(logger)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, notifier)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, notifier)
			}
		})
	}
}

func TestNotificationMessages(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel) // Suppress log output during tests

	t.Setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/test")

	notifier, err := NewSlackNotifier(logger)
	assert.NoError(t, err)
	assert.NotNil(t, notifier)

	tests := []struct {
		name     string
		testFunc func() error
	}{
		{
			name: "notify start",
			testFunc: func() error {
				return notifier.NotifyStart("test_task", "test_table", 1000)
			},
		},
		{
			name: "notify success",
			testFunc: func() error {
				return notifier.NotifySuccess("test_task", "test_table", 1000, 5*time.Minute)
			},
		},
		{
			name: "notify failure",
			testFunc: func() error {
				return notifier.NotifyFailure("test_task", "test_table", 1000, errors.New("test error"))
			},
		},
		{
			name: "notify warning",
			testFunc: func() error {
				return notifier.NotifyWarning("test_task", "test_table", "test warning message")
			},
		},
		{
			name: "notify pt-osc completion with new table count",
			testFunc: func() error {
				return notifier.NotifyPtOscCompletionWithNewTableCount("pt-osc", "test_table", 1000, 1000, 5*time.Minute, "pt-osc output log")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.testFunc()
			// Since we're using a fake webhook URL, we might get an error
			// but we're mainly testing that the message formatting doesn't panic
			// The actual error depends on network conditions and Slack's response
			_ = err // We don't assert error here as it's network dependent
		})
	}
}
