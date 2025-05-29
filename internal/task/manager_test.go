package task

import (
	"errors"
	"testing"
	"time"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockDBClient struct {
	mock.Mock
}

func (m *MockDBClient) GetTableRowCount(table string) (int64, error) {
	args := m.Called(table)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDBClient) ExecuteAlter(alterStatement string) error {
	args := m.Called(alterStatement)
	return args.Error(0)
}

func (m *MockDBClient) CheckMetadataLock(table string, thresholdSeconds int) (bool, error) {
	args := m.Called(table, thresholdSeconds)
	return args.Bool(0), args.Error(1)
}

func (m *MockDBClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockPtOscExecutor struct {
	mock.Mock
}

func (m *MockPtOscExecutor) Execute(task config.Task, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool) error {
	args := m.Called(task, ptOscConfig, dsn, forceDryRun)
	return args.Error(0)
}

type MockSlackNotifier struct {
	mock.Mock
}

func (m *MockSlackNotifier) NotifyStart(taskName, tableName string, rowCount int64) error {
	args := m.Called(taskName, tableName, rowCount)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifySuccess(taskName, tableName string, rowCount int64, duration time.Duration) error {
	args := m.Called(taskName, tableName, rowCount, duration)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyFailure(taskName, tableName string, rowCount int64, err error) error {
	args := m.Called(taskName, tableName, rowCount, err)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyWarning(taskName, tableName string, message string) error {
	args := m.Called(taskName, tableName, message)
	return args.Error(0)
}

func TestExecuteAllTasks(t *testing.T) {
	tests := []struct {
		name           string
		tasks          []config.Task
		rowCounts      map[string]int64
		expectError    bool
		expectedMethod string
	}{
		{
			name: "all small tasks",
			tasks: []config.Task{
				{Name: "task1", Table: "table1", AlterStatement: "ADD COLUMN foo INT", Threshold: 1000},
				{Name: "task2", Table: "table2", AlterStatement: "ADD COLUMN bar INT", Threshold: 1000},
			},
			rowCounts: map[string]int64{
				"table1": 500,
				"table2": 800,
			},
			expectError:    false,
			expectedMethod: "ALTER",
		},
		{
			name: "one large task",
			tasks: []config.Task{
				{Name: "task1", Table: "table1", AlterStatement: "ADD COLUMN foo INT", Threshold: 1000},
				{Name: "task2", Table: "table2", AlterStatement: "ADD COLUMN bar INT", Threshold: 1000},
			},
			rowCounts: map[string]int64{
				"table1": 500,
				"table2": 2000,
			},
			expectError:    false,
			expectedMethod: "PT-OSC",
		},
		{
			name: "multiple large tasks - should fail",
			tasks: []config.Task{
				{Name: "task1", Table: "table1", AlterStatement: "ADD COLUMN foo INT", Threshold: 1000},
				{Name: "task2", Table: "table2", AlterStatement: "ADD COLUMN bar INT", Threshold: 1000},
			},
			rowCounts: map[string]int64{
				"table1": 2000,
				"table2": 3000,
			},
			expectError: true,
		},
		{
			name: "row count error",
			tasks: []config.Task{
				{Name: "task1", Table: "table1", AlterStatement: "ADD COLUMN foo INT", Threshold: 1000},
			},
			rowCounts:   map[string]int64{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logrus.New()
			logger.SetLevel(logrus.FatalLevel)

			mockDB := &MockDBClient{}
			mockPtOsc := &MockPtOscExecutor{}
			mockSlack := &MockSlackNotifier{}

			cfg := &config.Config{
				Tasks: tt.tasks,
				Common: config.CommonConfig{
					PtOsc: config.PtOscConfig{},
				},
				DSN: "test-dsn",
			}

			manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, false)

			// Setup mocks
			for _, task := range tt.tasks {
				if rowCount, exists := tt.rowCounts[task.Table]; exists {
					mockDB.On("GetTableRowCount", task.Table).Return(rowCount, nil)
				} else {
					mockDB.On("GetTableRowCount", task.Table).Return(int64(0), errors.New("table not found"))
				}
			}

			if !tt.expectError {
				// Setup success mocks
				for _, task := range tt.tasks {
					rowCount := tt.rowCounts[task.Table]
					mockSlack.On("NotifyStart", task.Name, task.Table, rowCount).Return(nil)
					mockSlack.On("NotifySuccess", task.Name, task.Table, rowCount, mock.AnythingOfType("time.Duration")).Return(nil)

					if rowCount <= task.Threshold {
						// Small task - ALTER TABLE
						alterSQL := "ALTER TABLE " + task.Table + " " + task.AlterStatement
						mockDB.On("ExecuteAlter", alterSQL).Return(nil)
					} else {
						// Large task - pt-osc
						mockPtOsc.On("Execute", task, cfg.Common.PtOsc, cfg.DSN, false).Return(nil)
					}
				}
			} else {
				// Setup failure mocks for multiple large tables
				largeTaskCount := 0
				for _, task := range tt.tasks {
					if rowCount, exists := tt.rowCounts[task.Table]; exists && rowCount > task.Threshold {
						largeTaskCount++
					}
				}

				if largeTaskCount > 1 {
					for _, task := range tt.tasks {
						if rowCount, exists := tt.rowCounts[task.Table]; exists && rowCount > task.Threshold {
							mockSlack.On("NotifyFailure", task.Name, task.Table, rowCount, mock.AnythingOfType("*errors.errorString")).Return(nil)
						}
					}
				}
			}

			err := manager.ExecuteAllTasks()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockDB.AssertExpectations(t)
			mockPtOsc.AssertExpectations(t)
			mockSlack.AssertExpectations(t)
		})
	}
}

func TestSwapTable(t *testing.T) {
	tests := []struct {
		name           string
		tableName      string
		lockDetected   bool
		lockCheckError error
		swapError      error
		expectError    bool
		expectWarning  bool
	}{
		{
			name:          "successful swap without lock",
			tableName:     "test_table",
			lockDetected:  false,
			expectError:   false,
			expectWarning: false,
		},
		{
			name:          "successful swap with lock warning",
			tableName:     "test_table",
			lockDetected:  true,
			expectError:   false,
			expectWarning: true,
		},
		{
			name:           "lock check error",
			tableName:      "test_table",
			lockCheckError: errors.New("lock check failed"),
			expectError:    true,
		},
		{
			name:         "swap error",
			tableName:    "test_table",
			lockDetected: false,
			swapError:    errors.New("swap failed"),
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logrus.New()
			logger.SetLevel(logrus.FatalLevel)

			mockDB := &MockDBClient{}
			mockPtOsc := &MockPtOscExecutor{}
			mockSlack := &MockSlackNotifier{}

			cfg := &config.Config{
				Common: config.CommonConfig{
					Alert: config.AlertConfig{
						MetadataLockThresholdSeconds: 30,
					},
				},
			}

			manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, false)

			// Setup mocks
			if tt.lockCheckError != nil {
				mockDB.On("CheckMetadataLock", tt.tableName, 30).Return(false, tt.lockCheckError)
			} else {
				mockDB.On("CheckMetadataLock", tt.tableName, 30).Return(tt.lockDetected, nil)

				if tt.expectWarning {
					mockSlack.On("NotifyWarning", "swap", tt.tableName, mock.AnythingOfType("string")).Return(nil)
				}

				if tt.swapError != nil {
					mockDB.On("ExecuteAlter", mock.AnythingOfType("string")).Return(tt.swapError)
				} else {
					mockDB.On("ExecuteAlter", mock.AnythingOfType("string")).Return(nil)
				}
			}

			err := manager.SwapTable(tt.tableName)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockDB.AssertExpectations(t)
			mockSlack.AssertExpectations(t)
		})
	}
}

func TestCleanupTable(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	mockDB := &MockDBClient{}
	mockPtOsc := &MockPtOscExecutor{}
	mockSlack := &MockSlackNotifier{}

	cfg := &config.Config{}
	manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, false)

	tableName := "test_table"
	expectedSQL := "DROP TABLE IF EXISTS test_table_old"

	mockDB.On("ExecuteAlter", expectedSQL).Return(nil)

	err := manager.CleanupTable(tableName)

	require.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestCleanupTriggers(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	mockDB := &MockDBClient{}
	mockPtOsc := &MockPtOscExecutor{}
	mockSlack := &MockSlackNotifier{}

	cfg := &config.Config{}
	manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, false)

	tableName := "test_table"
	expectedTriggers := []string{
		"DROP TRIGGER IF EXISTS pt_osc_test_table_del",
		"DROP TRIGGER IF EXISTS pt_osc_test_table_upd",
		"DROP TRIGGER IF EXISTS pt_osc_test_table_ins",
	}

	for _, trigger := range expectedTriggers {
		mockDB.On("ExecuteAlter", trigger).Return(nil)
	}

	err := manager.CleanupTriggers(tableName)

	require.NoError(t, err)
	mockDB.AssertExpectations(t)
}
