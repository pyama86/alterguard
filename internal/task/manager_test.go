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

func (m *MockDBClient) ExecuteAlterWithDryRun(alterStatement string, dryRun bool) error {
	args := m.Called(alterStatement, dryRun)
	return args.Error(0)
}

func (m *MockDBClient) CheckMetadataLock(table string, thresholdSeconds int) (bool, error) {
	args := m.Called(table, thresholdSeconds)
	return args.Bool(0), args.Error(1)
}

func (m *MockDBClient) SetSessionConfig(lockWaitTimeout, innodbLockWaitTimeout int) error {
	args := m.Called(lockWaitTimeout, innodbLockWaitTimeout)
	return args.Error(0)
}

func (m *MockDBClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockPtOscExecutor struct {
	mock.Mock
}

func (m *MockPtOscExecutor) ExecuteAlter(tableName, alterStatement string, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool) error {
	args := m.Called(tableName, alterStatement, ptOscConfig, dsn, forceDryRun)
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
		queries        []string
		rowCounts      map[string]int64
		expectError    bool
		expectedMethod string
		initMock       func([]string, map[string]int64, *MockDBClient, *MockPtOscExecutor, *MockSlackNotifier)
	}{
		{
			name: "all small queries",
			queries: []string{
				"ALTER TABLE table1 ADD COLUMN foo INT",
				"ALTER TABLE table2 ADD COLUMN bar INT",
			},
			rowCounts: map[string]int64{
				"table1": 500,
				"table2": 800,
			},
			expectError:    false,
			expectedMethod: "ALTER",
			initMock: func(queries []string, rowCounts map[string]int64, d *MockDBClient, p *MockPtOscExecutor, m *MockSlackNotifier) {
				for tableName, rowCount := range rowCounts {
					d.On("GetTableRowCount", tableName).Return(rowCount, nil)
				}
				for _, query := range queries {
					d.On("ExecuteAlter", query).Return(nil)
				}
			},
		},
		{
			name: "one large table",
			queries: []string{
				"ALTER TABLE table1 ADD COLUMN foo INT",
				"ALTER TABLE table2 ADD COLUMN bar INT",
			},
			rowCounts: map[string]int64{
				"table1": 500,
				"table2": 2000,
			},
			expectError:    false,
			expectedMethod: "PT-OSC",
			initMock: func(queries []string, rowCounts map[string]int64, d *MockDBClient, p *MockPtOscExecutor, m *MockSlackNotifier) {
				for tableName, rowCount := range rowCounts {
					d.On("GetTableRowCount", tableName).Return(rowCount, nil)
				}
				d.On("ExecuteAlter", "ALTER TABLE table1 ADD COLUMN foo INT").Return(nil)

				m.On("NotifyStart", "large-alter", "table2", int64(2000)).Return(nil)
				m.On("NotifySuccess", "large-alter", "table2", int64(2000), mock.Anything).Return(nil)
				p.On("ExecuteAlter", "table2", "ADD COLUMN bar INT", config.PtOscConfig{}, "test-dsn", false).Return(nil)
			},
		},
		{
			name: "mixed queries",
			queries: []string{
				"CREATE TABLE new_table (id INT PRIMARY KEY)",
				"ALTER TABLE existing_table ADD COLUMN new_col INT",
				"DROP TABLE old_table",
			},
			rowCounts: map[string]int64{
				"existing_table": 500,
			},
			expectError:    false,
			expectedMethod: "MIXED",
			initMock: func(queries []string, rowCounts map[string]int64, d *MockDBClient, p *MockPtOscExecutor, m *MockSlackNotifier) {
				for tableName, rowCount := range rowCounts {
					d.On("GetTableRowCount", tableName).Return(rowCount, nil)
				}
				for _, query := range queries {
					d.On("ExecuteAlter", query).Return(nil)
				}
			},
		},
		{
			name: "dry run mode",
			queries: []string{
				"CREATE TABLE test_table (id INT PRIMARY KEY)",
				"ALTER TABLE table2 ADD COLUMN bar INT",
				"DROP TABLE old_table",
			},
			rowCounts: map[string]int64{
				"table2": 500,
			},
			expectError:    false,
			expectedMethod: "DRY_RUN",
			initMock: func(queries []string, rowCounts map[string]int64, d *MockDBClient, p *MockPtOscExecutor, m *MockSlackNotifier) {
				for tableName, rowCount := range rowCounts {
					d.On("GetTableRowCount", tableName).Return(rowCount, nil)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logrus.New()
			logger.SetLevel(logrus.FatalLevel)

			mockDB := &MockDBClient{}
			mockPtOsc := &MockPtOscExecutor{}
			mockSlack := &MockSlackNotifier{}
			if tt.initMock != nil {
				tt.initMock(tt.queries, tt.rowCounts, mockDB, mockPtOsc, mockSlack)
			}

			cfg := &config.Config{
				Queries: tt.queries,
				Common: config.CommonConfig{
					PtOsc:          config.PtOscConfig{},
					PtOscThreshold: 1000,
				},
				DSN: "test-dsn",
			}

			dryRun := tt.expectedMethod == "DRY_RUN"
			manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, dryRun)
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
					SessionConfig: config.SessionConfig{
						LockWaitTimeout:       0,
						InnodbLockWaitTimeout: 0,
					},
				},
			}

			manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, false)

			mockDB.On("SetSessionConfig", 0, 0).Return(nil)

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
