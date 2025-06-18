package task

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/pyama86/alterguard/internal/ptosc"
	"github.com/pyama86/alterguard/internal/slack"
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

func (m *MockPtOscExecutor) ExecuteAlterWithDryRunResult(tableName, alterStatement string, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool) (*ptosc.DryRunResult, error) {
	args := m.Called(tableName, alterStatement, ptOscConfig, dsn, forceDryRun)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ptosc.DryRunResult), args.Error(1)
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

func (m *MockSlackNotifier) NotifyStartWithQuery(taskName, tableName, query string, rowCount int64) error {
	args := m.Called(taskName, tableName, query, rowCount)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifySuccessWithQuery(taskName, tableName, query string, rowCount int64, duration time.Duration) error {
	args := m.Called(taskName, tableName, query, rowCount, duration)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyFailureWithQuery(taskName, tableName, query string, rowCount int64, err error) error {
	args := m.Called(taskName, tableName, query, rowCount, err)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyDryRunResult(taskName, tableName string, result *slack.DryRunResult, duration time.Duration) error {
	args := m.Called(taskName, tableName, result, duration)
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
					combinedQuery := fmt.Sprintf("`ALTER TABLE %s ADD COLUMN foo INT`", tableName)
					if tableName == "table2" {
						combinedQuery = fmt.Sprintf("`ALTER TABLE %s ADD COLUMN bar INT`", tableName)
					}
					m.On("NotifyStartWithQuery", "alter-table", tableName, combinedQuery, rowCount).Return(nil)
					m.On("NotifySuccessWithQuery", "alter-table", tableName, combinedQuery, rowCount, mock.Anything).Return(nil)
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

				// table1 is small (500 rows), so it uses alter-table
				m.On("NotifyStartWithQuery", "alter-table", "table1", "`ALTER TABLE table1 ADD COLUMN foo INT`", int64(500)).Return(nil)
				m.On("NotifySuccessWithQuery", "alter-table", "table1", "`ALTER TABLE table1 ADD COLUMN foo INT`", int64(500), mock.Anything).Return(nil)

				// table2 is large (2000 rows), so it uses pt-osc
				largeAlterQuery := "ALTER: `ALTER TABLE table2 ADD COLUMN bar INT`\npt-osc: `pt-online-schema-change --alter='ADD COLUMN bar INT' --execute`"
				m.On("NotifyStartWithQuery", "pt-osc", "table2", largeAlterQuery, int64(2000)).Return(nil)
				m.On("NotifySuccessWithQuery", "pt-osc", "table2", largeAlterQuery, int64(2000), mock.Anything).Return(nil)
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
					m.On("NotifyStartWithQuery", "alter-table", tableName, "`ALTER TABLE existing_table ADD COLUMN new_col INT`", rowCount).Return(nil)
					m.On("NotifySuccessWithQuery", "alter-table", tableName, "`ALTER TABLE existing_table ADD COLUMN new_col INT`", rowCount, mock.Anything).Return(nil)
				}

				// small-query (CREATE TABLE new_table)
				d.On("GetTableRowCount", "new_table").Return(int64(0), errors.New("table not found"))
				m.On("NotifyStartWithQuery", "small-query", "new_table", "`CREATE TABLE new_table (id INT PRIMARY KEY)`", int64(0)).Return(nil)
				m.On("NotifySuccessWithQuery", "small-query", "new_table", "`CREATE TABLE new_table (id INT PRIMARY KEY)`", int64(0), mock.Anything).Return(nil)

				// small-query (DROP TABLE old_table)
				d.On("GetTableRowCount", "old_table").Return(int64(0), errors.New("table not found"))
				m.On("NotifyStartWithQuery", "small-query", "old_table", "`DROP TABLE old_table`", int64(0)).Return(nil)
				m.On("NotifySuccessWithQuery", "small-query", "old_table", "`DROP TABLE old_table`", int64(0), mock.Anything).Return(nil)

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
					m.On("NotifyStartWithQuery", "alter-table (DRY RUN)", tableName, "`ALTER TABLE table2 ADD COLUMN bar INT`", rowCount).Return(nil)
					m.On("NotifySuccessWithQuery", "alter-table (DRY RUN)", tableName, "`ALTER TABLE table2 ADD COLUMN bar INT`", rowCount, mock.Anything).Return(nil)
				}
				// CREATE TABLE test_table
				d.On("GetTableRowCount", "test_table").Return(int64(0), errors.New("table not found"))
				m.On("NotifyStartWithQuery", "small-query (DRY RUN)", "test_table", "`CREATE TABLE test_table (id INT PRIMARY KEY)`", int64(0)).Return(nil)
				m.On("NotifySuccessWithQuery", "small-query (DRY RUN)", "test_table", "`CREATE TABLE test_table (id INT PRIMARY KEY)`", int64(0), mock.Anything).Return(nil)

				// DROP TABLE old_table
				d.On("GetTableRowCount", "old_table").Return(int64(0), errors.New("table not found"))
				m.On("NotifyStartWithQuery", "small-query (DRY RUN)", "old_table", "`DROP TABLE old_table`", int64(0)).Return(nil)
				m.On("NotifySuccessWithQuery", "small-query (DRY RUN)", "old_table", "`DROP TABLE old_table`", int64(0), mock.Anything).Return(nil)
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
		{
			name:          "dry run mode",
			tableName:     "test_table",
			lockDetected:  false,
			expectError:   false,
			expectWarning: false,
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

			isDryRun := tt.name == "dry run mode"
			manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, isDryRun)

			expectedQuery := fmt.Sprintf("`RENAME TABLE %s TO %s_old, _%s_new TO %s`", tt.tableName, tt.tableName, tt.tableName, tt.tableName)
			taskName := "swap"
			if isDryRun {
				taskName = "swap (DRY RUN)"
			}
			mockSlack.On("NotifyStartWithQuery", taskName, tt.tableName, expectedQuery, int64(0)).Return(nil)

			mockDB.On("SetSessionConfig", 0, 0).Return(nil)

			if tt.lockCheckError != nil {
				mockDB.On("CheckMetadataLock", tt.tableName, 30).Return(false, tt.lockCheckError)
				mockSlack.On("NotifyFailureWithQuery", taskName, tt.tableName, expectedQuery, int64(0), tt.lockCheckError).Return(nil)
			} else {
				mockDB.On("CheckMetadataLock", tt.tableName, 30).Return(tt.lockDetected, nil)

				if tt.expectWarning {
					mockSlack.On("NotifyWarning", "swap", tt.tableName, mock.AnythingOfType("string")).Return(nil)
				}

				if tt.swapError != nil {
					mockDB.On("ExecuteAlter", mock.AnythingOfType("string")).Return(tt.swapError)
					mockSlack.On("NotifyFailureWithQuery", taskName, tt.tableName, expectedQuery, int64(0), tt.swapError).Return(nil)
				} else {
					if !isDryRun {
						mockDB.On("ExecuteAlter", mock.AnythingOfType("string")).Return(nil)
					}
					mockSlack.On("NotifySuccessWithQuery", taskName, tt.tableName, expectedQuery, int64(0), mock.Anything).Return(nil)
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
	tests := []struct {
		name      string
		tableName string
		dryRun    bool
	}{
		{
			name:      "normal cleanup",
			tableName: "test_table",
			dryRun:    false,
		},
		{
			name:      "dry run cleanup",
			tableName: "test_table",
			dryRun:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logrus.New()
			logger.SetLevel(logrus.FatalLevel)

			mockDB := &MockDBClient{}
			mockPtOsc := &MockPtOscExecutor{}
			mockSlack := &MockSlackNotifier{}

			cfg := &config.Config{}
			manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, tt.dryRun)

			expectedSQL := "DROP TABLE IF EXISTS test_table_old"
			expectedQuery := "`DROP TABLE IF EXISTS test_table_old`"
			taskName := "cleanup"
			if tt.dryRun {
				taskName = "cleanup (DRY RUN)"
			}

			mockSlack.On("NotifyStartWithQuery", taskName, tt.tableName, expectedQuery, int64(0)).Return(nil)
			mockSlack.On("NotifySuccessWithQuery", taskName, tt.tableName, expectedQuery, int64(0), mock.Anything).Return(nil)

			if !tt.dryRun {
				mockDB.On("ExecuteAlter", expectedSQL).Return(nil)
			}

			err := manager.CleanupTable(tt.tableName)

			require.NoError(t, err)
			mockDB.AssertExpectations(t)
			mockSlack.AssertExpectations(t)
		})
	}
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
