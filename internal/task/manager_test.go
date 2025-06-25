package task

import (
	"errors"
	"fmt"
	"strings"
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

func (m *MockDBClient) GetNewTableRowCount(tableName string) (int64, error) {
	args := m.Called(tableName)
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

func (m *MockDBClient) SetSessionConfig(lockWaitTimeout, innodbLockWaitTimeout int) error {
	args := m.Called(lockWaitTimeout, innodbLockWaitTimeout)
	return args.Error(0)
}

func (m *MockDBClient) TableExists(tableName string) (bool, error) {
	args := m.Called(tableName)
	return args.Bool(0), args.Error(1)
}

func (m *MockDBClient) HasOtherActiveConnections() (bool, string, error) {
	args := m.Called()
	return args.Bool(0), args.String(1), args.Error(2)
}

func (m *MockDBClient) GetCurrentUser() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *MockDBClient) CheckNewTableExists(tableName string) (bool, error) {
	args := m.Called(tableName)
	return args.Bool(0), args.Error(1)
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

func (m *MockSlackNotifier) NotifySuccessWithQueryAndLog(taskName, tableName, query string, rowCount int64, duration time.Duration, ptOscLog string) error {
	args := m.Called(taskName, tableName, query, rowCount, duration, ptOscLog)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyFailureWithQueryAndLog(taskName, tableName, query string, rowCount int64, err error, ptOscLog string) error {
	args := m.Called(taskName, tableName, query, rowCount, err, ptOscLog)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyPtOscCompletionWithNewTableCount(taskName, tableName string, originalRowCount, newRowCount int64, duration time.Duration, ptOscLog string) error {
	args := m.Called(taskName, tableName, originalRowCount, newRowCount, duration, ptOscLog)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyDryRunResult(taskName, tableName string, result *slack.DryRunResult, duration time.Duration) error {
	args := m.Called(taskName, tableName, result, duration)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyConnectionCheckFailure(taskName, tableName, username string) error {
	args := m.Called(taskName, tableName, username)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyTriggerCleanupStart(taskName, tableName string, triggers []string) error {
	args := m.Called(taskName, tableName, triggers)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyTriggerCleanupSuccess(taskName, tableName string, triggers []string, duration time.Duration) error {
	args := m.Called(taskName, tableName, triggers, duration)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyTriggerCleanupFailure(taskName, tableName string, triggers []string, err error) error {
	args := m.Called(taskName, tableName, triggers, err)
	return args.Error(0)
}

func (m *MockSlackNotifier) NotifyPtOscPreCheckFailure(taskName, tableName string) error {
	args := m.Called(taskName, tableName)
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
				d.On("CheckNewTableExists", "table2").Return(false, nil) // 事前チェック: _table2_newは存在しない
				largeAlterQuery := "ALTER: `ALTER TABLE table2 ADD COLUMN bar INT`\npt-osc: `pt-online-schema-change --alter='ADD COLUMN bar INT' --execute`"
				m.On("NotifyStartWithQuery", "pt-osc", "table2", largeAlterQuery, int64(2000)).Return(nil)
				m.On("NotifyPtOscCompletionWithNewTableCount", "pt-osc", "table2", int64(2000), int64(1950), mock.Anything, mock.Anything).Return(nil)
				p.On("ExecuteAlter", "table2", "ADD COLUMN bar INT", config.PtOscConfig{}, "test-dsn", false).Return(nil)
				d.On("GetNewTableRowCount", "table2").Return(int64(1950), nil)
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
					ConnectionCheck: config.ConnectionCheckConfig{
						Enabled: false, // テストでは無効にする
					},
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
		name                string
		tableName           string
		originalTableExists bool
		newTableExists      bool
		tableExistsError    error
		swapError           error
		expectError         bool
		executionThreshold  int
		expectWarning       bool
	}{
		{
			name:                "successful swap",
			tableName:           "test_table",
			originalTableExists: true,
			newTableExists:      true,
			expectError:         false,
		},
		{
			name:                "original table does not exist",
			tableName:           "test_table",
			originalTableExists: false,
			newTableExists:      true,
			expectError:         true,
		},
		{
			name:                "new table does not exist",
			tableName:           "test_table",
			originalTableExists: true,
			newTableExists:      false,
			expectError:         true,
		},
		{
			name:             "table exists check error",
			tableName:        "test_table",
			tableExistsError: errors.New("table exists check failed"),
			expectError:      true,
		},
		{
			name:                "swap error",
			tableName:           "test_table",
			originalTableExists: true,
			newTableExists:      true,
			swapError:           errors.New("swap failed"),
			expectError:         true,
		},
		{
			name:                "dry run mode",
			tableName:           "test_table",
			originalTableExists: true,
			newTableExists:      true,
			expectError:         false,
		},
		{
			name:                "execution time threshold exceeded",
			tableName:           "test_table",
			originalTableExists: true,
			newTableExists:      true,
			expectError:         false,
			executionThreshold:  1,
			expectWarning:       true,
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
						ExecutionTimeThresholdSeconds: tt.executionThreshold,
					},
					SessionConfig: config.SessionConfig{
						LockWaitTimeout:       0,
						InnodbLockWaitTimeout: 0,
					},
				},
			}

			isDryRun := tt.name == "dry run mode"
			manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, isDryRun)

			// テーブル存在確認のモック設定
			if tt.tableExistsError != nil {
				mockDB.On("TableExists", tt.tableName).Return(false, tt.tableExistsError)
			} else {
				mockDB.On("TableExists", tt.tableName).Return(tt.originalTableExists, nil)
				if tt.originalTableExists {
					newTableName := fmt.Sprintf("_%s_new", tt.tableName)
					mockDB.On("TableExists", newTableName).Return(tt.newTableExists, nil)
				}
			}

			// テーブルが存在しない場合は早期リターンするため、以下の処理は実行されない
			if !tt.originalTableExists || !tt.newTableExists || tt.tableExistsError != nil {
				err := manager.SwapTable(tt.tableName)
				assert.Error(t, err)
				mockDB.AssertExpectations(t)
				return
			}

			expectedQuery := fmt.Sprintf("`RENAME TABLE %s TO %s_old, _%s_new TO %s`", tt.tableName, tt.tableName, tt.tableName, tt.tableName)
			taskName := "swap"
			if isDryRun {
				taskName = "swap (DRY RUN)"
			}
			mockSlack.On("NotifyStartWithQuery", taskName, tt.tableName, expectedQuery, int64(0)).Return(nil)

			mockDB.On("SetSessionConfig", 0, 0).Return(nil)

			if tt.swapError != nil {
				mockDB.On("ExecuteAlter", mock.AnythingOfType("string")).Return(tt.swapError)
				mockSlack.On("NotifyFailureWithQuery", taskName, tt.tableName, expectedQuery, int64(0), tt.swapError).Return(nil)
			} else {
				if !isDryRun {
					if tt.expectWarning {
						// ExecuteAlterを2秒間ブロックして、concurrent monitoringをテスト
						mockDB.On("ExecuteAlter", mock.AnythingOfType("string")).Run(func(args mock.Arguments) {
							time.Sleep(2 * time.Second) // 2秒待機してthresholdを超える
						}).Return(nil)
						mockSlack.On("NotifyWarning", taskName, tt.tableName, mock.MatchedBy(func(msg string) bool {
							return strings.Contains(msg, "Long execution time detected")
						})).Return(nil)
					} else {
						mockDB.On("ExecuteAlter", mock.AnythingOfType("string")).Return(nil)
					}
				}
				mockSlack.On("NotifySuccessWithQuery", taskName, tt.tableName, expectedQuery, int64(0), mock.Anything).Return(nil)
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
	tests := []struct {
		name          string
		tableName     string
		dryRun        bool
		triggerErrors map[string]error
		expectError   bool
	}{
		{
			name:        "successful cleanup",
			tableName:   "test_table",
			dryRun:      false,
			expectError: false,
		},
		{
			name:        "dry run cleanup",
			tableName:   "test_table",
			dryRun:      true,
			expectError: false,
		},
		{
			name:      "partial failure",
			tableName: "test_table",
			dryRun:    false,
			triggerErrors: map[string]error{
				"DROP TRIGGER IF EXISTS pt_osc_test_table_del": errors.New("trigger drop failed"),
			},
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

			cfg := &config.Config{}
			manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, tt.dryRun)

			expectedTriggers := []string{
				"pt_osc_test_table_del",
				"pt_osc_test_table_upd",
				"pt_osc_test_table_ins",
			}

			taskName := "trigger-cleanup"
			if tt.dryRun {
				taskName = "trigger-cleanup (DRY RUN)"
			}

			mockSlack.On("NotifyTriggerCleanupStart", taskName, tt.tableName, expectedTriggers).Return(nil)

			if !tt.dryRun {
				expectedSQL := []string{
					"DROP TRIGGER IF EXISTS pt_osc_test_table_del",
					"DROP TRIGGER IF EXISTS pt_osc_test_table_upd",
					"DROP TRIGGER IF EXISTS pt_osc_test_table_ins",
				}

				for _, sql := range expectedSQL {
					if err, exists := tt.triggerErrors[sql]; exists {
						mockDB.On("ExecuteAlter", sql).Return(err)
					} else {
						mockDB.On("ExecuteAlter", sql).Return(nil)
					}
				}
			}

			if tt.expectError {
				mockSlack.On("NotifyTriggerCleanupFailure", taskName, tt.tableName, expectedTriggers, mock.Anything).Return(nil)
			} else {
				mockSlack.On("NotifyTriggerCleanupSuccess", taskName, tt.tableName, expectedTriggers, mock.Anything).Return(nil)
			}

			err := manager.CleanupTriggers(tt.tableName)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			mockDB.AssertExpectations(t)
			mockSlack.AssertExpectations(t)
		})
	}
}

func TestPtOscWithNewTableCount(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	mockDB := &MockDBClient{}
	mockPtOsc := &MockPtOscExecutor{}
	mockSlack := &MockSlackNotifier{}

	cfg := &config.Config{
		Queries: []string{"ALTER TABLE large_table ADD COLUMN new_col INT"},
		Common: config.CommonConfig{
			PtOsc:          config.PtOscConfig{},
			PtOscThreshold: 1000,
			ConnectionCheck: config.ConnectionCheckConfig{
				Enabled: false,
			},
		},
		DSN: "test-dsn",
	}

	// 大きなテーブル（pt-oscを使用）
	mockDB.On("GetTableRowCount", "large_table").Return(int64(5000), nil)
	mockDB.On("CheckNewTableExists", "large_table").Return(false, nil) // 事前チェック: _large_table_newは存在しない
	mockDB.On("GetNewTableRowCount", "large_table").Return(int64(5001), nil)

	largeAlterQuery := "ALTER: `ALTER TABLE large_table ADD COLUMN new_col INT`\npt-osc: `pt-online-schema-change --alter='ADD COLUMN new_col INT' --execute`"
	mockSlack.On("NotifyStartWithQuery", "pt-osc", "large_table", largeAlterQuery, int64(5000)).Return(nil)
	mockSlack.On("NotifyPtOscCompletionWithNewTableCount", "pt-osc", "large_table", int64(5000), int64(5001), mock.Anything, mock.Anything).Return(nil)
	mockPtOsc.On("ExecuteAlter", "large_table", "ADD COLUMN new_col INT", config.PtOscConfig{}, "test-dsn", false).Return(nil)

	manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, false)
	err := manager.ExecuteAllTasks()

	require.NoError(t, err)
	mockDB.AssertExpectations(t)
	mockPtOsc.AssertExpectations(t)
	mockSlack.AssertExpectations(t)
}

func TestSwapTableConcurrentMonitoring(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)

	mockDB := &MockDBClient{}
	mockPtOsc := &MockPtOscExecutor{}
	mockSlack := &MockSlackNotifier{}

	cfg := &config.Config{
		Common: config.CommonConfig{
			Alert: config.AlertConfig{
				ExecutionTimeThresholdSeconds: 1, // 1秒でタイムアウト
			},
			SessionConfig: config.SessionConfig{
				LockWaitTimeout:       0,
				InnodbLockWaitTimeout: 0,
			},
		},
	}

	manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, false)

	tableName := "test_table"
	expectedQuery := fmt.Sprintf("`RENAME TABLE %s TO %s_old, _%s_new TO %s`", tableName, tableName, tableName, tableName)

	// テーブル存在確認のモック設定
	mockDB.On("TableExists", tableName).Return(true, nil)
	newTableName := fmt.Sprintf("_%s_new", tableName)
	mockDB.On("TableExists", newTableName).Return(true, nil)

	mockSlack.On("NotifyStartWithQuery", "swap", tableName, expectedQuery, int64(0)).Return(nil)
	mockDB.On("SetSessionConfig", 0, 0).Return(nil)

	// ExecuteAlterを2秒間ブロックして、concurrent monitoringをテスト
	mockDB.On("ExecuteAlter", mock.AnythingOfType("string")).Run(func(args mock.Arguments) {
		time.Sleep(2 * time.Second) // 2秒待機してthresholdを超える
	}).Return(nil)

	// 警告通知が呼ばれることを期待
	mockSlack.On("NotifyWarning", "swap", tableName, mock.MatchedBy(func(msg string) bool {
		return strings.Contains(msg, "Long execution time detected") && strings.Contains(msg, "operation is taking longer than 1 seconds")
	})).Return(nil)

	mockSlack.On("NotifySuccessWithQuery", "swap", tableName, expectedQuery, int64(0), mock.Anything).Return(nil)

	start := time.Now()
	err := manager.SwapTable(tableName)
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.True(t, duration >= 2*time.Second, "Test should take at least 2 seconds to verify concurrent monitoring")

	// 少し待ってからアサーションを実行（goroutineが完了するのを待つ）
	time.Sleep(100 * time.Millisecond)

	mockDB.AssertExpectations(t)
	mockSlack.AssertExpectations(t)
}

func TestConnectionCheck(t *testing.T) {
	tests := []struct {
		name                   string
		connectionCheckEnabled bool
		hasOtherConnections    bool
		connectionCheckError   error
		username               string
		expectError            bool
		expectNotification     bool
	}{
		{
			name:                   "connection check disabled",
			connectionCheckEnabled: false,
			hasOtherConnections:    true,
			username:               "testuser",
			expectError:            false,
			expectNotification:     false,
		},
		{
			name:                   "no other connections",
			connectionCheckEnabled: true,
			hasOtherConnections:    false,
			username:               "testuser",
			expectError:            false,
			expectNotification:     false,
		},
		{
			name:                   "other connections detected",
			connectionCheckEnabled: true,
			hasOtherConnections:    true,
			username:               "testuser",
			expectError:            true,
			expectNotification:     true,
		},
		{
			name:                   "connection check error",
			connectionCheckEnabled: true,
			connectionCheckError:   errors.New("connection check failed"),
			username:               "testuser",
			expectError:            true,
			expectNotification:     false,
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
				Queries: []string{"ALTER TABLE test_table ADD COLUMN foo INT"},
				Common: config.CommonConfig{
					PtOsc:          config.PtOscConfig{},
					PtOscThreshold: 1000,
					ConnectionCheck: config.ConnectionCheckConfig{
						Enabled: tt.connectionCheckEnabled,
					},
				},
				DSN: "test-dsn",
			}

			manager := NewManager(mockDB, mockPtOsc, mockSlack, logger, cfg, false)

			// 接続チェックが有効な場合のモック設定
			if tt.connectionCheckEnabled {
				if tt.connectionCheckError != nil {
					mockDB.On("HasOtherActiveConnections").Return(false, "", tt.connectionCheckError)
				} else {
					mockDB.On("HasOtherActiveConnections").Return(tt.hasOtherConnections, tt.username, nil)
					if tt.expectNotification {
						mockSlack.On("NotifyConnectionCheckFailure", "alter-table", "test_table", tt.username).Return(nil)
					}
				}
			}

			// GetTableRowCountは接続チェック前に呼ばれるため、常にモックを設定
			mockDB.On("GetTableRowCount", "test_table").Return(int64(500), nil)

			// 接続チェックが成功した場合の通常処理のモック
			if !tt.expectError {
				mockSlack.On("NotifyStartWithQuery", "alter-table", "test_table", "`ALTER TABLE test_table ADD COLUMN foo INT`", int64(500)).Return(nil)
				mockSlack.On("NotifySuccessWithQuery", "alter-table", "test_table", "`ALTER TABLE test_table ADD COLUMN foo INT`", int64(500), mock.Anything).Return(nil)
				mockDB.On("ExecuteAlter", "ALTER TABLE test_table ADD COLUMN foo INT").Return(nil)
			}

			err := manager.ExecuteAllTasks()

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
