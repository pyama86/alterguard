package database

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockDB struct {
	mock.Mock
}

func (m *MockDB) Get(dest any, query string, args ...any) error {
	mockArgs := []any{dest, query}
	mockArgs = append(mockArgs, args...)
	ret := m.Called(mockArgs...)
	return ret.Error(0)
}

func (m *MockDB) Exec(query string, args ...any) (sql.Result, error) {
	mockArgs := []any{query}
	mockArgs = append(mockArgs, args...)
	ret := m.Called(mockArgs...)
	if ret.Get(0) == nil {
		return nil, ret.Error(1)
	}
	return ret.Get(0).(sql.Result), ret.Error(1)
}

type MockResult struct {
	mock.Mock
}

func (m *MockResult) LastInsertId() (int64, error) {
	ret := m.Called()
	return ret.Get(0).(int64), ret.Error(1)
}

func (m *MockResult) RowsAffected() (int64, error) {
	ret := m.Called()
	return ret.Get(0).(int64), ret.Error(1)
}

type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) Debugf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Infof(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Warnf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Errorf(format string, args ...interface{}) {
	m.Called(format, args)
}

func TestGetTableRowCount(t *testing.T) {
	tests := []struct {
		name             string
		table            string
		innodbSysReturn  any
		innodbSysError   error
		innodbReturn     any
		innodbError      error
		tablesReturn     any
		tablesError      error
		countReturn      any
		countError       error
		expectCount      int64
		expectError      bool
		expectFallback   bool
		expectUseInnodb8 bool
		expectUseTables  bool
	}{
		{
			name:             "successful innodb_sys_tablestats",
			table:            "users",
			innodbSysReturn:  int64(1000),
			innodbSysError:   nil,
			expectCount:      1000,
			expectError:      false,
			expectFallback:   false,
			expectUseInnodb8: false,
			expectUseTables:  false,
		},
		{
			name:             "innodb_sys fails, innodb_tablestats succeeds",
			table:            "users",
			innodbSysReturn:  nil,
			innodbSysError:   sql.ErrNoRows,
			innodbReturn:     int64(800),
			innodbError:      nil,
			expectCount:      800,
			expectError:      false,
			expectFallback:   false,
			expectUseInnodb8: true,
			expectUseTables:  false,
		},
		{
			name:             "innodb fails, information_schema.tables succeeds",
			table:            "users",
			innodbSysReturn:  nil,
			innodbSysError:   sql.ErrNoRows,
			innodbReturn:     nil,
			innodbError:      sql.ErrNoRows,
			tablesReturn:     int64(600),
			tablesError:      nil,
			expectCount:      600,
			expectError:      false,
			expectFallback:   false,
			expectUseInnodb8: true,
			expectUseTables:  true,
		},
		{
			name:             "all stats tables fail, count succeeds",
			table:            "users",
			innodbSysReturn:  nil,
			innodbSysError:   sql.ErrNoRows,
			innodbReturn:     nil,
			innodbError:      sql.ErrNoRows,
			tablesReturn:     nil,
			tablesError:      sql.ErrNoRows,
			countReturn:      int64(500),
			countError:       nil,
			expectCount:      500,
			expectError:      false,
			expectFallback:   true,
			expectUseInnodb8: true,
			expectUseTables:  true,
		},
		{
			name:             "all methods fail",
			table:            "nonexistent",
			innodbSysReturn:  nil,
			innodbSysError:   sql.ErrNoRows,
			innodbReturn:     nil,
			innodbError:      sql.ErrNoRows,
			tablesReturn:     nil,
			tablesError:      sql.ErrNoRows,
			countReturn:      nil,
			countError:       assert.AnError,
			expectCount:      0,
			expectError:      true,
			expectFallback:   true,
			expectUseInnodb8: true,
			expectUseTables:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := &MockDB{}
			logger := logrus.New()
			logger.SetLevel(logrus.PanicLevel)
			client := &MySQLClient{db: nil, logger: logger}

			// INNODB_SYS_TABLESTATSクエリのモック設定
			if tt.innodbSysError != nil {
				mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
					return strings.Contains(query, "INNODB_SYS_TABLESTATS")
				}), tt.table).Return(tt.innodbSysError)
			} else {
				mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
					return strings.Contains(query, "INNODB_SYS_TABLESTATS")
				}), tt.table).Run(func(args mock.Arguments) {
					dest := args.Get(0).(*int64)
					*dest = tt.innodbSysReturn.(int64)
				}).Return(nil)
			}

			// INNODB_TABLESTATSクエリのモック設定 (MySQL 8.0)
			if tt.expectUseInnodb8 {
				if tt.innodbError != nil {
					mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
						return strings.Contains(query, "INNODB_TABLESTATS") && !strings.Contains(query, "INNODB_SYS_TABLESTATS")
					}), tt.table).Return(tt.innodbError)
				} else {
					mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
						return strings.Contains(query, "INNODB_TABLESTATS") && !strings.Contains(query, "INNODB_SYS_TABLESTATS")
					}), tt.table).Run(func(args mock.Arguments) {
						dest := args.Get(0).(*int64)
						*dest = tt.innodbReturn.(int64)
					}).Return(nil)
				}
			}

			// information_schema.TABLESクエリのモック設定
			if tt.expectUseTables {
				if tt.tablesError != nil {
					mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
						return strings.Contains(query, "TABLE_ROWS") && strings.Contains(query, "information_schema.TABLES")
					}), tt.table).Return(tt.tablesError)
				} else {
					mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
						return strings.Contains(query, "TABLE_ROWS") && strings.Contains(query, "information_schema.TABLES")
					}), tt.table).Run(func(args mock.Arguments) {
						dest := args.Get(0).(*int64)
						*dest = tt.tablesReturn.(int64)
					}).Return(nil)
				}
			}

			// フォールバック用のCOUNT(*)クエリのモック設定
			if tt.expectFallback {
				if tt.countError != nil {
					mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
						return strings.Contains(query, "COUNT(*)")
					})).Return(tt.countError)
				} else {
					mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
						return strings.Contains(query, "COUNT(*)")
					})).Run(func(args mock.Arguments) {
						dest := args.Get(0).(*int64)
						*dest = tt.countReturn.(int64)
					}).Return(nil)
				}
			}

			count, err := client.getTableRowCountWithDB(mockDB, tt.table)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, int64(0), count)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectCount, count)
			}

			mockDB.AssertExpectations(t)
		})
	}
}

func TestExecuteAlterWithDryRun(t *testing.T) {
	tests := []struct {
		name           string
		alterStatement string
		dryRun         bool
		expectError    bool
	}{
		{
			name:           "dry run mode",
			alterStatement: "ADD COLUMN foo INT",
			dryRun:         true,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &MySQLClient{db: nil}

			err := client.ExecuteAlterWithDryRun(tt.alterStatement, tt.dryRun)

			if tt.dryRun {
				assert.NoError(t, err, "dry run should not return error")
			} else if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExecuteAlter(t *testing.T) {
	tests := []struct {
		name           string
		alterStatement string
		mockResult     sql.Result
		mockError      error
		expectError    bool
	}{
		{
			name:           "successful alter",
			alterStatement: "ADD COLUMN foo INT",
			mockResult:     &MockResult{},
			mockError:      nil,
			expectError:    false,
		},
		{
			name:           "alter error",
			alterStatement: "INVALID ALTER",
			mockResult:     nil,
			mockError:      assert.AnError,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := &MockDB{}
			client := &MySQLClient{db: nil}

			mockDB.On("Exec", tt.alterStatement).Return(tt.mockResult, tt.mockError)

			err := client.executeAlterWithDB(mockDB, tt.alterStatement)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockDB.AssertExpectations(t)
		})
	}
}

func TestGetTableRowCountForSwap(t *testing.T) {
	tests := []struct {
		name        string
		table       string
		countReturn int64
		countError  error
		expectCount int64
		expectError bool
	}{
		{
			name:        "successful count for swap",
			table:       "users",
			countReturn: 1000,
			countError:  nil,
			expectCount: 1000,
			expectError: false,
		},
		{
			name:        "count error for swap",
			table:       "nonexistent",
			countReturn: 0,
			countError:  assert.AnError,
			expectCount: 0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := &MockDB{}
			logger := logrus.New()
			logger.SetLevel(logrus.PanicLevel)
			client := &MySQLClient{db: nil, logger: logger}

			// COUNT(*)クエリのモック設定
			if tt.countError != nil {
				mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
					return strings.Contains(query, "COUNT(*)")
				})).Return(tt.countError)
			} else {
				mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
					return strings.Contains(query, "COUNT(*)")
				})).Run(func(args mock.Arguments) {
					dest := args.Get(0).(*int64)
					*dest = tt.countReturn
				}).Return(nil)
			}

			// テスト用に直接mockDBを使用
			count, err := client.getTableRowCountForSwapWithDB(mockDB, tt.table)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, int64(0), count)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectCount, count)
			}

			mockDB.AssertExpectations(t)
		})
	}
}

func TestGetNewTableRowCountForSwap(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		countReturn int64
		countError  error
		expectCount int64
		expectError bool
	}{
		{
			name:        "successful new table count for swap",
			tableName:   "users",
			countReturn: 1000,
			countError:  nil,
			expectCount: 1000,
			expectError: false,
		},
		{
			name:        "new table count error for swap",
			tableName:   "nonexistent",
			countReturn: 0,
			countError:  assert.AnError,
			expectCount: 0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := &MockDB{}
			logger := logrus.New()
			logger.SetLevel(logrus.PanicLevel)
			client := &MySQLClient{db: nil, logger: logger}

			// COUNT(*)クエリのモック設定 (_tableName_new用)
			expectedNewTableName := fmt.Sprintf("_%s_new", tt.tableName)
			if tt.countError != nil {
				mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
					return strings.Contains(query, "COUNT(*)")
				})).Return(tt.countError)
			} else {
				mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
					return strings.Contains(query, "COUNT(*)") && strings.Contains(query, expectedNewTableName)
				})).Run(func(args mock.Arguments) {
					dest := args.Get(0).(*int64)
					*dest = tt.countReturn
				}).Return(nil)
			}

			// テスト用に直接mockDBを使用
			count, err := client.getTableRowCountForSwapWithDB(mockDB, expectedNewTableName)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, int64(0), count)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectCount, count)
			}

			mockDB.AssertExpectations(t)
		})
	}
}
