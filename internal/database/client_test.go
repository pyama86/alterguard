package database

import (
	"database/sql"
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
		name           string
		table          string
		innodbReturn   any
		innodbError    error
		countReturn    any
		countError     error
		expectCount    int64
		expectError    bool
		expectFallback bool
	}{
		{
			name:           "successful innodb count",
			table:          "users",
			innodbReturn:   int64(1000),
			innodbError:    nil,
			expectCount:    1000,
			expectError:    false,
			expectFallback: false,
		},
		{
			name:           "innodb fails, count succeeds",
			table:          "users",
			innodbReturn:   nil,
			innodbError:    sql.ErrNoRows,
			countReturn:    int64(500),
			countError:     nil,
			expectCount:    500,
			expectError:    false,
			expectFallback: true,
		},
		{
			name:           "both fail",
			table:          "nonexistent",
			innodbReturn:   nil,
			innodbError:    sql.ErrNoRows,
			countReturn:    nil,
			countError:     assert.AnError,
			expectCount:    0,
			expectError:    true,
			expectFallback: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := &MockDB{}
			logger := logrus.New()
			logger.SetLevel(logrus.PanicLevel)
			client := &MySQLClient{db: nil, logger: logger}

			// INNODB_SYS_TABLESTATSクエリのモック設定
			if tt.innodbError != nil {
				mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
					return strings.Contains(query, "INNODB_SYS_TABLESTATS")
				}), tt.table).Return(tt.innodbError)
			} else {
				mockDB.On("Get", mock.Anything, mock.MatchedBy(func(query string) bool {
					return strings.Contains(query, "INNODB_SYS_TABLESTATS")
				}), tt.table).Run(func(args mock.Arguments) {
					dest := args.Get(0).(*int64)
					*dest = tt.innodbReturn.(int64)
				}).Return(nil)
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
