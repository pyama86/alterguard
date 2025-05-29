package database

import (
	"database/sql"
	"testing"

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

func TestGetTableRowCount(t *testing.T) {
	tests := []struct {
		name        string
		table       string
		mockReturn  any
		mockError   error
		expectCount int64
		expectError bool
	}{
		{
			name:        "successful count",
			table:       "users",
			mockReturn:  int64(1000),
			mockError:   nil,
			expectCount: 1000,
			expectError: false,
		},
		{
			name:        "table not found",
			table:       "nonexistent",
			mockReturn:  nil,
			mockError:   sql.ErrNoRows,
			expectCount: 0,
			expectError: true,
		},
		{
			name:        "database error",
			table:       "users",
			mockReturn:  nil,
			mockError:   assert.AnError,
			expectCount: 0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := &MockDB{}
			client := &MySQLClient{db: nil}

			if tt.mockError != nil {
				mockDB.On("Get", mock.Anything, mock.AnythingOfType("string"), tt.table).Return(tt.mockError)
			} else {
				mockDB.On("Get", mock.Anything, mock.AnythingOfType("string"), tt.table).Run(func(args mock.Arguments) {
					dest := args.Get(0).(*int64)
					*dest = tt.mockReturn.(int64)
				}).Return(nil)
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
