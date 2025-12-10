package ptarchiver

import (
	"testing"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildArgsWithPassword(t *testing.T) {
	logger := logrus.New()
	executor := NewPtArchiverExecutor(logger)

	tests := []struct {
		name                 string
		tableName            string
		ptArchiverConfig     config.PtArchiverConfig
		dsn                  string
		dryRun               bool
		expectedArgsContains []string
		expectedPassword     string
	}{
		{
			name:      "basic configuration",
			tableName: "users_old",
			ptArchiverConfig: config.PtArchiverConfig{
				Limit:          10000,
				CommitEach:     true,
				Progress:       10000,
				MaxLag:         2.0,
				NoCheckCharset: true,
				BulkDelete:     true,
				PrimaryKeyOnly: true,
				Statistics:     true,
				Where:          "1=1",
				Enabled:        true,
			},
			dsn:    "user:pass@tcp(localhost:3306)/testdb",
			dryRun: false,
			expectedArgsContains: []string{
				"--source=h=localhost,P=3306,D=testdb,t=users_old",
				"--user=user",
				"--password=pass",
				"--where=1=1",
				"--limit=10000",
				"--commit-each",
				"--purge",
				"--progress=10000",
				"--max-lag=2.000000",
				"--no-check-charset",
				"--bulk-delete",
				"--primary-key-only",
				"--statistics",
			},
			expectedPassword: "pass",
		},
		{
			name:      "dry run mode",
			tableName: "orders_old",
			ptArchiverConfig: config.PtArchiverConfig{
				Limit:      10000,
				CommitEach: true,
				Where:      "1=1",
				Enabled:    true,
			},
			dsn:    "user:pass@tcp(localhost:3306)/testdb",
			dryRun: true,
			expectedArgsContains: []string{
				"--source=h=localhost,P=3306,D=testdb,t=orders_old",
				"--user=user",
				"--password=pass",
				"--where=1=1",
				"--limit=10000",
				"--commit-each",
				"--purge",
				"--dry-run",
			},
			expectedPassword: "pass",
		},
		{
			name:      "no password",
			tableName: "products_old",
			ptArchiverConfig: config.PtArchiverConfig{
				Where:   "1=1",
				Enabled: true,
			},
			dsn:    "user@tcp(localhost:3306)/testdb",
			dryRun: false,
			expectedArgsContains: []string{
				"--source=h=localhost,P=3306,D=testdb,t=products_old",
				"--user=user",
				"--where=1=1",
				"--purge",
			},
			expectedPassword: "",
		},
		{
			name:      "custom where clause",
			tableName: "logs_old",
			ptArchiverConfig: config.PtArchiverConfig{
				Where:   "created_at < NOW() - INTERVAL 30 DAY",
				Limit:   5000,
				Enabled: true,
			},
			dsn:    "user:pass@tcp(localhost:3306)/testdb",
			dryRun: false,
			expectedArgsContains: []string{
				"--source=h=localhost,P=3306,D=testdb,t=logs_old",
				"--user=user",
				"--password=pass",
				"--where=created_at < NOW() - INTERVAL 30 DAY",
				"--limit=5000",
				"--purge",
			},
			expectedPassword: "pass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, password, err := executor.BuildArgsWithPassword(tt.tableName, tt.ptArchiverConfig, tt.dsn, tt.dryRun)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedPassword, password)

			for _, expected := range tt.expectedArgsContains {
				assert.Contains(t, args, expected, "args should contain %s", expected)
			}
		})
	}
}

func TestParseDSN(t *testing.T) {
	logger := logrus.New()
	executor := NewPtArchiverExecutor(logger)

	tests := []struct {
		name             string
		dsn              string
		expectedHost     string
		expectedPort     string
		expectedDatabase string
		expectedUser     string
		expectedPassword string
		expectError      bool
	}{
		{
			name:             "full dsn with password",
			dsn:              "user:pass@tcp(localhost:3306)/testdb",
			expectedHost:     "localhost",
			expectedPort:     "3306",
			expectedDatabase: "testdb",
			expectedUser:     "user",
			expectedPassword: "pass",
			expectError:      false,
		},
		{
			name:             "dsn without password",
			dsn:              "root@tcp(127.0.0.1:3306)/mydb",
			expectedHost:     "127.0.0.1",
			expectedPort:     "3306",
			expectedDatabase: "mydb",
			expectedUser:     "root",
			expectedPassword: "",
			expectError:      false,
		},
		{
			name:             "dsn with query parameters",
			dsn:              "user:pass@tcp(localhost:3306)/testdb?parseTime=true",
			expectedHost:     "localhost",
			expectedPort:     "3306",
			expectedDatabase: "testdb",
			expectedUser:     "user",
			expectedPassword: "pass",
			expectError:      false,
		},
		{
			name:        "invalid dsn format",
			dsn:         "invalid_dsn",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, database, user, password, err := executor.ParseDSN(tt.dsn)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedHost, host)
				assert.Equal(t, tt.expectedPort, port)
				assert.Equal(t, tt.expectedDatabase, database)
				assert.Equal(t, tt.expectedUser, user)
				assert.Equal(t, tt.expectedPassword, password)
			}
		})
	}
}

func TestContainsErrorPattern(t *testing.T) {
	logger := logrus.New()
	executor := NewPtArchiverExecutor(logger)

	tests := []struct {
		name     string
		line     string
		expected bool
	}{
		{
			name:     "error prefix",
			line:     "ERROR: Connection failed",
			expected: true,
		},
		{
			name:     "fatal prefix",
			line:     "FATAL: Unable to connect to MySQL",
			expected: true,
		},
		{
			name:     "pt-archiver error",
			line:     "pt-archiver: error: table doesn't exist",
			expected: true,
		},
		{
			name:     "mysql error - unknown table",
			line:     "Unknown table 'testdb.users'",
			expected: true,
		},
		{
			name:     "mysql error - access denied",
			line:     "Access denied for user 'test'@'localhost'",
			expected: true,
		},
		{
			name:     "normal output",
			line:     "Archiving 10000 rows...",
			expected: false,
		},
		{
			name:     "progress output",
			line:     "Archived 50000 rows",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executor.containsErrorPattern(tt.line)
			assert.Equal(t, tt.expected, result)
		})
	}
}
