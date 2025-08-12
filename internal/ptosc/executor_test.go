package ptosc

import (
	"testing"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildArgsWithPassword(t *testing.T) {
	logger := logrus.New()
	executor := NewPtOscExecutor(logger)

	tests := []struct {
		name             string
		tableName        string
		alterStatement   string
		ptOscConfig      config.PtOscConfig
		dsn              string
		forceDryRun      bool
		expectedArgs     []string
		expectedPassword string
	}{
		{
			name:           "basic configuration",
			tableName:      "users",
			alterStatement: "ADD COLUMN foo INT",
			ptOscConfig: config.PtOscConfig{
				Charset:         "utf8mb4",
				RecursionMethod: "dsn",
				NoSwapTables:    true,
				ChunkSize:       1000,
				MaxLag:          1.5,
				Statistics:      true,
				DryRun:          false,
			},
			dsn:         "user:pass@tcp(localhost:3306)/testdb",
			forceDryRun: false,
			expectedArgs: []string{
				"--alter=ADD COLUMN foo INT",
				"--charset=utf8mb4",
				"--recursion-method=dsn",
				"--recursion-dsn=h=localhost,P=3306,D=testdb,t=users,u=user",
				"--ask-pass",
				"--no-swap-tables",
				"--chunk-size=1000",
				"--max-lag=1.500000",
				"--statistics",
				"--execute",
				"h=localhost,P=3306,D=testdb,t=users,u=user",
			},
			expectedPassword: "pass",
		},
		{
			name:           "force dry run",
			tableName:      "orders",
			alterStatement: "DROP INDEX ix_old",
			ptOscConfig: config.PtOscConfig{
				DryRun: false,
			},
			dsn:         "user:pass@tcp(localhost:3306)/testdb",
			forceDryRun: true,
			expectedArgs: []string{
				"--alter=DROP INDEX ix_old",
				"--ask-pass",
				"--dry-run",
				"h=localhost,P=3306,D=testdb,t=orders,u=user",
			},
			expectedPassword: "pass",
		},
		{
			name:           "config dry run",
			tableName:      "products",
			alterStatement: "MODIFY COLUMN price DECIMAL(10,2)",
			ptOscConfig: config.PtOscConfig{
				DryRun: true,
			},
			dsn:         "user:pass@tcp(localhost:3306)/testdb",
			forceDryRun: false,
			expectedArgs: []string{
				"--alter=MODIFY COLUMN price DECIMAL(10,2)",
				"--ask-pass",
				"--dry-run",
				"h=localhost,P=3306,D=testdb,t=products,u=user",
			},
			expectedPassword: "pass",
		},
		{
			name:           "no password",
			tableName:      "users",
			alterStatement: "ADD COLUMN bar VARCHAR(255)",
			ptOscConfig:    config.PtOscConfig{},
			dsn:            "user@tcp(localhost:3306)/testdb",
			forceDryRun:    false,
			expectedArgs: []string{
				"--alter=ADD COLUMN bar VARCHAR(255)",
				"--execute",
				"h=localhost,P=3306,D=testdb,t=users,u=user",
			},
			expectedPassword: "",
		},
		{
			name:           "no-drop options with execute",
			tableName:      "users",
			alterStatement: "ADD COLUMN baz INT",
			ptOscConfig: config.PtOscConfig{
				NoDropTriggers: true,
				NoDropNewTable: true,
				NoDropOldTable: true,
				DryRun:         false,
			},
			dsn:         "user:pass@tcp(localhost:3306)/testdb",
			forceDryRun: false,
			expectedArgs: []string{
				"--alter=ADD COLUMN baz INT",
				"--ask-pass",
				"--no-drop-triggers",
				"--no-drop-new-table",
				"--no-drop-old-table",
				"--execute",
				"h=localhost,P=3306,D=testdb,t=users,u=user",
			},
			expectedPassword: "pass",
		},
		{
			name:           "no-drop options with force dry run excludes no-drop-triggers and no-drop-new-table",
			tableName:      "users",
			alterStatement: "ADD COLUMN qux INT",
			ptOscConfig: config.PtOscConfig{
				NoDropTriggers: true,
				NoDropNewTable: true,
				NoDropOldTable: true,
				DryRun:         false,
			},
			dsn:         "user:pass@tcp(localhost:3306)/testdb",
			forceDryRun: true,
			expectedArgs: []string{
				"--alter=ADD COLUMN qux INT",
				"--ask-pass",
				"--no-drop-old-table",
				"--dry-run",
				"h=localhost,P=3306,D=testdb,t=users,u=user",
			},
			expectedPassword: "pass",
		},
		{
			name:           "no-drop options with config dry run excludes no-drop-triggers and no-drop-new-table",
			tableName:      "users",
			alterStatement: "ADD COLUMN quux INT",
			ptOscConfig: config.PtOscConfig{
				NoDropTriggers: true,
				NoDropNewTable: true,
				NoDropOldTable: true,
				DryRun:         true,
			},
			dsn:         "user:pass@tcp(localhost:3306)/testdb",
			forceDryRun: false,
			expectedArgs: []string{
				"--alter=ADD COLUMN quux INT",
				"--ask-pass",
				"--no-drop-old-table",
				"--dry-run",
				"h=localhost,P=3306,D=testdb,t=users,u=user",
			},
			expectedPassword: "pass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, password, err := executor.BuildArgsWithPassword(tt.tableName, tt.alterStatement, tt.ptOscConfig, tt.dsn, tt.forceDryRun)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedArgs, args)
			assert.Equal(t, tt.expectedPassword, password)
		})
	}
}

func TestContainsErrorPattern(t *testing.T) {
	logger := logrus.New()
	executor := NewPtOscExecutor(logger)

	tests := []struct {
		name     string
		line     string
		expected bool
	}{
		{
			name:     "SQL syntax error",
			line:     "ERROR 1064 (42000): You have an error in your SQL syntax",
			expected: true,
		},
		{
			name:     "Unknown column error",
			line:     "ERROR 1054 (42S22): Unknown column 'invalid_col' in 'field list'",
			expected: true,
		},
		{
			name:     "Unknown table error",
			line:     "ERROR 1146 (42S02): Table 'testdb.nonexistent' doesn't exist",
			expected: true,
		},
		{
			name:     "Duplicate column name",
			line:     "ERROR 1060 (42S21): Duplicate column name 'id'",
			expected: true,
		},
		{
			name:     "pt-osc error prefix",
			line:     "pt-online-schema-change: error: Connection failed",
			expected: true,
		},
		{
			name:     "pt-osc fatal prefix",
			line:     "pt-online-schema-change: fatal: Cannot connect to MySQL",
			expected: true,
		},
		{
			name:     "Generic error prefix",
			line:     "ERROR: Operation failed",
			expected: true,
		},
		{
			name:     "Fatal prefix",
			line:     "FATAL: Database connection lost",
			expected: true,
		},
		{
			name:     "Can't create table",
			line:     "Can't create table 'testdb.users' (errno: 150)",
			expected: true,
		},
		{
			name:     "Access denied",
			line:     "ERROR 1045 (28000): Access denied for user 'test'@'localhost'",
			expected: true,
		},
		{
			name:     "Normal log message",
			line:     "Copying approximately 1000 rows...",
			expected: false,
		},
		{
			name:     "Progress message",
			line:     "Copied 500 rows",
			expected: false,
		},
		{
			name:     "Success message",
			line:     "Successfully altered table",
			expected: false,
		},
		{
			name:     "Column name containing error",
			line:     "Processing column error_log",
			expected: false,
		},
		{
			name:     "Table name containing error",
			line:     "Altering table user_errors",
			expected: false,
		},
		{
			name:     "Empty line",
			line:     "",
			expected: false,
		},
		{
			name:     "Case insensitive SQL syntax error",
			line:     "Error 1064: you have an error in your sql syntax",
			expected: true,
		},
		{
			name:     "Cannot read response error",
			line:     "Cannot read response; is Term::ReadKey installed?",
			expected: true,
		},
		{
			name:     "Enter MySQL password prompt",
			line:     "Enter MySQL password: ",
			expected: false,
		},
		{
			name:     "Can't locate Term/ReadKey error",
			line:     "Can't locate Term/ReadKey.pm in @INC",
			expected: true,
		},
		{
			name:     "Case insensitive cannot read response",
			line:     "CANNOT READ RESPONSE; IS TERM::READKEY INSTALLED?",
			expected: true,
		},
		{
			name:     "Case insensitive enter mysql password",
			line:     "ENTER MYSQL PASSWORD:",
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

func TestParseDSN(t *testing.T) {
	logger := logrus.New()
	executor := NewPtOscExecutor(logger)

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
			name:             "valid DSN with password",
			dsn:              "user:pass@tcp(localhost:3306)/testdb",
			expectedHost:     "localhost",
			expectedPort:     "3306",
			expectedDatabase: "testdb",
			expectedUser:     "user",
			expectedPassword: "pass",
			expectError:      false,
		},
		{
			name:             "valid DSN without password",
			dsn:              "user@tcp(localhost:3306)/testdb",
			expectedHost:     "localhost",
			expectedPort:     "3306",
			expectedDatabase: "testdb",
			expectedUser:     "user",
			expectedPassword: "",
			expectError:      false,
		},
		{
			name:             "valid DSN with IP",
			dsn:              "user:pass@tcp(192.168.1.100:3306)/mydb",
			expectedHost:     "192.168.1.100",
			expectedPort:     "3306",
			expectedDatabase: "mydb",
			expectedUser:     "user",
			expectedPassword: "pass",
			expectError:      false,
		},
		{
			name:        "invalid DSN - no @",
			dsn:         "user:pass:tcp(localhost:3306)/testdb",
			expectError: true,
		},
		{
			name:        "invalid DSN - not TCP",
			dsn:         "user:pass@unix(/tmp/mysql.sock)/testdb",
			expectError: true,
		},
		{
			name:        "invalid DSN - malformed host:port",
			dsn:         "user:pass@tcp(localhost)/testdb",
			expectError: true,
		},
		{
			name:        "invalid DSN - invalid port",
			dsn:         "user:pass@tcp(localhost:abc)/testdb",
			expectError: true,
		},
		{
			name:        "invalid DSN - no database",
			dsn:         "user:pass@tcp(localhost:3306)",
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
