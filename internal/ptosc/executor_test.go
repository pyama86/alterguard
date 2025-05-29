package ptosc

import (
	"testing"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildArgs(t *testing.T) {
	logger := logrus.New()
	executor := NewPtOscExecutor(logger)

	tests := []struct {
		name         string
		task         config.Task
		ptOscConfig  config.PtOscConfig
		dsn          string
		forceDryRun  bool
		expectedArgs []string
	}{
		{
			name: "basic configuration",
			task: config.Task{
				Name:           "add_column",
				Table:          "users",
				AlterStatement: "ADD COLUMN foo INT",
				Threshold:      1000000,
			},
			ptOscConfig: config.PtOscConfig{
				Charset:         "utf8mb4",
				RecursionMethod: "dsn=D=<db>,t=dsns",
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
				"h=localhost,P=3306,D=testdb,t=users",
				"--charset=utf8mb4",
				"--recursion-method=dsn=D=testdb,t=dsns",
				"--dsn=user@tcp(localhost:3306)/testdb",
				"--ask-pass",
				"--no-swap-tables",
				"--chunk-size=1000",
				"--max-lag=1.500000",
				"--statistics",
				"--execute",
			},
		},
		{
			name: "force dry run",
			task: config.Task{
				Name:           "drop_index",
				Table:          "orders",
				AlterStatement: "DROP INDEX ix_old",
				Threshold:      500000,
			},
			ptOscConfig: config.PtOscConfig{
				DryRun: false,
			},
			dsn:         "user:pass@tcp(localhost:3306)/testdb",
			forceDryRun: true,
			expectedArgs: []string{
				"--alter=DROP INDEX ix_old",
				"h=localhost,P=3306,D=testdb,t=orders",
				"--dsn=user@tcp(localhost:3306)/testdb",
				"--ask-pass",
				"--dry-run",
				"--execute",
			},
		},
		{
			name: "config dry run",
			task: config.Task{
				Name:           "modify_column",
				Table:          "products",
				AlterStatement: "MODIFY COLUMN price DECIMAL(10,2)",
				Threshold:      100000,
			},
			ptOscConfig: config.PtOscConfig{
				DryRun: true,
			},
			dsn:         "user:pass@tcp(localhost:3306)/testdb",
			forceDryRun: false,
			expectedArgs: []string{
				"--alter=MODIFY COLUMN price DECIMAL(10,2)",
				"h=localhost,P=3306,D=testdb,t=products",
				"--dsn=user@tcp(localhost:3306)/testdb",
				"--ask-pass",
				"--dry-run",
				"--execute",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, _, err := executor.BuildArgsWithPassword(tt.task, tt.ptOscConfig, tt.dsn, tt.forceDryRun)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedArgs, args)
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
		expectError      bool
	}{
		{
			name:             "valid DSN",
			dsn:              "user:pass@tcp(localhost:3306)/testdb",
			expectedHost:     "localhost",
			expectedPort:     "3306",
			expectedDatabase: "testdb",
			expectError:      false,
		},
		{
			name:             "valid DSN with IP",
			dsn:              "user:pass@tcp(192.168.1.100:3306)/mydb",
			expectedHost:     "192.168.1.100",
			expectedPort:     "3306",
			expectedDatabase: "mydb",
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
			host, port, database, _, _, err := executor.ParseDSN(tt.dsn)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedHost, host)
				assert.Equal(t, tt.expectedPort, port)
				assert.Equal(t, tt.expectedDatabase, database)
			}
		})
	}
}
