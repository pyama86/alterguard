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
