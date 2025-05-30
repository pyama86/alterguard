package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name          string
		commonConfig  string
		tasksConfig   string
		dsnEnv        string
		expectError   bool
		expectedTasks int
		expectedDSN   string
	}{
		{
			name: "valid config",
			commonConfig: `
pt_osc:
  charset: utf8mb4
  recursion_method: "dsn=D=test,t=dsns"
  no_swap_tables: true
  chunk_size: 1000
  max_lag: 1.5
  statistics: true
  dry_run: false
pt_osc_threshold: 1000000
alert:
  metadata_lock_threshold_seconds: 30
`,
			tasksConfig: `
- name: add_column
  queries:
    - "ALTER TABLE users ADD COLUMN foo INT"
  threshold: 1000000
- name: drop_index
  queries:
    - "ALTER TABLE orders DROP INDEX ix_old"
`,
			dsnEnv:        "user:pass@tcp(localhost:3306)/test",
			expectError:   false,
			expectedTasks: 2,
			expectedDSN:   "user:pass@tcp(localhost:3306)/test",
		},
		{
			name:         "missing DSN environment variable",
			commonConfig: "pt_osc:\n  charset: utf8mb4\npt_osc_threshold: 1000000\nalert:\n  metadata_lock_threshold_seconds: 30",
			tasksConfig:  "- name: test\n  queries:\n    - \"ALTER TABLE users ADD COLUMN foo INT\"",
			dsnEnv:       "",
			expectError:  true,
		},
		{
			name:         "empty tasks",
			commonConfig: "pt_osc:\n  charset: utf8mb4\npt_osc_threshold: 1000000\nalert:\n  metadata_lock_threshold_seconds: 30",
			tasksConfig:  "",
			dsnEnv:       "user:pass@tcp(localhost:3306)/test",
			expectError:  true,
		},
		{
			name:         "invalid task - missing name",
			commonConfig: "pt_osc:\n  charset: utf8mb4\npt_osc_threshold: 1000000\nalert:\n  metadata_lock_threshold_seconds: 30",
			tasksConfig:  "- queries:\n    - \"ALTER TABLE users ADD COLUMN foo INT\"",
			dsnEnv:       "user:pass@tcp(localhost:3306)/test",
			expectError:  true,
		},
		{
			name:         "invalid task - empty queries",
			commonConfig: "pt_osc:\n  charset: utf8mb4\npt_osc_threshold: 1000000\nalert:\n  metadata_lock_threshold_seconds: 30",
			tasksConfig:  "- name: test\n  queries: []",
			dsnEnv:       "user:pass@tcp(localhost:3306)/test",
			expectError:  true,
		},
		{
			name:         "invalid task - negative threshold",
			commonConfig: "pt_osc:\n  charset: utf8mb4\npt_osc_threshold: 1000000\nalert:\n  metadata_lock_threshold_seconds: 30",
			tasksConfig:  "- name: test\n  queries:\n    - \"ALTER TABLE users ADD COLUMN foo INT\"\n  threshold: -1",
			dsnEnv:       "user:pass@tcp(localhost:3306)/test",
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			commonPath := filepath.Join(tmpDir, "common.yaml")
			tasksPath := filepath.Join(tmpDir, "tasks.yaml")

			require.NoError(t, os.WriteFile(commonPath, []byte(tt.commonConfig), 0644))
			require.NoError(t, os.WriteFile(tasksPath, []byte(tt.tasksConfig), 0644))

			if tt.dsnEnv != "" {
				t.Setenv("DATABASE_DSN", tt.dsnEnv)
			} else {
				os.Unsetenv("DATABASE_DSN")
			}

			config, err := LoadConfig(commonPath, tasksPath)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				require.NoError(t, err)
				require.NotNil(t, config)
				assert.Equal(t, tt.expectedTasks, len(config.Tasks))
				assert.Equal(t, tt.expectedDSN, config.DSN)
				assert.Equal(t, "utf8mb4", config.Common.PtOsc.Charset)
				assert.Equal(t, int64(1000000), config.Common.PtOscThreshold)
				assert.Equal(t, 30, config.Common.Alert.MetadataLockThresholdSeconds)
			}
		})
	}
}

func TestLoadCommonConfig(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		expectError bool
	}{
		{
			name: "valid config",
			content: `
pt_osc:
  charset: utf8mb4
  recursion_method: "dsn=D=test,t=dsns"
  no_swap_tables: true
  chunk_size: 1000
  max_lag: 1.5
  statistics: true
  dry_run: false
pt_osc_threshold: 1000000
alert:
  metadata_lock_threshold_seconds: 30
`,
			expectError: false,
		},
		{
			name:        "invalid YAML",
			content:     "invalid: yaml: content:",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0644))

			config, err := loadCommonConfig(path)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, config)
			} else {
				require.NoError(t, err)
				require.NotNil(t, config)
			}
		})
	}
}

func TestLoadTasksConfig(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		expectError bool
		expectTasks int
	}{
		{
			name: "valid tasks",
			content: `
- name: add_column
  queries:
    - "ALTER TABLE users ADD COLUMN foo INT"
  threshold: 1000000
- name: drop_index
  queries:
    - "ALTER TABLE orders DROP INDEX ix_old"
`,
			expectError: false,
			expectTasks: 2,
		},
		{
			name:        "empty tasks",
			content:     "",
			expectError: true,
		},
		{
			name:        "invalid YAML",
			content:     "- invalid: yaml: content:",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "tasks.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0644))

			tasks, err := loadTasksConfig(path)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, tasks)
			} else {
				require.NoError(t, err)
				assert.Len(t, tasks, tt.expectTasks)
			}
		})
	}
}
