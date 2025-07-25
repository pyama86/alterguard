package config

import (
	"testing"
)

func TestLoadConfigWithoutTasks(t *testing.T) {
	tests := []struct {
		name        string
		commonPath  string
		environment string
		setupEnv    func()
		cleanupEnv  func()
		wantErr     bool
	}{
		{
			name:        "valid config without tasks",
			commonPath:  "../../examples/config-common.yaml",
			environment: "test",
			setupEnv: func() {
				t.Setenv("DATABASE_DSN", "user:pass@tcp(localhost:3306)/test")
			},
			wantErr: false,
		},
		{
			name:        "missing DSN environment variable",
			commonPath:  "../../examples/config-common.yaml",
			environment: "test",
			setupEnv: func() {
				// Explicitly unset DATABASE_DSN
				t.Setenv("DATABASE_DSN", "")
			},
			wantErr: true,
		},
		{
			name:        "invalid common config path",
			commonPath:  "nonexistent.yaml",
			environment: "test",
			setupEnv: func() {
				t.Setenv("DATABASE_DSN", "user:pass@tcp(localhost:3306)/test")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupEnv()
			if tt.cleanupEnv != nil {
				defer tt.cleanupEnv()
			}

			cfg, err := LoadConfigWithoutTasks(tt.commonPath, tt.environment)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfigWithoutTasks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if cfg == nil {
					t.Error("LoadConfigWithoutTasks() returned nil config")
					return
				}
				if len(cfg.Queries) != 0 {
					t.Errorf("LoadConfigWithoutTasks() queries should be empty, got %d queries", len(cfg.Queries))
				}
				if cfg.Environment != tt.environment {
					t.Errorf("LoadConfigWithoutTasks() environment = %v, want %v", cfg.Environment, tt.environment)
				}
			}
		})
	}
}

func TestPtOscThresholdEnvironmentVariable(t *testing.T) {
	tests := []struct {
		name          string
		commonPath    string
		envValue      string
		expectedValue int64
		setupEnv      func(string)
		cleanupEnv    func()
		wantErr       bool
	}{
		{
			name:          "PT_OSC_THRESHOLD environment variable override",
			commonPath:    "../../examples/config-common.yaml",
			envValue:      "5000",
			expectedValue: 5000,
			setupEnv: func(value string) {
				t.Setenv("DATABASE_DSN", "user:pass@tcp(localhost:3306)/test")
				t.Setenv("PT_OSC_THRESHOLD", value)
			},
			wantErr: false,
		},
		{
			name:       "invalid PT_OSC_THRESHOLD value",
			commonPath: "../../examples/config-common.yaml",
			envValue:   "invalid",
			setupEnv: func(value string) {
				t.Setenv("DATABASE_DSN", "user:pass@tcp(localhost:3306)/test")
				t.Setenv("PT_OSC_THRESHOLD", value)
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupEnv(tt.envValue)
			if tt.cleanupEnv != nil {
				defer tt.cleanupEnv()
			}

			cfg, err := LoadConfigWithoutTasks(tt.commonPath, "test")
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfigWithoutTasks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.expectedValue > 0 {
				if cfg.Common.PtOscThreshold != tt.expectedValue {
					t.Errorf("PtOscThreshold = %v, want %v", cfg.Common.PtOscThreshold, tt.expectedValue)
				}
			}
		})
	}
}
