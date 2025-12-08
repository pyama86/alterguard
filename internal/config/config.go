package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type CommonConfig struct {
	PtOsc               PtOscConfig           `yaml:"pt_osc"`
	Alert               AlertConfig           `yaml:"alert"`
	PtOscThreshold      int64                 `yaml:"pt_osc_threshold"`
	SessionConfig       SessionConfig         `yaml:"session_config"`
	ConnectionCheck     ConnectionCheckConfig `yaml:"connection_check"`
	DisableAnalyzeTable bool                  `yaml:"disable_analyze_table"`
}

type PtOscConfig struct {
	Charset         string  `yaml:"charset"`
	RecursionMethod string  `yaml:"recursion_method"`
	NoSwapTables    bool    `yaml:"no_swap_tables"`
	ChunkSize       int     `yaml:"chunk_size"`
	MaxLag          float64 `yaml:"max_lag"`
	Statistics      bool    `yaml:"statistics"`
	DryRun          bool    `yaml:"dry_run"`
	NoDropTriggers  bool    `yaml:"no_drop_triggers"`
	NoDropNewTable  bool    `yaml:"no_drop_new_table"`
	NoDropOldTable  bool    `yaml:"no_drop_old_table"`
}

type AlertConfig struct {
	ExecutionTimeThresholdSeconds int `yaml:"execution_time_threshold_seconds"`
}

type SessionConfig struct {
	LockWaitTimeout       int `yaml:"lock_wait_timeout"`
	InnodbLockWaitTimeout int `yaml:"innodb_lock_wait_timeout"`
}

type ConnectionCheckConfig struct {
	Enabled bool `yaml:"enabled"`
}

type Config struct {
	Common      CommonConfig
	Queries     []string
	DSN         string
	Environment string
}

func LoadConfig(commonConfigPath, tasksConfigPath string) (*Config, error) {
	return LoadConfigWithEnvironment(commonConfigPath, tasksConfigPath, "")
}

func LoadConfigWithEnvironment(commonConfigPath, tasksConfigPath, environment string) (*Config, error) {
	common, err := loadCommonConfig(commonConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load common config: %w", err)
	}

	queries, err := loadQueriesConfig(tasksConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load queries config: %w", err)
	}

	dsn := os.Getenv("DATABASE_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_DSN environment variable is not set")
	}

	env := resolveEnvironment(environment)

	return &Config{
		Common:      *common,
		Queries:     queries,
		DSN:         dsn,
		Environment: env,
	}, nil
}

func LoadConfigWithoutTasks(commonConfigPath, environment string) (*Config, error) {
	common, err := loadCommonConfig(commonConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load common config: %w", err)
	}

	dsn := os.Getenv("DATABASE_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_DSN environment variable is not set")
	}

	env := resolveEnvironment(environment)

	return &Config{
		Common:      *common,
		Queries:     []string{},
		DSN:         dsn,
		Environment: env,
	}, nil
}

func LoadConfigWithStdin(commonConfigPath, tasksConfigPath string, useStdin bool) (*Config, error) {
	return LoadConfigWithStdinAndEnvironment(commonConfigPath, tasksConfigPath, useStdin, "")
}

func LoadConfigWithStdinAndEnvironment(commonConfigPath, tasksConfigPath string, useStdin bool, environment string) (*Config, error) {
	common, err := loadCommonConfig(commonConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load common config: %w", err)
	}

	var queries []string
	if tasksConfigPath != "" {
		fileQueries, err := loadQueriesConfig(tasksConfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load queries config: %w", err)
		}
		queries = append(queries, fileQueries...)
	}

	if useStdin {
		stdinQueries, err := loadQueriesFromStdin()
		if err != nil {
			return nil, fmt.Errorf("failed to load queries from stdin: %w", err)
		}
		queries = append(queries, stdinQueries...)
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("no queries provided")
	}

	dsn := os.Getenv("DATABASE_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_DSN environment variable is not set")
	}

	env := resolveEnvironment(environment)

	return &Config{
		Common:      *common,
		Queries:     queries,
		DSN:         dsn,
		Environment: env,
	}, nil
}

func resolveEnvironment(cmdLineEnv string) string {
	if cmdLineEnv != "" {
		return cmdLineEnv
	}

	if envVar := os.Getenv("ALTERGUARD_ENVIRONMENT"); envVar != "" {
		return envVar
	}

	return ""
}

func ResolveEnvironment(cmdLineEnv string) string {
	return resolveEnvironment(cmdLineEnv)
}

func loadCommonConfig(path string) (*CommonConfig, error) {
	data, err := os.ReadFile(path) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("failed to read file [%s]: %w", path, err)
	}

	var config CommonConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse YAML [%s]: %w", path, err)
	}

	// デフォルト値を設定（YAMLで明示的にfalseが設定されていない限りtrueにする）
	if !isConnectionCheckExplicitlyDisabled(data) {
		config.ConnectionCheck.Enabled = true
	}

	// 環境変数でpt_osc_thresholdをオーバーライド
	if envThreshold := os.Getenv("PT_OSC_THRESHOLD"); envThreshold != "" {
		if threshold, err := strconv.ParseInt(envThreshold, 10, 64); err == nil {
			config.PtOscThreshold = threshold
		}
	}

	return &config, nil
}

func isConnectionCheckExplicitlyDisabled(data []byte) bool {
	content := string(data)
	return strings.Contains(content, "connection_check:") &&
		(strings.Contains(content, "enabled: false") || strings.Contains(content, "enabled:false"))
}

func loadQueriesConfig(path string) ([]string, error) {
	data, err := os.ReadFile(path) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("failed to read file [%s]: %w", path, err)
	}

	var queries []string
	if err := yaml.Unmarshal(data, &queries); err != nil {
		return nil, fmt.Errorf("failed to parse YAML [%s]: %w", path, err)
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("no queries defined in [%s]", path)
	}

	for i, query := range queries {
		if strings.TrimSpace(query) == "" {
			return nil, fmt.Errorf("query is empty [index: %d]", i)
		}
	}

	return queries, nil
}

func loadQueriesFromStdin() ([]string, error) {
	var queries []string
	var currentQuery strings.Builder

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		currentQuery.WriteString(line)
		if strings.HasSuffix(line, ";") {
			query := strings.TrimSuffix(currentQuery.String(), ";")
			query = strings.TrimSpace(query)
			if query != "" {
				queries = append(queries, query)
			}
			currentQuery.Reset()
		} else {
			currentQuery.WriteString(" ")
		}
	}

	if currentQuery.Len() > 0 {
		query := strings.TrimSpace(currentQuery.String())
		if query != "" {
			queries = append(queries, query)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read from stdin: %w", err)
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("no queries provided from stdin")
	}

	return queries, nil
}
