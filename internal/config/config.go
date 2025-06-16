package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type CommonConfig struct {
	PtOsc          PtOscConfig   `yaml:"pt_osc"`
	Alert          AlertConfig   `yaml:"alert"`
	PtOscThreshold int64         `yaml:"pt_osc_threshold"`
	SessionConfig  SessionConfig `yaml:"session_config"`
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
	MetadataLockThresholdSeconds int `yaml:"metadata_lock_threshold_seconds"`
}

type SessionConfig struct {
	LockWaitTimeout       int `yaml:"lock_wait_timeout"`
	InnodbLockWaitTimeout int `yaml:"innodb_lock_wait_timeout"`
}

type Config struct {
	Common  CommonConfig
	Queries []string
	DSN     string
}

func LoadConfig(commonConfigPath, tasksConfigPath string) (*Config, error) {
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

	return &Config{
		Common:  *common,
		Queries: queries,
		DSN:     dsn,
	}, nil
}

func LoadConfigWithStdin(commonConfigPath, tasksConfigPath string, useStdin bool) (*Config, error) {
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

	return &Config{
		Common:  *common,
		Queries: queries,
		DSN:     dsn,
	}, nil
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

	return &config, nil
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
