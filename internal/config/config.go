package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type CommonConfig struct {
	PtOsc PtOscConfig `yaml:"pt_osc"`
	Alert AlertConfig `yaml:"alert"`
}

type PtOscConfig struct {
	Charset         string  `yaml:"charset"`
	RecursionMethod string  `yaml:"recursion_method"`
	NoSwapTables    bool    `yaml:"no_swap_tables"`
	ChunkSize       int     `yaml:"chunk_size"`
	MaxLag          float64 `yaml:"max_lag"`
	Statistics      bool    `yaml:"statistics"`
	DryRun          bool    `yaml:"dry_run"`
}

type AlertConfig struct {
	MetadataLockThresholdSeconds int `yaml:"metadata_lock_threshold_seconds"`
}

type Task struct {
	Name           string `yaml:"name"`
	Table          string `yaml:"table"`
	AlterStatement string `yaml:"alter_statement"`
	Threshold      int64  `yaml:"threshold"`
}

type Config struct {
	Common CommonConfig
	Tasks  []Task
	DSN    string
}

func LoadConfig(commonConfigPath, tasksConfigPath string) (*Config, error) {
	common, err := loadCommonConfig(commonConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load common config: %w", err)
	}

	tasks, err := loadTasksConfig(tasksConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load tasks config: %w", err)
	}

	dsn := os.Getenv("DATABASE_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_DSN environment variable is not set")
	}

	return &Config{
		Common: *common,
		Tasks:  tasks,
		DSN:    dsn,
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

func loadTasksConfig(path string) ([]Task, error) {
	data, err := os.ReadFile(path) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("failed to read file [%s]: %w", path, err)
	}

	var tasks []Task
	if err := yaml.Unmarshal(data, &tasks); err != nil {
		return nil, fmt.Errorf("failed to parse YAML [%s]: %w", path, err)
	}

	if len(tasks) == 0 {
		return nil, fmt.Errorf("no tasks defined in [%s]", path)
	}

	for i, task := range tasks {
		if task.Name == "" {
			return nil, fmt.Errorf("task name is empty [index: %d]", i)
		}
		if task.Table == "" {
			return nil, fmt.Errorf("table name is empty [task: %s]", task.Name)
		}
		if task.AlterStatement == "" {
			return nil, fmt.Errorf("alter statement is empty [task: %s]", task.Name)
		}
		if task.Threshold <= 0 {
			return nil, fmt.Errorf("invalid threshold [task: %s, threshold: %d]", task.Name, task.Threshold)
		}
	}

	return tasks, nil
}
