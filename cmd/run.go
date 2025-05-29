package cmd

import (
	"fmt"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/pyama86/alterguard/internal/database"
	"github.com/pyama86/alterguard/internal/ptosc"
	"github.com/pyama86/alterguard/internal/slack"
	"github.com/pyama86/alterguard/internal/task"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Execute all schema change tasks",
	Long: `Execute all schema change tasks defined in the tasks configuration file.

Tasks with row count <= threshold will be executed using ALTER TABLE.
Tasks with row count > threshold will be executed using pt-online-schema-change.

If multiple tasks exceed the threshold, the command will fail with an error.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runTasks()
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}

func runTasks() error {
	logger.Info("Starting alterguard run command")

	// Load configuration
	cfg, err := config.LoadConfig(commonConfigPath, tasksConfigPath)
	if err != nil {
		logger.Errorf("Failed to load configuration: %v", err)
		return fmt.Errorf("configuration load failed: %w", err)
	}

	logger.Infof("Loaded configuration with %d tasks", len(cfg.Tasks))

	// Initialize database client
	dbClient, err := database.NewMySQLClient(cfg.DSN)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return fmt.Errorf("database connection failed: %w", err)
	}
	defer func() {
		if closeErr := dbClient.Close(); closeErr != nil {
			logger.Errorf("Failed to close database connection: %v", closeErr)
		}
	}()

	logger.Info("Database connection established")

	// Initialize pt-osc executor
	ptoscExecutor := ptosc.NewPtOscExecutor(logger)

	// Initialize Slack notifier
	slackNotifier, err := slack.NewSlackNotifier(logger)
	if err != nil {
		logger.Errorf("Failed to initialize Slack notifier: %v", err)
		return fmt.Errorf("Slack notifier initialization failed: %w", err)
	}

	logger.Info("Slack notifier initialized")

	// Initialize task manager
	taskManager := task.NewManager(dbClient, ptoscExecutor, slackNotifier, logger, cfg, dryRun)

	// Execute all tasks
	logger.Info("Starting task execution")
	if err := taskManager.ExecuteAllTasks(); err != nil {
		logger.Errorf("Task execution failed: %v", err)
		return fmt.Errorf("task execution failed: %w", err)
	}

	logger.Info("All tasks completed successfully")
	return nil
}
