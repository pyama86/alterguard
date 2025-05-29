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

var swapCmd = &cobra.Command{
	Use:   "swap [table_name]",
	Short: "Swap backup table with original table",
	Long: `Swap the backup table created by pt-online-schema-change with the original table.

This command performs a RENAME TABLE operation to swap:
- original_table -> original_table_old
- _original_table_new -> original_table

It also monitors for metadata locks and sends warnings if they exceed the configured threshold.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return swapTable(args[0])
	},
}

func init() {
	rootCmd.AddCommand(swapCmd)
}

func swapTable(tableName string) error {
	logger.Infof("Starting table swap for %s", tableName)

	// Load configuration
	cfg, err := config.LoadConfig(commonConfigPath, tasksConfigPath)
	if err != nil {
		logger.Errorf("Failed to load configuration: %v", err)
		return fmt.Errorf("configuration load failed: %w", err)
	}

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

	// Initialize pt-osc executor (not used for swap but required for manager)
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

	// Execute table swap
	logger.Infof("Starting table swap for %s", tableName)
	if err := taskManager.SwapTable(tableName); err != nil {
		logger.Errorf("Table swap failed: %v", err)
		return fmt.Errorf("table swap failed: %w", err)
	}

	logger.Infof("Table swap completed successfully for %s", tableName)
	return nil
}
