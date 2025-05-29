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

var (
	dropTable    bool
	dropTriggers bool
)

var cleanupCmd = &cobra.Command{
	Use:   "cleanup [table_name]",
	Short: "Clean up backup tables and triggers",
	Long: `Clean up resources created by pt-online-schema-change.

Available cleanup operations:
- --drop-table: Drop the backup table (table_name_old)
- --drop-triggers: Drop pt-osc triggers (pt_osc_table_name_*)

At least one cleanup operation must be specified.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if !dropTable && !dropTriggers {
			return fmt.Errorf("at least one cleanup operation must be specified (--drop-table or --drop-triggers)")
		}
		return cleanupTable(args[0])
	},
}

func init() {
	cleanupCmd.Flags().BoolVar(&dropTable, "drop-table", false, "Drop backup table")
	cleanupCmd.Flags().BoolVar(&dropTriggers, "drop-triggers", false, "Drop pt-osc triggers")
	rootCmd.AddCommand(cleanupCmd)
}

func cleanupTable(tableName string) error {
	logger.Infof("Starting cleanup for %s", tableName)

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

	// Initialize pt-osc executor (not used for cleanup but required for manager)
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

	// Execute cleanup operations
	if dropTable {
		logger.Infof("Dropping backup table for %s", tableName)
		if err := taskManager.CleanupTable(tableName); err != nil {
			logger.Errorf("Failed to drop backup table: %v", err)
			return fmt.Errorf("backup table cleanup failed: %w", err)
		}
		logger.Infof("Backup table cleanup completed for %s", tableName)
	}

	if dropTriggers {
		logger.Infof("Dropping triggers for %s", tableName)
		if err := taskManager.CleanupTriggers(tableName); err != nil {
			logger.Errorf("Failed to drop triggers: %v", err)
			return fmt.Errorf("trigger cleanup failed: %w", err)
		}
		logger.Infof("Trigger cleanup completed for %s", tableName)
	}

	logger.Infof("Cleanup completed successfully for %s", tableName)
	return nil
}
