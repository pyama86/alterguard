package cmd

import (
	"fmt"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/pyama86/alterguard/internal/database"
	"github.com/pyama86/alterguard/internal/ptarchiver"
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

It also monitors for metadata locks and sends warnings if they exceed the configured threshold.
With --drop-triggers, pt-osc triggers are dropped after a successful swap.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return swapTable(args[0])
	},
}

var swapDropTriggers bool

func init() {
	swapCmd.Flags().BoolVar(&swapDropTriggers, "drop-triggers", false, "Drop pt-osc triggers after successful swap")
	rootCmd.AddCommand(swapCmd)
}

func swapTable(tableName string) error {
	logger.Infof("Starting table swap for %s", tableName)

	// Load configuration
	cfg, err := config.LoadConfigWithoutTasks(commonConfigPath, environment)
	if err != nil {
		logger.Errorf("Failed to load configuration: %v", err)
		return fmt.Errorf("configuration load failed: %w", err)
	}

	// Initialize database client
	dbClient, err := database.NewMySQLClient(cfg.DSN, logger)
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

	replicaLagFetcher, closeReplicaLagFetcher, err := newReplicaLagFetcher(cfg, dbClient)
	if err != nil {
		logger.Errorf("Failed to initialize replica lag fetcher: %v", err)
		return fmt.Errorf("replica lag fetcher initialization failed: %w", err)
	}
	defer closeReplicaLagFetcher()

	// Initialize pt-osc executor (not used for swap but required for manager)
	ptoscExecutor := ptosc.NewPtOscExecutor(logger, replicaLagFetcher)

	// Initialize pt-archiver executor (not used for swap but required for manager)
	ptarchiverExecutor := ptarchiver.NewPtArchiverExecutor(logger)

	// Initialize Slack notifier
	slackNotifier, err := slack.NewSlackNotifierWithEnvironment(logger, cfg.Environment)
	if err != nil {
		logger.Errorf("Failed to initialize Slack notifier: %v", err)
		return fmt.Errorf("slack notifier initialization failed: %w", err)
	}

	logger.Info("Slack notifier initialized")

	// Initialize task manager
	taskManager := task.NewManager(dbClient, ptoscExecutor, ptarchiverExecutor, slackNotifier, logger, cfg, dryRun)

	// Execute table swap
	logger.Infof("Starting table swap for %s", tableName)
	swap := taskManager.SwapTable
	if swapDropTriggers {
		swap = taskManager.SwapTableWithTriggerCleanup
	}
	if err := swap(tableName); err != nil {
		logger.Errorf("Swap operation failed: %v", err)
		return fmt.Errorf("swap operation failed: %w", err)
	}

	logger.Infof("Table swap completed successfully for %s", tableName)

	return nil
}
