package cmd

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	commonConfigPath string
	tasksConfigPath  string
	dryRun           bool
	logger           *logrus.Logger
	version          string
)

var rootCmd = &cobra.Command{
	Use:   "alterguard",
	Short: "MySQL schema change utility",
	Long: `alterguard is a MySQL schema change utility that automatically chooses between
ALTER TABLE and pt-online-schema-change based on table row count.

It supports:
- Automatic method selection based on row count thresholds
- Slack notifications for status updates
- Kubernetes job execution
- Dry run mode for testing`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		setupLogger()
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&commonConfigPath, "common-config", "", "Path to common configuration file (required)")
	rootCmd.PersistentFlags().StringVar(&tasksConfigPath, "tasks-config", "", "Path to tasks configuration file (required)")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Force pt-osc to run in dry-run mode")

	if err := rootCmd.MarkPersistentFlagRequired("common-config"); err != nil {
		logrus.Fatalf("Error marking common-config flag as required: %v", err)
	}
	if err := rootCmd.MarkPersistentFlagRequired("tasks-config"); err != nil {
		logrus.Fatalf("Error marking tasks-config flag as required: %v", err)
	}
}

func setupLogger() {
	logger = logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	if os.Getenv("DEBUG") == "true" {
		logger.SetLevel(logrus.DebugLevel)
	}
}
