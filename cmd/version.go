package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of alterguard",
	Long:  `All software has versions. This is alterguard's`,
	Run: func(cmd *cobra.Command, args []string) {
		if version == "" {
			fmt.Println("alterguard version: development")
		} else {
			fmt.Printf("alterguard version: %s\n", version)
		}
	},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// versionコマンドではsetupLoggerを呼ばない
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
	// versionコマンドでは必須フラグを無効にする
	versionCmd.PersistentFlags().StringVar(&commonConfigPath, "common-config", "", "Path to common configuration file")
	versionCmd.PersistentFlags().StringVar(&tasksConfigPath, "tasks-config", "", "Path to tasks configuration file")
	versionCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Force pt-osc to run in dry-run mode")
}
