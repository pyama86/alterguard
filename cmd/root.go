package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// JSTFormatter は日本時間でログを出力するカスタムフォーマッター
type JSTFormatter struct {
	logrus.TextFormatter
}

// Format は日本時間でフォーマットされたログエントリを返す
func (f *JSTFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// 日本時間のタイムゾーンを取得
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		jst = time.FixedZone("JST", 9*60*60) // フォールバック
	}

	// エントリの時刻を日本時間に変換
	timestamp := entry.Time.In(jst).Format("2006/01/02 15:04:05 JST")

	// ログレベルを大文字で表示
	level := fmt.Sprintf("[%s]", entry.Level.String())

	// メッセージをフォーマット
	message := fmt.Sprintf("%s %s %s", timestamp, level, entry.Message)

	// フィールドがある場合は追加
	if len(entry.Data) > 0 {
		for key, value := range entry.Data {
			message += fmt.Sprintf(" %s=%v", key, value)
		}
	}

	return []byte(message + "\n"), nil
}

var (
	commonConfigPath string
	tasksConfigPath  string
	dryRun           bool
	environment      string
	logger           *logrus.Logger
	version          string
)

var rootCmd = &cobra.Command{
	Use:          "alterguard",
	SilenceUsage: true,
	Short:        "MySQL schema change utility",
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
	rootCmd.PersistentFlags().StringVar(&tasksConfigPath, "tasks-config", "", "Path to tasks configuration file (required unless --stdin is used)")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Force pt-osc to run in dry-run mode")
	rootCmd.PersistentFlags().StringVarP(&environment, "environment", "e", "", "Environment name (e.g., dev, qa, prod)")

	if err := rootCmd.MarkPersistentFlagRequired("common-config"); err != nil {
		logrus.Fatalf("Error marking common-config flag as required: %v", err)
	}
}

func setupLogger() {
	logger = logrus.New()
	logger.SetFormatter(&JSTFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	if os.Getenv("DEBUG") == "true" {
		logger.SetLevel(logrus.DebugLevel)
	}
}
