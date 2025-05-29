package ptosc

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/sirupsen/logrus"
)

type Executor interface {
	Execute(task config.Task, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool) error
}

type PtOscExecutor struct {
	logger *logrus.Logger
}

func NewPtOscExecutor(logger *logrus.Logger) *PtOscExecutor {
	return &PtOscExecutor{
		logger: logger,
	}
}

func (e *PtOscExecutor) Execute(task config.Task, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool) error {
	args, password, err := e.BuildArgsWithPassword(task, ptOscConfig, dsn, forceDryRun)
	if err != nil {
		return fmt.Errorf("failed to build pt-osc arguments: %w", err)
	}

	e.logger.Infof("Executing pt-online-schema-change for table %s", task.Table)
	e.logger.Debugf("Command: pt-online-schema-change %s", strings.Join(args, " "))

	cmd := exec.Command("pt-online-schema-change", args...) // #nosec G204

	// パスワードがある場合は標準入力経由で渡す
	if password != "" {
		cmd.Stdin = strings.NewReader(password + "\n")
	}

	output, err := cmd.CombinedOutput()

	if err != nil {
		e.logger.Errorf("pt-osc execution failed: %s", string(output))
		return fmt.Errorf("pt-online-schema-change failed for table %s: %w", task.Table, err)
	}

	e.logger.Infof("pt-online-schema-change completed successfully for table %s", task.Table)
	e.logger.Debugf("Output: %s", string(output))

	return nil
}

func (e *PtOscExecutor) BuildArgsWithPassword(
	task config.Task,
	ptOscConfig config.PtOscConfig,
	rawDSN string,
	forceDryRun bool,
) ([]string, string, error) {
	// ParseDSN はユーザー提供の Go ドライバ形式 DSN (user:pass@tcp(host:port)/db) 等を分解すると想定
	host, port, database, user, password, err := e.ParseDSN(rawDSN)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse DSN: %w", err)
	}

	// PT-OSC 用の DSN を構築 (パスワードは --ask-pass で対話入力)
	ptOscDSN := fmt.Sprintf(
		"h=%s,P=%s,D=%s,t=%s,u=%s",
		host, port, database, task.Table, user,
	)

	// 引数のビルド
	args := []string{
		fmt.Sprintf("--alter=%s", task.AlterStatement),
	}

	if ptOscConfig.Charset != "" {
		args = append(args, fmt.Sprintf("--charset=%s", ptOscConfig.Charset))
	}

	if ptOscConfig.RecursionMethod != "" {
		method := strings.ReplaceAll(ptOscConfig.RecursionMethod, "<db>", database)
		method = strings.ReplaceAll(method, "<table>", task.Table)
		args = append(args, fmt.Sprintf("--recursion-method=%s", method))
		if method == "dsn" {
			// スレーブにも同じ DSN を使う場合
			args = append(args, fmt.Sprintf("--recursion-dsn=%s", ptOscDSN))
		}
	}

	if password != "" {
		// パスワードがあるときは PT-OSC に書かずに 対話的に入力
		args = append(args, "--ask-pass")
	}

	if ptOscConfig.NoSwapTables {
		args = append(args, "--no-swap-tables")
	}
	if ptOscConfig.ChunkSize > 0 {
		args = append(args, fmt.Sprintf("--chunk-size=%d", ptOscConfig.ChunkSize))
	}
	if ptOscConfig.MaxLag > 0 {
		args = append(args, fmt.Sprintf("--max-lag=%f", ptOscConfig.MaxLag))
	}
	if ptOscConfig.Statistics {
		args = append(args, "--statistics")
	}

	// dry-run と execute は排他
	if forceDryRun || ptOscConfig.DryRun {
		args = append(args, "--dry-run")
	} else {
		args = append(args, "--execute")
	}

	// 最後に PT-OSC 用 DSN を１つだけ追加
	args = append(args, ptOscDSN)

	return args, password, nil
}

func (e *PtOscExecutor) ParseDSN(dsn string) (host, port, database, user, password string, err error) {
	parts := strings.Split(dsn, "@")
	if len(parts) != 2 {
		return "", "", "", "", "", fmt.Errorf("invalid DSN format")
	}

	userPart := parts[0]
	hostPart := parts[1]

	// Parse user:password
	userParts := strings.Split(userPart, ":")
	if len(userParts) == 1 {
		user = userParts[0]
		password = ""
	} else if len(userParts) == 2 {
		user = userParts[0]
		password = userParts[1]
	} else {
		return "", "", "", "", "", fmt.Errorf("invalid user:password format")
	}

	if !strings.HasPrefix(hostPart, "tcp(") {
		return "", "", "", "", "", fmt.Errorf("only TCP connections are supported")
	}

	hostPart = strings.TrimPrefix(hostPart, "tcp(")
	parts = strings.Split(hostPart, ")/")
	if len(parts) != 2 {
		return "", "", "", "", "", fmt.Errorf("invalid DSN format")
	}

	hostPort := parts[0]
	database = parts[1]

	hostPortParts := strings.Split(hostPort, ":")
	if len(hostPortParts) != 2 {
		return "", "", "", "", "", fmt.Errorf("invalid host:port format")
	}

	host = hostPortParts[0]
	port = hostPortParts[1]

	if _, err := strconv.Atoi(port); err != nil {
		return "", "", "", "", "", fmt.Errorf("invalid port number: %s", port)
	}

	return host, port, database, user, password, nil
}
