package ptosc

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"regexp"
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

	tableName := e.extractTableName(task)
	e.logger.Infof("Executing pt-online-schema-change for table %s", tableName)
	e.logger.Debugf("Command: pt-online-schema-change %s", strings.Join(args, " "))

	cmd := exec.Command("pt-online-schema-change", args...) // #nosec G204

	if password != "" {
		e.logger.Debugf("Using password for pt-online-schema-change")
		cmd.Stdin = strings.NewReader(password + "\n")
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w", err)
	}

	// 標準出力・エラー出力をリアルタイムで読みながらログに出す
	go e.logOutput(stdoutPipe, false)
	go e.logOutput(stderrPipe, true)

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("pt-online-schema-change failed for table %s: %w", tableName, err)
	}

	e.logger.Infof("pt-online-schema-change completed successfully for table %s", tableName)
	return nil
}

func (e *PtOscExecutor) logOutput(r io.Reader, isError bool) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if isError {
			e.logger.Errorf("[pt-osc] %s", line)
		} else {
			e.logger.Infof("[pt-osc] %s", line)
		}
	}
}
func (e *PtOscExecutor) BuildArgsWithPassword(
	task config.Task,
	ptOscConfig config.PtOscConfig,
	rawDSN string,
	forceDryRun bool,
) ([]string, string, error) {
	host, port, database, user, password, err := e.ParseDSN(rawDSN)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse DSN: %w", err)
	}

	tableName := e.extractTableName(task)
	alterStatement := e.extractAlterStatement(task)

	ptOscDSN := fmt.Sprintf(
		"h=%s,P=%s,D=%s,t=%s,u=%s",
		host, port, database, tableName, user,
	)

	args := []string{
		fmt.Sprintf("--alter=%s", alterStatement),
	}

	if ptOscConfig.Charset != "" {
		args = append(args, fmt.Sprintf("--charset=%s", ptOscConfig.Charset))
	}

	if ptOscConfig.RecursionMethod != "" {
		method := strings.ReplaceAll(ptOscConfig.RecursionMethod, "<db>", database)
		method = strings.ReplaceAll(method, "<table>", tableName)
		args = append(args, fmt.Sprintf("--recursion-method=%s", method))
		if method == "dsn" {
			args = append(args, fmt.Sprintf("--recursion-dsn=%s", ptOscDSN))
		}
	}

	if password != "" {
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

	if forceDryRun || ptOscConfig.DryRun {
		args = append(args, "--dry-run")
	} else {
		args = append(args, "--execute")
	}

	args = append(args, ptOscDSN)

	return args, password, nil
}

func (e *PtOscExecutor) extractTableName(task config.Task) string {
	for _, query := range task.Queries {
		query = strings.TrimSpace(query)
		alterTableRe := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + "`" + `?([^` + "`" + `\s]+)` + "`" + `?`)
		if matches := alterTableRe.FindStringSubmatch(query); len(matches) > 1 {
			return strings.Trim(matches[1], "`")
		}
	}
	return ""
}

func (e *PtOscExecutor) extractAlterStatement(task config.Task) string {
	for _, query := range task.Queries {
		query = strings.TrimSpace(query)
		alterTableRe := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + "`" + `?[^` + "`" + `\s]+` + "`" + `?\s+(.+)`)
		if matches := alterTableRe.FindStringSubmatch(query); len(matches) > 1 {
			return matches[1]
		}
	}
	return ""
}

func (e *PtOscExecutor) ParseDSN(dsn string) (host, port, database, user, password string, err error) {
	parts := strings.Split(dsn, "@")
	if len(parts) != 2 {
		return "", "", "", "", "", fmt.Errorf("invalid DSN format")
	}

	userPart := parts[0]
	hostPart := parts[1]

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
