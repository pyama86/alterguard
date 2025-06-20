package ptosc

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/sirupsen/logrus"
)

type DryRunResult struct {
	EstimatedTime    string
	AffectedRows     int64
	ChunkCount       int
	ValidationResult string
	Warnings         []string
	Summary          string
}

type Executor interface {
	ExecuteAlter(tableName, alterStatement string, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool) error
	ExecuteAlterWithDryRunResult(tableName, alterStatement string, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool) (*DryRunResult, error)
}

type PtOscExecutor struct {
	logger        *logrus.Logger
	hasError      bool
	errorMessages []string
	outputLines   []string
	outputSummary string
	mutex         sync.Mutex
}

func NewPtOscExecutor(logger *logrus.Logger) *PtOscExecutor {
	return &PtOscExecutor{
		logger: logger,
	}
}

func (e *PtOscExecutor) ExecuteAlter(tableName, alterStatement string, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool) error {
	e.mutex.Lock()
	e.hasError = false
	e.errorMessages = []string{}
	e.outputLines = []string{}
	e.outputSummary = ""
	e.mutex.Unlock()

	args, password, err := e.BuildArgsWithPassword(tableName, alterStatement, ptOscConfig, dsn, forceDryRun)
	if err != nil {
		return fmt.Errorf("failed to build pt-osc arguments: %w", err)
	}

	// マスクされたコマンドをログ出力（パスワードを隠す）
	maskedArgs := make([]string, len(args))
	copy(maskedArgs, args)
	for i, arg := range maskedArgs {
		if arg == "--ask-pass" {
			maskedArgs[i] = "--ask-pass [password masked]"
		}
	}
	e.logger.Infof("Executing pt-online-schema-change command: pt-online-schema-change %s", strings.Join(maskedArgs, " "))

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

	go e.logOutputWithSummary(stdoutPipe, false)
	go e.logOutputWithSummary(stderrPipe, true)

	cmdErr := cmd.Wait()

	e.mutex.Lock()
	defer e.mutex.Unlock()

	// コマンドが異常終了した場合、またはエラーパターンが検出された場合はエラーとする
	if cmdErr != nil || e.hasError {
		var errorMsg string
		if cmdErr != nil && e.hasError {
			errorMsg = fmt.Sprintf("pt-online-schema-change failed for table %s: %v (detected errors: %s)",
				tableName, cmdErr, strings.Join(e.errorMessages, "; "))
		} else if cmdErr != nil {
			errorMsg = fmt.Sprintf("pt-online-schema-change failed for table %s: %v", tableName, cmdErr)
		} else {
			errorMsg = fmt.Sprintf("pt-online-schema-change detected errors for table %s: %s",
				tableName, strings.Join(e.errorMessages, "; "))
		}
		return fmt.Errorf("%s", errorMsg)
	}

	e.logger.Infof("pt-online-schema-change completed successfully for table %s", tableName)
	return nil
}

func (e *PtOscExecutor) logOutput(r io.Reader, isError bool) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()

		if e.containsErrorPattern(line) {
			e.mutex.Lock()
			e.hasError = true
			e.errorMessages = append(e.errorMessages, line)
			e.mutex.Unlock()
		}

		if isError {
			e.logger.Errorf("[pt-osc] %s", line)
		} else {
			e.logger.Infof("[pt-osc] %s", line)
		}
	}
}

func (e *PtOscExecutor) logOutputWithSummary(r io.Reader, isError bool) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()

		e.mutex.Lock()
		if e.outputSummary != "" {
			e.outputSummary += "\n"
		}
		if isError {
			e.outputSummary += "[STDERR] " + line
		} else {
			e.outputSummary += "[STDOUT] " + line
		}
		e.mutex.Unlock()

		if e.containsErrorPattern(line) {
			e.mutex.Lock()
			e.hasError = true
			e.errorMessages = append(e.errorMessages, line)
			e.mutex.Unlock()
		}

		if isError {
			e.logger.Errorf("[pt-osc] %s", line)
		} else {
			e.logger.Infof("[pt-osc] %s", line)
		}
	}
}

func (e *PtOscExecutor) GetOutputSummary() string {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.outputSummary
}

func (e *PtOscExecutor) containsErrorPattern(line string) bool {
	line = strings.ToLower(strings.TrimSpace(line))

	errorPrefixes := []string{
		"error:",
		"fatal:",
		"pt-online-schema-change: error",
		"pt-online-schema-change: fatal",
	}

	for _, prefix := range errorPrefixes {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}

	// https://perconadev.atlassian.net/issues/?jql=project%20%3D%20%22PT%22%20AND%20component%20%3D%20%22pt-online-schema-change%2
	// 上記が対応されるまでの暫定対応
	mysqlErrors := []string{
		"you have an error in your sql syntax",
		"unknown column",
		"unknown table",
		"duplicate column name",
		"duplicate key name",
		"doesn't exist",
		"can't create table",
		"can't drop table",
		"can't add foreign key constraint",
		"duplicate entry",
		"column cannot be null",
		"data too long for column",
		"out of range value",
		"access denied",
		"connection refused",
		"lost connection",
		"cannot connect to mysql",
		"cannot read response",
		"can't locate term/readkey",
		"operation failed",
	}

	for _, errMsg := range mysqlErrors {
		if strings.Contains(line, errMsg) {
			return true
		}
	}

	return false
}

func (e *PtOscExecutor) BuildArgsWithPassword(
	tableName, alterStatement string,
	ptOscConfig config.PtOscConfig,
	rawDSN string,
	forceDryRun bool,
) ([]string, string, error) {
	host, port, database, user, password, err := e.ParseDSN(rawDSN)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse DSN: %w", err)
	}

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

	if ptOscConfig.NoDropTriggers {
		args = append(args, "--no-drop-triggers")
	}

	if ptOscConfig.NoDropNewTable {
		args = append(args, "--no-drop-new-table")
	}

	if ptOscConfig.NoDropOldTable {
		args = append(args, "--no-drop-old-table")
	}

	if forceDryRun || ptOscConfig.DryRun {
		args = append(args, "--dry-run")
	} else {
		args = append(args, "--execute")
	}

	args = append(args, ptOscDSN)

	return args, password, nil
}

func (e *PtOscExecutor) ParseDSN(dsn string) (host, port, database, user, password string, err error) {
	parts := strings.Split(dsn, "@tcp")
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

	if !strings.HasPrefix(hostPart, "(") {
		return "", "", "", "", "", fmt.Errorf("only TCP connections are supported")
	}

	hostPart = strings.TrimPrefix(hostPart, "(")
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

func (e *PtOscExecutor) ExecuteAlterWithDryRunResult(tableName, alterStatement string, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool) (*DryRunResult, error) {
	if !forceDryRun && !ptOscConfig.DryRun {
		_, err := e.executeAlterInternal(tableName, alterStatement, ptOscConfig, dsn, forceDryRun, nil)
		return nil, err
	}

	result := &DryRunResult{
		Warnings: []string{},
	}

	_, err := e.executeAlterInternal(tableName, alterStatement, ptOscConfig, dsn, forceDryRun, result)
	return result, err
}

func (e *PtOscExecutor) executeAlterInternal(tableName, alterStatement string, ptOscConfig config.PtOscConfig, dsn string, forceDryRun bool, dryRunResult *DryRunResult) (bool, error) {
	e.mutex.Lock()
	e.hasError = false
	e.errorMessages = []string{}
	e.mutex.Unlock()

	args, password, err := e.BuildArgsWithPassword(tableName, alterStatement, ptOscConfig, dsn, forceDryRun)
	if err != nil {
		return false, fmt.Errorf("failed to build pt-osc arguments: %w", err)
	}

	// マスクされたコマンドをログ出力（パスワードを隠す）
	maskedArgs := make([]string, len(args))
	copy(maskedArgs, args)
	for i, arg := range maskedArgs {
		if arg == "--ask-pass" {
			maskedArgs[i] = "--ask-pass [password masked]"
		}
	}
	e.logger.Infof("Executing pt-online-schema-change command: pt-online-schema-change %s", strings.Join(maskedArgs, " "))

	cmd := exec.Command("pt-online-schema-change", args...) // #nosec G204

	if password != "" {
		e.logger.Debugf("Using password for pt-online-schema-change")
		cmd.Stdin = strings.NewReader(password + "\n")
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return false, fmt.Errorf("failed to get stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return false, fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return false, fmt.Errorf("failed to start command: %w", err)
	}

	if dryRunResult != nil {
		go e.logOutputWithDryRunAnalysis(stdoutPipe, false, dryRunResult)
		go e.logOutputWithDryRunAnalysis(stderrPipe, true, dryRunResult)
	} else {
		go e.logOutput(stdoutPipe, false)
		go e.logOutput(stderrPipe, true)
	}

	cmdErr := cmd.Wait()

	e.mutex.Lock()
	defer e.mutex.Unlock()

	// コマンドが異常終了した場合、またはエラーパターンが検出された場合はエラーとする
	if cmdErr != nil || e.hasError {
		var errorMsg string
		if cmdErr != nil && e.hasError {
			errorMsg = fmt.Sprintf("pt-online-schema-change failed for table %s: %v (detected errors: %s)",
				tableName, cmdErr, strings.Join(e.errorMessages, "; "))
		} else if cmdErr != nil {
			errorMsg = fmt.Sprintf("pt-online-schema-change failed for table %s: %v", tableName, cmdErr)
		} else {
			errorMsg = fmt.Sprintf("pt-online-schema-change detected errors for table %s: %s",
				tableName, strings.Join(e.errorMessages, "; "))
		}
		return false, fmt.Errorf("%s", errorMsg)
	}

	e.logger.Infof("pt-online-schema-change completed successfully for table %s", tableName)
	return true, nil
}

func (e *PtOscExecutor) logOutputWithDryRunAnalysis(r io.Reader, isError bool, result *DryRunResult) {
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()

		// 全ての出力をSummaryに追加
		if result.Summary != "" {
			result.Summary += "\n"
		}
		if isError {
			result.Summary += "[STDERR] " + line
		} else {
			result.Summary += "[STDOUT] " + line
		}

		if e.containsErrorPattern(line) {
			e.mutex.Lock()
			e.hasError = true
			e.errorMessages = append(e.errorMessages, line)
			e.mutex.Unlock()
		}

		// 簡単な検証結果の設定
		if strings.Contains(line, "Dry run complete") {
			result.ValidationResult = "Dry run completed successfully"
		} else if strings.Contains(line, "Starting a dry run") {
			result.ValidationResult = "Dry run started"
		}

		if isError {
			e.logger.Errorf("[pt-osc] %s", line)
		} else {
			e.logger.Infof("[pt-osc] %s", line)
		}
	}
}

// analyzeDryRunLine は使用しない（全ログをSummaryに含めるため）
