package ptarchiver

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/sirupsen/logrus"
)

type Executor interface {
	ExecutePurge(tableName string, ptArchiverConfig config.PtArchiverConfig, dsn string, dryRun bool) error
}

type PtArchiverExecutor struct {
	logger        *logrus.Logger
	hasError      bool
	errorMessages []string
	outputSummary string
	mutex         sync.Mutex
}

func NewPtArchiverExecutor(logger *logrus.Logger) *PtArchiverExecutor {
	return &PtArchiverExecutor{
		logger: logger,
	}
}

func (e *PtArchiverExecutor) ExecutePurge(tableName string, ptArchiverConfig config.PtArchiverConfig, dsn string, dryRun bool) error {
	e.mutex.Lock()
	e.hasError = false
	e.errorMessages = []string{}
	e.outputSummary = ""
	e.mutex.Unlock()

	args, password, err := e.BuildArgsWithPassword(tableName, ptArchiverConfig, dsn, dryRun)
	if err != nil {
		return fmt.Errorf("failed to build pt-archiver arguments: %w", err)
	}

	maskedArgs := make([]string, len(args))
	copy(maskedArgs, args)
	for i, arg := range maskedArgs {
		if strings.HasPrefix(arg, "--password=") {
			maskedArgs[i] = "--password=[masked]"
		}
	}
	e.logger.Infof("Executing pt-archiver command: pt-archiver %s", strings.Join(maskedArgs, " "))

	cmd := exec.Command("pt-archiver", args...) // #nosec G204

	if password != "" {
		e.logger.Debugf("Using password for pt-archiver")
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

	if cmdErr != nil || e.hasError {
		var errorMsg string
		if cmdErr != nil && e.hasError {
			errorMsg = fmt.Sprintf("pt-archiver failed for table %s: %v (detected errors: %s)",
				tableName, cmdErr, strings.Join(e.errorMessages, "; "))
		} else if cmdErr != nil {
			errorMsg = fmt.Sprintf("pt-archiver failed for table %s: %v", tableName, cmdErr)
		} else {
			errorMsg = fmt.Sprintf("pt-archiver detected errors for table %s: %s",
				tableName, strings.Join(e.errorMessages, "; "))
		}
		return fmt.Errorf("%s", errorMsg)
	}

	e.logger.Infof("pt-archiver completed successfully for table %s", tableName)
	return nil
}

func (e *PtArchiverExecutor) logOutputWithSummary(r io.Reader, isError bool) {
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
			e.logger.Errorf("[pt-archiver] %s", line)
		} else {
			e.logger.Infof("[pt-archiver] %s", line)
		}
	}
}

func (e *PtArchiverExecutor) GetOutputSummary() string {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.outputSummary
}

func (e *PtArchiverExecutor) containsErrorPattern(line string) bool {
	line = strings.ToLower(strings.TrimSpace(line))

	errorPrefixes := []string{
		"error:",
		"fatal:",
		"pt-archiver: error",
		"pt-archiver: fatal",
	}

	for _, prefix := range errorPrefixes {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}

	mysqlErrors := []string{
		"you have an error in your sql syntax",
		"unknown column",
		"unknown table",
		"doesn't exist",
		"can't create table",
		"access denied",
		"connection refused",
		"lost connection",
		"cannot connect to mysql",
		"operation failed",
	}

	for _, errMsg := range mysqlErrors {
		if strings.Contains(line, errMsg) {
			return true
		}
	}

	return false
}

func (e *PtArchiverExecutor) BuildArgsWithPassword(
	tableName string,
	ptArchiverConfig config.PtArchiverConfig,
	rawDSN string,
	dryRun bool,
) ([]string, string, error) {
	host, port, database, user, password, err := e.ParseDSN(rawDSN)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse DSN: %w", err)
	}

	sourceSpec := fmt.Sprintf("h=%s,P=%s,D=%s,t=%s", host, port, database, tableName)

	args := []string{
		fmt.Sprintf("--source=%s", sourceSpec),
		fmt.Sprintf("--user=%s", user),
	}

	if password != "" {
		args = append(args, fmt.Sprintf("--password=%s", password))
	}

	if ptArchiverConfig.Where != "" {
		args = append(args, fmt.Sprintf("--where=%s", ptArchiverConfig.Where))
	} else {
		args = append(args, "--where=1=1")
	}

	if ptArchiverConfig.Limit > 0 {
		args = append(args, fmt.Sprintf("--limit=%d", ptArchiverConfig.Limit))
	}

	if ptArchiverConfig.CommitEach {
		args = append(args, "--commit-each")
	}

	args = append(args, "--purge")

	if ptArchiverConfig.Progress > 0 {
		args = append(args, fmt.Sprintf("--progress=%d", ptArchiverConfig.Progress))
	}

	if ptArchiverConfig.MaxLag > 0 {
		args = append(args, fmt.Sprintf("--max-lag=%f", ptArchiverConfig.MaxLag))
	}

	if ptArchiverConfig.NoCheckCharset {
		args = append(args, "--no-check-charset")
	}

	if ptArchiverConfig.BulkDelete {
		args = append(args, "--bulk-delete")
	}

	if ptArchiverConfig.PrimaryKeyOnly {
		args = append(args, "--primary-key-only")
	}

	if ptArchiverConfig.Statistics {
		args = append(args, "--statistics")
	}

	if dryRun {
		args = append(args, "--dry-run")
	}

	return args, password, nil
}

func (e *PtArchiverExecutor) ParseDSN(dsn string) (host, port, database, user, password string, err error) {
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

	if strings.Contains(database, "?") {
		database = strings.Split(database, "?")[0]
	}

	hostPortParts := strings.Split(hostPort, ":")
	if len(hostPortParts) != 2 {
		return "", "", "", "", "", fmt.Errorf("invalid host:port format")
	}

	host = hostPortParts[0]
	port = hostPortParts[1]

	return host, port, database, user, password, nil
}
