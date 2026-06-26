package ptosc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/sirupsen/logrus"
)

const (
	defaultAuroraCheckInterval = 5 * time.Second
	defaultAuroraPauseFilePath = "/tmp/alterguard-ptosc-pause"
)

type ReplicaLagFetcher interface {
	GetMaxAuroraReplicaLagMs() (float64, error)
}

type AuroraMonitor struct {
	cfg           config.AuroraReplicaCheckConfig
	fetcher       ReplicaLagFetcher
	logger        *logrus.Logger
	checkInterval time.Duration
	pauseFilePath string

	mu             sync.Mutex
	pauseFileOwned bool
}

func NewAuroraMonitor(cfg config.AuroraReplicaCheckConfig, fetcher ReplicaLagFetcher, logger *logrus.Logger) (*AuroraMonitor, error) {
	interval, err := resolveCheckInterval(cfg.CheckInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid aurora_replica_check.check_interval: %w", err)
	}

	pauseFilePath := cfg.PauseFilePath
	if pauseFilePath == "" {
		pauseFilePath = defaultAuroraPauseFilePath
	}

	return &AuroraMonitor{
		cfg:           cfg,
		fetcher:       fetcher,
		logger:        logger,
		checkInterval: interval,
		pauseFilePath: pauseFilePath,
	}, nil
}

func (m *AuroraMonitor) PauseFilePath() string {
	return m.pauseFilePath
}

// pause-fileの作成/削除と REPLICA_HOST_STATUS の読取が可能か事前確認する。失敗時は実行を止めるべきというシグナル。
func (m *AuroraMonitor) Preflight() error {
	tmp, err := os.Create(m.pauseFilePath) // #nosec G304
	if err != nil {
		return fmt.Errorf("cannot create pause file %s: %w", m.pauseFilePath, err)
	}
	if cerr := tmp.Close(); cerr != nil {
		_ = os.Remove(m.pauseFilePath)
		return fmt.Errorf("cannot close pause file %s: %w", m.pauseFilePath, cerr)
	}
	if rerr := os.Remove(m.pauseFilePath); rerr != nil {
		return fmt.Errorf("cannot remove pause file %s: %w", m.pauseFilePath, rerr)
	}

	if _, err := m.fetcher.GetMaxAuroraReplicaLagMs(); err != nil {
		return fmt.Errorf("cannot read information_schema.REPLICA_HOST_STATUS: %w", err)
	}
	return nil
}

// 監視ループを開始。ctxがキャンセルされるかStopが呼ばれるまで動く。
func (m *AuroraMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	m.checkOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			m.cleanupPauseFile()
			return
		case <-ticker.C:
			m.checkOnce(ctx)
		}
	}
}

func (m *AuroraMonitor) checkOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	lagMs, err := m.fetcher.GetMaxAuroraReplicaLagMs()
	if err != nil {
		m.logger.Warnf("Aurora replica lag check failed: %v", err)
		return
	}

	if lagMs > m.cfg.MaxLagMs {
		m.logger.Warnf("Aurora replica lag %.2fms exceeds threshold %.2fms; creating pause file %s",
			lagMs, m.cfg.MaxLagMs, m.pauseFilePath)
		if err := m.createPauseFile(); err != nil {
			m.logger.Errorf("Failed to create pause file %s: %v", m.pauseFilePath, err)
		}
		return
	}

	m.logger.Debugf("Aurora replica lag %.2fms within threshold %.2fms", lagMs, m.cfg.MaxLagMs)
	if err := m.removePauseFileIfOwned(); err != nil {
		m.logger.Warnf("Failed to remove pause file %s: %v", m.pauseFilePath, err)
	}
}

func (m *AuroraMonitor) createPauseFile() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := os.OpenFile(m.pauseFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644) // #nosec G304
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "paused at %s due to aurora replica lag\n", time.Now().Format(time.RFC3339)); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	m.pauseFileOwned = true
	return nil
}

func (m *AuroraMonitor) removePauseFileIfOwned() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.pauseFileOwned {
		return nil
	}
	if err := os.Remove(m.pauseFilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	m.pauseFileOwned = false
	m.logger.Infof("Aurora replica lag recovered; removed pause file %s", m.pauseFilePath)
	return nil
}

func (m *AuroraMonitor) cleanupPauseFile() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.pauseFileOwned {
		return
	}
	if err := os.Remove(m.pauseFilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		m.logger.Warnf("Failed to remove pause file %s on shutdown: %v", m.pauseFilePath, err)
		return
	}
	m.pauseFileOwned = false
}

func resolveCheckInterval(raw string) (time.Duration, error) {
	if raw == "" {
		return defaultAuroraCheckInterval, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("check_interval must be positive, got %s", raw)
	}
	return d, nil
}
