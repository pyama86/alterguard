package ptosc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeFetcher struct {
	lagMs atomic.Value
	err   atomic.Value
	calls atomic.Int64
}

func newFakeFetcher(initialLag float64) *fakeFetcher {
	f := &fakeFetcher{}
	f.lagMs.Store(initialLag)
	return f
}

func (f *fakeFetcher) setLag(v float64) {
	f.lagMs.Store(v)
}

func (f *fakeFetcher) setErr(err error) {
	if err == nil {
		f.err.Store((*errWrap)(nil))
		return
	}
	f.err.Store(&errWrap{err: err})
}

func (f *fakeFetcher) GetMaxAuroraReplicaLagMs() (float64, error) {
	f.calls.Add(1)
	if v := f.err.Load(); v != nil {
		if w, ok := v.(*errWrap); ok && w != nil {
			return 0, w.err
		}
	}
	return f.lagMs.Load().(float64), nil
}

type errWrap struct{ err error }

func TestAuroraMonitorPreflight(t *testing.T) {
	t.Run("writable path passes", func(t *testing.T) {
		dir := t.TempDir()
		cfg := config.AuroraReplicaCheckConfig{
			Enabled:       true,
			MaxLagMs:      1000,
			PauseFilePath: filepath.Join(dir, "pause"),
		}
		m, err := NewAuroraMonitor(cfg, newFakeFetcher(0), logrus.New())
		require.NoError(t, err)

		require.NoError(t, m.Preflight())
		_, statErr := os.Stat(cfg.PauseFilePath)
		assert.True(t, os.IsNotExist(statErr), "pause file should be cleaned up after preflight")
	})

	t.Run("unwritable path fails", func(t *testing.T) {
		cfg := config.AuroraReplicaCheckConfig{
			Enabled:       true,
			MaxLagMs:      1000,
			PauseFilePath: "/nonexistent-dir-for-alterguard/pause",
		}
		m, err := NewAuroraMonitor(cfg, newFakeFetcher(0), logrus.New())
		require.NoError(t, err)
		err = m.Preflight()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot create pause file")
	})

	t.Run("information_schema unreadable fails", func(t *testing.T) {
		dir := t.TempDir()
		cfg := config.AuroraReplicaCheckConfig{
			Enabled:       true,
			MaxLagMs:      1000,
			PauseFilePath: filepath.Join(dir, "pause"),
		}
		fetcher := newFakeFetcher(0)
		fetcher.setErr(errors.New("access denied"))

		m, err := NewAuroraMonitor(cfg, fetcher, logrus.New())
		require.NoError(t, err)

		err = m.Preflight()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "information_schema.REPLICA_HOST_STATUS")
	})
}

func TestAuroraMonitorRunCreatesAndRemovesPauseFile(t *testing.T) {
	dir := t.TempDir()
	pausePath := filepath.Join(dir, "pause")

	fetcher := newFakeFetcher(2000) // 閾値超
	cfg := config.AuroraReplicaCheckConfig{
		Enabled:       true,
		MaxLagMs:      1000,
		CheckInterval: "20ms",
		PauseFilePath: pausePath,
	}
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	m, err := NewAuroraMonitor(cfg, fetcher, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	require.Eventually(t, func() bool {
		_, err := os.Stat(pausePath)
		return err == nil
	}, 1*time.Second, 10*time.Millisecond, "pause file should be created")

	// 遅延を解消するとファイルが削除される
	fetcher.setLag(100)
	require.Eventually(t, func() bool {
		_, err := os.Stat(pausePath)
		return os.IsNotExist(err)
	}, 1*time.Second, 10*time.Millisecond, "pause file should be removed when lag recovers")
}

func TestAuroraMonitorCleanupOnCancel(t *testing.T) {
	dir := t.TempDir()
	pausePath := filepath.Join(dir, "pause")

	fetcher := newFakeFetcher(5000)
	cfg := config.AuroraReplicaCheckConfig{
		Enabled:       true,
		MaxLagMs:      1000,
		CheckInterval: "20ms",
		PauseFilePath: pausePath,
	}
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	m, err := NewAuroraMonitor(cfg, fetcher, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.Run(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(pausePath)
		return err == nil
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	<-done

	_, err = os.Stat(pausePath)
	assert.True(t, os.IsNotExist(err), "pause file should be cleaned up after cancel")
}

func TestResolveCheckInterval(t *testing.T) {
	t.Run("default when empty", func(t *testing.T) {
		d, err := resolveCheckInterval("")
		require.NoError(t, err)
		assert.Equal(t, defaultAuroraCheckInterval, d)
	})
	t.Run("parses duration", func(t *testing.T) {
		d, err := resolveCheckInterval("250ms")
		require.NoError(t, err)
		assert.Equal(t, 250*time.Millisecond, d)
	})
	t.Run("rejects zero", func(t *testing.T) {
		_, err := resolveCheckInterval("0s")
		require.Error(t, err)
	})
	t.Run("rejects garbage", func(t *testing.T) {
		_, err := resolveCheckInterval("not-a-duration")
		require.Error(t, err)
	})
}
