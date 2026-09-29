package ptosc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/pyama86/alterguard/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubLagFetcher struct {
	lagMs float64
	err   error
}

func (s stubLagFetcher) GetMaxAuroraReplicaLagMs() (float64, error) {
	return s.lagMs, s.err
}

type stubBinlogLagSource struct {
	lagMs float64
	err   error
}

func (s stubBinlogLagSource) GetBinlogReplicaLagMs() (float64, error) {
	return s.lagMs, s.err
}

func TestMaxReplicaLagFetcher(t *testing.T) {
	errOther := errors.New("access denied")
	errStopped := fmt.Errorf("Seconds_Behind_Source is NULL: %w", database.ErrReplicationStopped)

	tests := []struct {
		name        string
		fetchers    []ReplicaLagFetcher
		expectLagMs float64
		expectErr   error
	}{
		{
			name:        "returns max lag",
			fetchers:    []ReplicaLagFetcher{stubLagFetcher{lagMs: 100}, stubLagFetcher{lagMs: 3000}, stubLagFetcher{lagMs: 50}},
			expectLagMs: 3000,
		},
		{
			name:      "propagates error",
			fetchers:  []ReplicaLagFetcher{stubLagFetcher{lagMs: 100}, stubLagFetcher{err: errOther}},
			expectErr: errOther,
		},
		{
			name:      "stopped takes precedence over other errors",
			fetchers:  []ReplicaLagFetcher{stubLagFetcher{err: errOther}, stubLagFetcher{lagMs: 100}, stubLagFetcher{err: errStopped}},
			expectErr: database.ErrReplicationStopped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lagMs, err := NewMaxReplicaLagFetcher(tt.fetchers...).GetMaxAuroraReplicaLagMs()
			if tt.expectErr != nil {
				require.ErrorIs(t, err, tt.expectErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectLagMs, lagMs)
		})
	}
}

func TestBinlogReplicaLagFetcher(t *testing.T) {
	tests := []struct {
		name        string
		source      stubBinlogLagSource
		expectLagMs float64
		expectErr   error
	}{
		{
			name:        "returns source lag",
			source:      stubBinlogLagSource{lagMs: 1500},
			expectLagMs: 1500,
		},
		{
			name:      "keeps stopped error identifiable",
			source:    stubBinlogLagSource{err: fmt.Errorf("wrapped: %w", database.ErrReplicationStopped)},
			expectErr: database.ErrReplicationStopped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lagMs, err := NewBinlogReplicaLagFetcher(tt.source).GetMaxAuroraReplicaLagMs()
			if tt.expectErr != nil {
				require.ErrorIs(t, err, tt.expectErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectLagMs, lagMs)
		})
	}
}
