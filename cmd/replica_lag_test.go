package cmd

import (
	"testing"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubReplicaLagFetcher struct{}

func (stubReplicaLagFetcher) GetMaxAuroraReplicaLagMs() (float64, error) { return 0, nil }

func TestNewReplicaLagFetcherReturnsPrimaryWithoutBinlogReplicas(t *testing.T) {
	tests := []struct {
		name              string
		enabled           bool
		binlogReplicaDSNs []string
	}{
		{name: "aurora replica check disabled", enabled: false, binlogReplicaDSNs: []string{"user:pass@tcp(cluster-b:3306)/test"}},
		{name: "no binlog replica DSNs", enabled: true, binlogReplicaDSNs: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{BinlogReplicaDSNs: tt.binlogReplicaDSNs}
			cfg.Common.PtOsc.AuroraReplicaCheck.Enabled = tt.enabled
			primary := stubReplicaLagFetcher{}

			fetcher, closeFn, err := newReplicaLagFetcher(cfg, primary)
			require.NoError(t, err)
			assert.Equal(t, primary, fetcher)
			closeFn()
		})
	}
}
