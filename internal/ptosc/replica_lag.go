package ptosc

import (
	"errors"
	"fmt"

	"github.com/pyama86/alterguard/internal/database"
)

// 複数の遅延取得元の最大値を返す。
type MaxReplicaLagFetcher struct {
	fetchers []ReplicaLagFetcher
}

func NewMaxReplicaLagFetcher(fetchers ...ReplicaLagFetcher) *MaxReplicaLagFetcher {
	return &MaxReplicaLagFetcher{fetchers: fetchers}
}

// 停止中は pause させたいので、他の取得元のエラーより ErrReplicationStopped を優先して返す。
func (f *MaxReplicaLagFetcher) GetMaxAuroraReplicaLagMs() (float64, error) {
	var maxLagMs float64
	var firstErr error
	for _, fetcher := range f.fetchers {
		lagMs, err := fetcher.GetMaxAuroraReplicaLagMs()
		if err != nil {
			if errors.Is(err, database.ErrReplicationStopped) {
				return 0, err
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if lagMs > maxLagMs {
			maxLagMs = lagMs
		}
	}
	if firstErr != nil {
		return 0, firstErr
	}
	return maxLagMs, nil
}

type BinlogReplicaLagSource interface {
	GetBinlogReplicaLagMs() (float64, error)
}

// binlog レプリカの遅延を ReplicaLagFetcher として扱うアダプタ。
type BinlogReplicaLagFetcher struct {
	source BinlogReplicaLagSource
}

func NewBinlogReplicaLagFetcher(source BinlogReplicaLagSource) *BinlogReplicaLagFetcher {
	return &BinlogReplicaLagFetcher{source: source}
}

func (f *BinlogReplicaLagFetcher) GetMaxAuroraReplicaLagMs() (float64, error) {
	lagMs, err := f.source.GetBinlogReplicaLagMs()
	if err != nil {
		return 0, fmt.Errorf("binlog replica: %w", err)
	}
	return lagMs, nil
}
