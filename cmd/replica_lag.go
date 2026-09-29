package cmd

import (
	"fmt"

	"github.com/pyama86/alterguard/internal/config"
	"github.com/pyama86/alterguard/internal/database"
	"github.com/pyama86/alterguard/internal/ptosc"
)

// aurora_replica_check 有効時のみ BINLOG_REPLICA_DSNS の各クラスタも遅延監視対象に加える。
// 返す close 関数で追加した接続を閉じる。
func newReplicaLagFetcher(cfg *config.Config, primary ptosc.ReplicaLagFetcher) (ptosc.ReplicaLagFetcher, func(), error) {
	if !cfg.Common.PtOsc.AuroraReplicaCheck.Enabled || len(cfg.BinlogReplicaDSNs) == 0 {
		return primary, func() {}, nil
	}

	var clients []*database.MySQLClient
	closeClients := func() {
		for _, client := range clients {
			if err := client.Close(); err != nil {
				logger.Errorf("Failed to close binlog replica connection: %v", err)
			}
		}
	}

	fetchers := []ptosc.ReplicaLagFetcher{primary}
	for i, dsn := range cfg.BinlogReplicaDSNs {
		client, err := database.NewMySQLClient(dsn, logger)
		if err != nil {
			closeClients()
			return nil, nil, fmt.Errorf("binlog replica [index: %d] connection failed: %w", i, err)
		}
		clients = append(clients, client)
		fetchers = append(fetchers, ptosc.NewBinlogReplicaLagFetcher(client))
	}

	logger.Infof("Binlog replica connections established: %d", len(clients))
	return ptosc.NewMaxReplicaLagFetcher(fetchers...), closeClients, nil
}
