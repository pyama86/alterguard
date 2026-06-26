# alterguard

MySQL schema change utility that automatically chooses between `ALTER TABLE` and Percona Toolkit's `pt-online-schema-change` based on table row count.

## Overview

alterguard is a tool for safely and efficiently executing MySQL schema changes. It automatically selects the appropriate change method based on table row count thresholds.

### Key Features

- **Automatic Method Selection**: Chooses ALTER TABLE or pt-online-schema-change based on row count thresholds
- **Buffer Pool Size Check**: Prevents dropping tables that are heavily cached in memory
- **Slack Notifications**: Sends execution status notifications to Slack
- **Kubernetes Ready**: Optimized for Kubernetes Job execution
- **Dry Run Mode**: Safe test execution
- **Stdin Support**: Read queries from standard input
- **Strict Error Handling**: Immediate stop on error occurrence

## Installation

### Binary Build

```bash
git clone https://github.com/pyama86/alterguard.git
cd alterguard
go build -o alterguard .
```

### Docker

```bash
docker build -t alterguard .
```

## Configuration

### Environment Variables

| Variable            | Required | Description                                                            |
| ------------------- | -------- | ---------------------------------------------------------------------- |
| `DATABASE_DSN`      | ✓        | MySQL connection string (e.g., `user:pass@tcp(localhost:3306)/dbname`) |
| `SLACK_WEBHOOK_URL` | ✓        | Slack Webhook URL                                                      |
| `DEBUG`             | -        | Set to `true` to enable debug logging                                  |

### Configuration Files

#### Common Configuration (`config-common.yaml`)

```yaml
pt_osc:
  charset: utf8mb4
  recursion_method: "dsn=D=<db>,t=dsns"
  no_swap_tables: true
  chunk_size: 1000
  max_lag: 1.5
  statistics: true
  dry_run: false
  no_drop_triggers: false
  no_drop_new_table: false
  no_drop_old_table: false
  no_check_unique_key_change: false
  no_check_alter: false
  aurora_replica_check:
    enabled: false
    max_lag_ms: 1000
    check_interval: 5s
    pause_file_path: /tmp/alterguard-ptosc-pause

pt_osc_threshold: 1000000

alert:
  metadata_lock_threshold_seconds: 30

session_config:
  lock_wait_timeout: 10
  innodb_lock_wait_timeout: 10

# Disable ANALYZE TABLE execution before table swap (default: false, enabled)
disable_analyze_table: false

# Buffer pool size check threshold (optional, disabled if 0 or not set)
# Drop old table only if buffer pool size is below this threshold (in MB)
buffer_pool_size_threshold_mb: 100.0
```

#### Task Definition (`tasks.yaml`)

```yaml
- "CREATE TABLE `user_profiles` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int unsigned NOT NULL,
  `profile_data` json DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_user_profiles_user_id` (`user_id`),
  INDEX `idx_user_profiles_updated_at` (`updated_at`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"

- "ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active'"

- "ALTER TABLE orders DROP INDEX ix_legacy_status"

- "ALTER TABLE products MODIFY COLUMN price DECIMAL(10,2) NOT NULL"

- "DROP TABLE IF EXISTS old_user_sessions"
```

### Configuration Options

#### pt_osc Section

| Option                      | Type    | Default | Description                                                                        |
| --------------------------- | ------- | ------- | ---------------------------------------------------------------------------------- |
| `charset`                   | string  | utf8mb4 | Character set for pt-online-schema-change                                          |
| `recursion_method`          | string  | -       | Replication lag detection method                                                   |
| `no_swap_tables`            | bool    | true    | Skip table swapping (manual swap required)                                         |
| `chunk_size`                | int     | 1000    | Number of rows to process per chunk                                                |
| `max_lag`                   | float64 | 1.5     | Maximum replication lag threshold (seconds)                                        |
| `statistics`                | bool    | true    | Enable statistics collection                                                       |
| `dry_run`                   | bool    | false   | Run in dry-run mode                                                                |
| `no_drop_triggers`          | bool    | false   | Do not drop triggers after completion                                              |
| `no_drop_new_table`         | bool    | false   | Do not drop new table on failure                                                   |
| `no_drop_old_table`         | bool    | false   | Do not drop old table after swap                                                   |
| `no_check_unique_key_change`| bool    | false   | Disable unique key change check. When true, pt-osc can run even if the ALTER adds a unique index (bypasses pt-osc default safety check) |
| `no_check_alter`            | bool    | false   | Disable ALTER statement validation. When true, pt-osc can run even if the ALTER contains potentially unsafe operations like column renames (bypasses pt-osc default safety check) |
| `aurora_replica_check`      | object  | -       | Aurora reader replica lag monitor (see below) |

#### Aurora Replica Check Section (`pt_osc.aurora_replica_check`)

For Amazon Aurora MySQL clusters, pt-osc's standard `recursion_method` cannot observe reader lag. When enabled, alterguard polls `information_schema.REPLICA_HOST_STATUS` while pt-osc runs; if the maximum reader lag exceeds `max_lag_ms`, alterguard writes the pause file passed to pt-osc as `--pause-file`, which automatically suspends pt-osc until the lag recovers and the file is removed.

| Option            | Type    | Default                          | Description                                                                              |
| ----------------- | ------- | -------------------------------- | ---------------------------------------------------------------------------------------- |
| `enabled`         | bool    | false                            | Enable Aurora reader lag monitoring                                                      |
| `max_lag_ms`      | float64 | 0                                | Lag threshold in milliseconds. Above this value the pause file is created                |
| `check_interval`  | string  | 5s                               | Poll interval (Go duration string, e.g. `1s`, `500ms`)                                   |
| `pause_file_path` | string  | `/tmp/alterguard-ptosc-pause`    | Pause file location passed to pt-osc                                                     |

When enabled, alterguard performs a preflight before launching pt-osc:

1. Creates and removes the pause file to verify filesystem permissions.
2. Issues a `SELECT` against `information_schema.REPLICA_HOST_STATUS` to verify read permissions.

If either check fails, pt-osc is **not** started and an error is returned. The required MySQL privileges are described in the *Aurora support* section below.

#### Global Settings

| Option                         | Type    | Default | Description                                                                              |
| ------------------------------ | ------- | ------- | ---------------------------------------------------------------------------------------- |
| `pt_osc_threshold`             | int64   | -       | Row count threshold for using pt-osc                                                     |
| `disable_analyze_table`        | bool    | false   | Disable ANALYZE TABLE execution before table swap (default: enabled)                     |
| `buffer_pool_size_threshold_mb`| float64 | 0       | Buffer pool size threshold in MB for cleanup operations (0 = disabled, no size check) |

#### Alert Section

| Option                            | Type | Default | Description                               |
| --------------------------------- | ---- | ------- | ----------------------------------------- |
| `metadata_lock_threshold_seconds` | int  | 30      | Metadata lock warning threshold (seconds) |

#### Session Config Section

| Option                     | Type | Default | Description                                      |
| -------------------------- | ---- | ------- | ------------------------------------------------ |
| `lock_wait_timeout`        | int  | 10      | MySQL lock_wait_timeout setting (seconds)        |
| `innodb_lock_wait_timeout` | int  | 10      | MySQL innodb_lock_wait_timeout setting (seconds) |

## Usage

### Basic Usage

```bash
# Execute all tasks from file
./alterguard run --common-config config-common.yaml --tasks-config tasks.yaml

# Execute in dry-run mode
./alterguard run --common-config config-common.yaml --tasks-config tasks.yaml --dry-run

# Read queries from stdin
./alterguard run --common-config config-common.yaml --stdin

# Combine file and stdin input
./alterguard run --common-config config-common.yaml --tasks-config tasks.yaml --stdin

# Swap tables
./alterguard swap users --common-config config-common.yaml --tasks-config tasks.yaml

# Cleanup operations
./alterguard cleanup users --drop-table --common-config config-common.yaml --tasks-config tasks.yaml
./alterguard cleanup users --drop-triggers --common-config config-common.yaml --tasks-config tasks.yaml
./alterguard cleanup users --drop-table --drop-triggers --common-config config-common.yaml --tasks-config tasks.yaml
```

### Subcommands

#### `run`

Executes all tasks sequentially. Tables with row count ≤ `pt_osc_threshold` are processed with ALTER TABLE, while tables exceeding the threshold are processed with pt-online-schema-change.

**Options:**

- `--stdin`: Read queries from standard input
- `--dry-run`: Force pt-osc to run in dry-run mode

#### `swap [table_name]`

Swaps the backup table created by pt-online-schema-change with the original table.

Before swapping, executes ANALYZE TABLE on `_original_table_new` to update statistics (can be disabled with `disable_analyze_table: true`).

Performs RENAME TABLE operations:

- `original_table` → `original_table_old`
- `_original_table_new` → `original_table`

#### `cleanup [table_name]`

Cleans up resources created by pt-online-schema-change.

**Options:**

- `--drop-table`: Drop backup table (`table_name_old`)
- `--drop-triggers`: Drop triggers created by pt-osc (`pt_osc_table_name_*`)

At least one cleanup operation must be specified.

**Buffer Pool Size Check:**

When `buffer_pool_size_threshold_mb` is configured, the cleanup operation with `--drop-table` performs a safety check before dropping the old table:

1. Queries `INFORMATION_SCHEMA.INNODB_BUFFER_PAGE` to calculate the table's buffer pool size
2. Compares the size against the configured threshold
3. Only drops the table if the buffer pool size is below the threshold
4. Returns an error if the size exceeds the threshold, preventing potential performance impact

This feature helps prevent dropping tables that are still heavily cached in memory, which could cause performance degradation when the table data needs to be reloaded into the buffer pool.

### Using Standard Input

You can provide SQL queries via standard input:

```bash
# From pipe
echo "ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT FALSE;" | ./alterguard run --common-config config-common.yaml --stdin

# From file
cat migration.sql | ./alterguard run --common-config config-common.yaml --stdin

# Interactive input (terminate with Ctrl+D)
./alterguard run --common-config config-common.yaml --stdin
```

Queries from stdin should be terminated with semicolons. Multi-line queries are supported.

## Execution Flow

1. **Configuration Loading**: Loads settings from YAML configuration files and environment variables
2. **Query Collection**: Loads queries from tasks file and/or stdin
3. **Database Connection**: Establishes connection using DATABASE_DSN
4. **Table Analysis**: For ALTER TABLE statements, gets row count and compares with `pt_osc_threshold`
5. **Method Selection**:
   - Row count ≤ threshold: Direct ALTER TABLE execution
   - Row count > threshold: pt-online-schema-change execution
6. **Execution**: Processes all queries sequentially
7. **Error Handling**: Stops immediately on any error to prevent data corruption

## Kubernetes Usage

### Job Manifest Example

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: alterguard-schema-change
spec:
  template:
    spec:
      containers:
        - name: alterguard
          image: alterguard:latest
          command: ["./alterguard", "run"]
          args:
            - "--common-config=/config/config-common.yaml"
            - "--tasks-config=/config/tasks.yaml"
          env:
            - name: DATABASE_DSN
              valueFrom:
                secretKeyRef:
                  name: mysql-secret
                  key: dsn
            - name: SLACK_WEBHOOK_URL
              valueFrom:
                secretKeyRef:
                  name: slack-secret
                  key: webhook-url
          volumeMounts:
            - name: config
              mountPath: /config
      volumes:
        - name: config
          configMap:
            name: alterguard-config
      restartPolicy: Never
  backoffLimit: 0
```

### ConfigMap Example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: alterguard-config
data:
  config-common.yaml: |
    pt_osc:
      charset: utf8mb4
      recursion_method: "dsn=D=<db>,t=dsns"
      no_swap_tables: true
      chunk_size: 1000
      max_lag: 1.5
      statistics: true
      dry_run: false
      no_check_unique_key_change: false
      no_check_alter: false

    pt_osc_threshold: 1000000

    alert:
      metadata_lock_threshold_seconds: 30

    session_config:
      lock_wait_timeout: 10
      innodb_lock_wait_timeout: 10

    buffer_pool_size_threshold_mb: 100.0

  tasks.yaml: |
    - "ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active'"
    - "ALTER TABLE orders DROP INDEX ix_legacy_status"
    - "DROP TABLE IF EXISTS old_user_sessions"
```

## Slack Notifications

alterguard sends Slack notifications at the following times:

- **Start**: Task execution begins
- **Success**: Task completion (including execution time)
- **Failure**: Error occurrence
- **Warning**: Metadata lock detection

### Notification Example

```
🚀 Schema change started
Query: ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active'
Row count: 500000

✅ Schema change completed successfully
Query: ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active'
Row count: 500000
Duration: 2m30s
Method: ALTER TABLE
```

## Testing

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Test specific package
go test ./internal/config
```

## Development

### Prerequisites

- Go 1.24+
- MySQL 5.7+ or 8.0+
- pt-online-schema-change (Percona Toolkit)

### Local Development

```bash
# Install dependencies
go mod download

# Build
go build -o alterguard .

# Create .env file (optional)
echo "DATABASE_DSN=user:pass@tcp(localhost:3306)/testdb" > .env
echo "SLACK_WEBHOOK_URL=https://hooks.slack.com/services/..." >> .env

# Run with file input
./alterguard run --common-config examples/config-common.yaml --tasks-config examples/tasks.yaml

# Run with stdin input
echo "ALTER TABLE test ADD COLUMN new_col INT;" | ./alterguard run --common-config examples/config-common.yaml --stdin
```

## Aurora Support

When running against an Amazon Aurora MySQL cluster, enable `pt_osc.aurora_replica_check` to throttle pt-osc based on reader replica lag observed in `information_schema.REPLICA_HOST_STATUS`.

### Required MySQL Privileges

The user specified in `DATABASE_DSN` must be able to read `information_schema.REPLICA_HOST_STATUS`. In Aurora this view is exposed via the standard `SELECT` privilege on `information_schema` (already implicit for any authenticated user), but make sure that the role used has not been further restricted, e.g.:

```sql
GRANT SELECT ON `information_schema`.* TO 'alterguard'@'%';
```

The other privileges already required by pt-osc and alterguard remain unchanged (`ALTER`, `CREATE`, `DROP`, `INSERT`, `UPDATE`, `DELETE`, `INDEX`, `LOCK TABLES`, `TRIGGER`, `PROCESS`, `REPLICATION CLIENT`).

### Required OS Permissions

The process running alterguard must be able to create and remove files at `pause_file_path`. The default `/tmp/alterguard-ptosc-pause` works for most setups; in Kubernetes ensure the path is writable (`emptyDir` volume mount, or default writable filesystem).

## License

MIT License

## Contributing

Pull requests and issue reports are welcome.

## Important Notes

- Always test in a non-production environment before using in production
- Understand pt-online-schema-change limitations before use
- Large table changes may take considerable time
- Metadata locks may occur, so set appropriate maintenance windows
- The tool stops immediately on any error to prevent data corruption
- When using `--stdin`, queries must be terminated with semicolons
- The `pt_osc_threshold` setting determines when to use pt-online-schema-change vs direct ALTER TABLE
- Session timeout settings help prevent long-running locks during schema changes
- The `buffer_pool_size_threshold_mb` setting helps prevent performance degradation by checking buffer pool usage before dropping tables. Set this value based on your system's buffer pool size and acceptable cache eviction impact
