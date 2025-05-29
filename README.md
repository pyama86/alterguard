# alterguard

MySQL schema change utility that automatically chooses between `ALTER TABLE` and Percona Toolkit's `pt-online-schema-change` based on table row count.

## Overview

alterguard is a tool for safely and efficiently executing MySQL schema changes. It automatically selects the appropriate change method based on table row count thresholds.

### Key Features

- **Automatic Method Selection**: Chooses ALTER TABLE or pt-online-schema-change based on row count thresholds
- **Slack Notifications**: Sends execution status notifications to Slack
- **Kubernetes Ready**: Optimized for Kubernetes Job execution
- **Dry Run Mode**: Safe test execution
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

alert:
  metadata_lock_threshold_seconds: 30
```

#### Task Definition (`tasks.yaml`)

```yaml
- name: add_user_column
  table: users
  alter_statement: ADD COLUMN foo INT
  threshold: 1000000

- name: drop_old_index
  table: orders
  alter_statement: DROP INDEX ix_old
  threshold: 500000
```

## Usage

### Basic Usage

```bash
# Execute all tasks
./alterguard run --common-config config-common.yaml --tasks-config tasks.yaml

# Execute in dry-run mode
./alterguard run --common-config config-common.yaml --tasks-config tasks.yaml --dry-run

# Swap tables
./alterguard swap users --common-config config-common.yaml --tasks-config tasks.yaml

# Cleanup
./alterguard cleanup users --drop-table --common-config config-common.yaml --tasks-config tasks.yaml
./alterguard cleanup users --drop-triggers --common-config config-common.yaml --tasks-config tasks.yaml
```

### Subcommands

#### `run`

Executes all tasks sequentially. Tables with row count ≤ threshold are processed with ALTER TABLE, while tables exceeding the threshold are processed with pt-online-schema-change.

#### `swap`

Swaps the backup table created by pt-online-schema-change with the original table.

#### `cleanup`

- `--drop-table`: Drops backup table (`table_name_old`)
- `--drop-triggers`: Drops triggers created by pt-osc

## Execution Flow

1. **Configuration Loading**: Loads settings from YAML configuration files and environment variables
2. **Table Classification**: Gets row count for each table and classifies by threshold
3. **Small Table Processing**: Processes tables ≤ threshold with ALTER TABLE
4. **Large Table Processing**: Processes tables > threshold with pt-osc (only if single table)
5. **Error Handling**: Stops with error if multiple large tables exist

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
    alert:
      metadata_lock_threshold_seconds: 30

  tasks.yaml: |
    - name: add_user_column
      table: users
      alter_statement: ADD COLUMN foo INT
      threshold: 1000000
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
Task: add_user_column
Table: users
Row count: 500000

✅ Schema change completed successfully
Task: add_user_column
Table: users
Row count: 500000
Duration: 2m30s
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

- Go 1.21+
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

# Run
./alterguard run --common-config examples/config-common.yaml --tasks-config examples/tasks.yaml
```

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
