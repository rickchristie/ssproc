# ssproc

**Stateful Simple Process Processor** - A distributed job executor for Go with automatic retries, compensation (rollback), and at-least-once execution guarantees. Uses PostgreSQL as the only backend.

## Why ssproc?

Building reliable distributed systems is hard. When a multi-step operation fails halfway through, you need:
- **Automatic retries** that persist across server restarts
- **Compensation logic** to rollback partial changes
- **Distributed coordination** so multiple workers don't duplicate effort
- **Visibility** into what's running, what failed, and why

Most solutions require complex infrastructure: message queues, separate orchestrators, or in-memory stores that lose data on restart.

**ssproc takes a different approach**: it uses PostgreSQL - the database you already have - as the durable job store. No Redis. No Kafka. No separate coordinator service. Just your app and Postgres.

## Features

- **At-least-once execution** - Jobs persist in Postgres and retry until success
- **Saga pattern support** - Define compensation (rollback) transactions for each step
- **Distributed execution** - Multiple executors coordinate via database leases
- **Atomic job registration** - Register jobs within your app's database transaction
- **Delayed execution** - Schedule jobs for future processing
- **Automatic cleanup** - Completed jobs deleted after configurable retention
- **Panic recovery** - Subprocess panics are caught and logged, not lost
- **Comprehensive filtering** - Query jobs by status, process, time range with pagination

## Installation

```bash
go get github.com/rickchristie/ssproc
```

Requires Go 1.21+ and PostgreSQL 12+.

## Quick Start

### 1. Create the jobs table

```sql
CREATE TABLE ssproc_jobs (
    job_id TEXT PRIMARY KEY,
    job_data TEXT NOT NULL,
    process_id TEXT NOT NULL,
    goroutine_id VARCHAR(512) NOT NULL DEFAULT '',
    goroutine_heart_beat_ts TIMESTAMPTZ,
    goroutine_lease_expire_ts TIMESTAMPTZ,
    status VARCHAR(64) NOT NULL DEFAULT 'ready',
    next_subprocess INT NOT NULL DEFAULT 0,
    run_type VARCHAR(64) NOT NULL DEFAULT 'normal',
    goroutine_ids TEXT[] NOT NULL DEFAULT '{}',
    exec_count INT NOT NULL DEFAULT 0,
    comp_count INT NOT NULL DEFAULT 0,
    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    start_after_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_ts TIMESTAMPTZ,
    end_ts TIMESTAMPTZ,
    last_update_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Required indexes for performance
CREATE INDEX idx_ssproc_jobs_open ON ssproc_jobs (process_id, status, start_after_ts, goroutine_lease_expire_ts);
CREATE INDEX idx_ssproc_jobs_cleanup ON ssproc_jobs (process_id, status, end_ts);
```

### 2. Define your job data and process

```go
package main

import (
    "context"
    "encoding/json"
    "log"

    "github.com/rickchristie/ssproc"
)

// JobData must implement GetJobId()
type OrderJobData struct {
    OrderID string `json:"order_id"`
    Amount  int    `json:"amount"`
    Status  string `json:"status"`
}

func (d OrderJobData) GetJobId() string {
    return d.OrderID
}

// Process defines the subprocess chain
type OrderProcess struct{}

func (p *OrderProcess) Id() string {
    return "order-fulfillment"
}

func (p *OrderProcess) GetSubprocesses() []*ssproc.Subprocess[OrderJobData] {
    return []*ssproc.Subprocess[OrderJobData]{
        {
            Transaction: func(ctx context.Context, goroutineId string, data OrderJobData, update ssproc.JobDataUpdater[OrderJobData]) ssproc.SubprocessResult {
                // Step 1: Reserve inventory
                log.Printf("Reserving inventory for order %s", data.OrderID)
                data.Status = "inventory_reserved"
                update(data)
                return ssproc.SRSuccess
            },
        },
        {
            Transaction: func(ctx context.Context, goroutineId string, data OrderJobData, update ssproc.JobDataUpdater[OrderJobData]) ssproc.SubprocessResult {
                // Step 2: Charge payment
                log.Printf("Charging $%d for order %s", data.Amount, data.OrderID)
                data.Status = "payment_charged"
                update(data)
                return ssproc.SRSuccess
            },
        },
        {
            Transaction: func(ctx context.Context, goroutineId string, data OrderJobData, update ssproc.JobDataUpdater[OrderJobData]) ssproc.SubprocessResult {
                // Step 3: Ship order
                log.Printf("Shipping order %s", data.OrderID)
                data.Status = "shipped"
                update(data)
                return ssproc.SRSuccess
            },
        },
    }
}

func (p *OrderProcess) Serialize(data OrderJobData) (string, error) {
    b, err := json.Marshal(data)
    return string(b), err
}

func (p *OrderProcess) Deserialize(s string) (OrderJobData, error) {
    var data OrderJobData
    err := json.Unmarshal([]byte(s), &data)
    return data, err
}
```

### 3. Create executor and register jobs

```go
import "time"

func main() {
    ctx := context.Background()
    connStr := "postgres://user:pass@localhost:5432/mydb"

    process := &OrderProcess{}

    // Create storage
    storage, err := ssproc.NewPgStorage(ssproc.PgStorageConfig{
        ConnStrSelector: ssproc.NewStaticSelector(connStr),
        Schema:          "public",
        Table:           "ssproc_jobs",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Create and start executor
    executor, err := ssproc.NewExecutor(ctx, process, storage, ssproc.ExecutorConfig{
        MaxWorkers:        10,
        SweepInterval:     30 * time.Second,
        ExecutionTimeout:  5 * time.Minute,
        MaxExecutionCount: 3,
    })
    if err != nil {
        log.Fatal(err)
    }
    executor.Start()
    defer executor.Stop()

    // Create client and register a job
    client := ssproc.NewClientSimple(storage, process)
    err = client.Register(ctx, OrderJobData{
        OrderID: "order-123",
        Amount:  9999,
        Status:  "pending",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Job will be picked up and executed by the executor
    select {}
}
```

## Core Concepts

### Job Lifecycle

```
Register → ready → [executor picks up] → executing → done
                                      ↓
                              (on failure)
                                      ↓
                              retry (up to MaxExecutionCount)
                                      ↓
                              compensation → compensated
                                      ↓
                              (if compensation fails)
                                      ↓
                                    error
```

### Subprocesses

A Process consists of one or more Subprocesses executed in order. Each subprocess returns a result:

| Result | Meaning |
|--------|---------|
| `SRSuccess` | Continue to next subprocess |
| `SRFailed` | Retry the job (or compensate if max retries exceeded) |
| `SREarlyExitDone` | Mark job as done immediately, skip remaining subprocesses |
| `SREarlyExitError` | Mark job as error immediately |

### Leases and Heartbeats

When an executor picks up a job, it acquires a lease by setting `goroutine_lease_expire_ts`. While executing, it sends heartbeats to extend the lease. If an executor crashes, its lease expires and another executor can take over the job.

### Atomic Registration with `RegisterTx`

Register jobs within your application's database transaction for atomic operations:

```go
import "github.com/rickchristie/ssproc/pgxadapter"

tx, _ := conn.Begin(ctx)
defer tx.Rollback(ctx)

// Your app logic
_, _ = tx.Exec(ctx, "INSERT INTO orders ...")

// Register job in same transaction
client.RegisterTx(ctx, pgxadapter.NewFromTx(tx), jobData)

tx.Commit(ctx) // Job only registered if commit succeeds
```

## Configuration

### ExecutorConfig

| Field | Default | Description |
|-------|---------|-------------|
| `ExecutorName` | `""` | Prefix for goroutine IDs (useful for debugging) |
| `MaxWorkers` | `NumCPU` | Maximum concurrent job executions |
| `MinWorkers` | `0` | Minimum workers to keep alive |
| `HeartbeatInterval` | `45s` | How often to renew job lease |
| `LeaseExpireDuration` | `1m` | Time before lease expires (must be > HeartbeatInterval) |
| `SweepInterval` | `50s` | How often to poll for new jobs |
| `SweepIntervalJitter` | `1s` | Random jitter added to sweep interval |
| `MaxJobsPerSweep` | `100` | Maximum jobs to fetch per sweep |
| `ExecutionTimeout` | `5m` | Timeout for entire job execution |
| `MaxExecutionCount` | `5` | Max retries before compensation |
| `MaxCompensationCount` | `5` | Max compensation retries before error |
| `EnableCleanup` | `false` | Automatically delete completed jobs |
| `CleanupInterval` | - | How often to run cleanup |
| `CleanupThreshold` | - | Minimum age of completed jobs to delete |
| `CleanupBatchSize` | `500` | Jobs to delete per cleanup batch |

### PgStorageConfig

| Field | Description |
|-------|-------------|
| `ConnStrSelector` | Connection string selector (use `NewStaticSelector` for single endpoint) |
| `Schema` | PostgreSQL schema name |
| `Table` | Table name for jobs |
| `Logger` | Optional logger implementation |
| `Location` | Timezone for timestamps (default: UTC) |

## Advanced Usage

### Delayed Execution

Schedule a job to start at a future time:

```go
client.RegisterStartAfter(ctx, jobData, time.Now().Add(1*time.Hour))
```

### Immediate Execution

Execute a job synchronously and wait for the result:

```go
result, err := executor.RegisterExecuteWait(ctx, "trace-123", jobData)
if err != nil {
    // Handle error
}
// result is the final job data after all subprocesses complete
```

### Query Jobs

Filter and paginate through jobs:

```go
filter := ssproc.JobFilter{
    ProcessId: "order-fulfillment",
    Status:    ssproc.JobStatusReady,
}
jobs, total, err := storage.FilterJobs(ctx, filter, 1, 50)
```

### Multiple Database Endpoints

Use `RoundRobinSelector` for load balancing across read replicas:

```go
selector := ssproc.NewRoundRobinSelector([]string{
    "postgres://host1:5432/db",
    "postgres://host2:5432/db",
})
storage, _ := ssproc.NewPgStorage(ssproc.PgStorageConfig{
    ConnStrSelector: selector,
    // ...
})
```

### Custom Logger

Implement the `Logger` interface:

```go
type Logger interface {
    Debug(msg string, keyvals ...any)
    Info(msg string, keyvals ...any)
    Warn(msg string, keyvals ...any)
    Error(msg string, keyvals ...any)
    Fatal(msg string, keyvals ...any)
}
```

## Schema Requirements

ssproc validates your table schema on startup. Required columns:

| Column | Type | Constraints |
|--------|------|-------------|
| `job_id` | `TEXT` | `PRIMARY KEY` |
| `job_data` | `TEXT` | `NOT NULL` |
| `process_id` | `TEXT` | `NOT NULL` |
| `goroutine_id` | `VARCHAR(512)` | `NOT NULL DEFAULT ''` |
| `goroutine_heart_beat_ts` | `TIMESTAMPTZ` | nullable |
| `goroutine_lease_expire_ts` | `TIMESTAMPTZ` | nullable |
| `status` | `VARCHAR(64)` | `NOT NULL DEFAULT 'ready'` |
| `next_subprocess` | `INT` | `NOT NULL DEFAULT 0` |
| `run_type` | `VARCHAR(64)` | `NOT NULL DEFAULT 'normal'` |
| `goroutine_ids` | `TEXT[]` | `NOT NULL DEFAULT '{}'` |
| `exec_count` | `INT` | `NOT NULL DEFAULT 0` |
| `comp_count` | `INT` | `NOT NULL DEFAULT 0` |
| `created_ts` | `TIMESTAMPTZ` | `NOT NULL` |
| `start_after_ts` | `TIMESTAMPTZ` | `NOT NULL` |
| `started_ts` | `TIMESTAMPTZ` | nullable |
| `end_ts` | `TIMESTAMPTZ` | nullable |
| `last_update_ts` | `TIMESTAMPTZ` | `NOT NULL` |

Required indexes:
1. Primary key on `job_id`
2. Composite index on `(process_id, status, start_after_ts, goroutine_lease_expire_ts)`
3. Composite index on `(process_id, status, end_ts)`

## License

See repository for license details.
