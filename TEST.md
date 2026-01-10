# Test Suite Documentation

This document provides a comprehensive rundown of all test scenarios in the ssproc library.

## Overview

The test suite consists of **36 tests** across 3 test files:
- `executor_test.go` - Executor and Client tests
- `storage_pg_test.go` - PostgreSQL storage layer tests
- `compensation_test.go` - Compensation (Saga rollback) feature tests

---

## Executor Tests (`executor_test.go`)

### TestExecutor_SingleJob
**Purpose:** Verifies basic single job execution flow.
- Registers a job using `RegisterExecuteWait`
- Verifies job data is updated after execution
- Confirms job reaches `JSDone` status

### TestExecutor_MultipleJobs
**Purpose:** Tests concurrent execution of multiple jobs via the sweeper.
- Starts executor with sweeper enabled
- Registers 5 jobs via client
- Waits for all jobs to complete
- Verifies all jobs reach `JSDone` status

### TestExecutor_ExecuteNow
**Purpose:** Tests immediate job execution bypassing the sweeper.
- Configures executor with long sweep interval (1 hour)
- Registers a job via client
- Calls `ExecuteNow` to execute immediately
- Verifies job reaches `JSDone` status

### TestClient_Register
**Purpose:** Verifies basic job registration via client.
- Registers a job with `Register`
- Confirms job exists with `JSReady` status
- Validates job ID and process ID are correct

### TestClient_RegisterStartAfter
**Purpose:** Tests delayed job registration.
- Registers a job with `RegisterStartAfter` set to 1 hour in future
- Verifies `StartAfterTs` is correctly set

### TestExecutor_Cleanup
**Purpose:** Tests automatic cleanup of completed jobs.
- Enables cleanup with short interval (200ms) and threshold (1ms)
- Registers and executes a job
- Waits for job to complete
- Verifies job is deleted by cleanup sweeper

---

## Storage Tests (`storage_pg_test.go`)

### TestPgStorage_RegisterJob
**Purpose:** Tests basic job registration in PostgreSQL.
- Creates and registers a job directly via storage
- Verifies all job fields are stored correctly
- Confirms heartbeat/lease timestamps are initially null

### TestPgStorage_RegisterJob_DuplicateError
**Purpose:** Tests duplicate job ID detection.
- Registers a job
- Attempts to register same job ID again
- Verifies `JobIdAlreadyExist` error is returned

### TestPgStorage_GetOpenJobCandidates
**Purpose:** Tests retrieval of executable job candidates.
- Registers a ready job
- Calls `GetOpenJobCandidates`
- Verifies job ID is in returned list

### TestPgStorage_TryTakeOverJob
**Purpose:** Tests job lease acquisition.
- Registers a job
- Calls `TryTakeOverJob` with a goroutine ID
- Verifies goroutine ID is set
- Confirms heartbeat and lease timestamps are populated

### TestPgStorage_SendHeartbeat
**Purpose:** Tests heartbeat update mechanism.
- Registers and takes over a job
- Calls `SendHeartbeat`
- Verifies no error (heartbeat accepted)

### TestPgStorage_UpdateJob
**Purpose:** Tests job state update.
- Registers and takes over a job
- Updates job data, exec count, and next subprocess
- Verifies all changes are persisted

### TestPgStorage_ClearDoneJobs
**Purpose:** Tests cleanup of completed jobs.
- Registers a job and marks it as done
- Sets end timestamp in the past
- Calls `ClearDoneJobs`
- Verifies 1 job is deleted

### TestPgStorage_FilterJobs
**Purpose:** Tests job filtering/querying.
- Registers 5 jobs with same process ID
- Filters by process ID
- Verifies correct count and results

---

## Compensation Tests (`compensation_test.go`)

### Validation Tests

#### TestExecutor_RunCompensation_NoCompensationDefined
**Purpose:** Validates that enabling compensation requires at least one subprocess with compensation defined.
- Creates process with no compensation functions
- Attempts to create executor with `RunCompensation=true`
- Verifies error: "no subprocess has Compensation defined"

#### TestExecutor_RunCompensation_MaxCompCountZero
**Purpose:** Validates that `MaxCompensationCount` must be > 0 when compensation is enabled.
- Creates process with compensation defined
- Attempts to create executor with `RunCompensation=true` and `MaxCompensationCount=0`
- Verifies error: "MaxCompensationCount must be > 0"

---

### Basic Compensation Flow Tests

#### TestCompensation_BasicFlow
**Purpose:** Tests the complete compensation flow from failure to compensated state.
- Creates 3-subprocess process where subprocess[2] fails
- Sets `MaxExecutionCount=2` to trigger compensation after retries
- Executes until job reaches `JSCompensated` status
- Verifies:
  - Job status is `JSCompensated`
  - Job run type is `RTCompensation`
  - All compensation functions were called (reverse order)

#### TestCompensation_SkipNilCompensation
**Purpose:** Verifies nil compensation functions are skipped without error.
- Creates process with compensation[1] set to nil
- Subprocess[2] fails, triggering compensation
- Verifies:
  - Job reaches `JSCompensated`
  - compensation[2] and compensation[0] were called
  - compensation[1] was skipped (call count = 0)

#### TestCompensation_DisabledByDefault
**Purpose:** Confirms compensation is disabled by default for backward compatibility.
- Creates process with compensation functions
- Sets `RunCompensation=false` (default)
- Subprocess fails and exhausts retries
- Verifies:
  - Job reaches `JSError` (not `JSCompensated`)
  - Run type remains `RTNormal`
  - No compensation functions were called

#### TestCompensation_MaxCountExhausted
**Purpose:** Tests behavior when compensation retries are exhausted.
- Both transaction[2] and compensation[2] always fail
- Sets `MaxCompensationCount=2`
- Verifies:
  - Job reaches `JSError` status
  - Run type is `RTCompensation`
  - `CompCount` equals max (2)

---

### SubprocessResult Tests

#### TestCompensation_SREarlyExitDone
**Purpose:** Tests `SREarlyExitDone` during compensation (skip remaining compensations).
- Transaction[2] fails, compensation[2] returns `SREarlyExitDone`
- Verifies:
  - Job reaches `JSCompensated`
  - Only compensation[2] was called
  - compensation[1] and compensation[0] were skipped

#### TestCompensation_SREarlyExitError
**Purpose:** Tests `SREarlyExitError` during compensation (immediate error state).
- Transaction[2] fails, compensation[2] returns `SREarlyExitError`
- Verifies job reaches `JSError` status immediately

---

### Compensation Trigger Tests

#### TestCompensation_TriggerOnSREarlyExitError
**Purpose:** Verifies compensation triggers immediately on `SREarlyExitError` from transaction.
- Transaction[1] returns `SREarlyExitError`
- Verifies:
  - Job reaches `JSCompensated` in single execution
  - compensation[1] and compensation[0] were called
  - compensation[2] was not called (never reached)

#### TestCompensation_NoTriggerOnSREarlyExitDone
**Purpose:** Confirms `SREarlyExitDone` from transaction does NOT trigger compensation.
- Transaction[1] returns `SREarlyExitDone`
- Verifies:
  - Job reaches `JSDone` (not compensated)
  - Run type is `RTNormal`
  - No compensation functions were called

---

### Cleanup Tests

#### TestCleanup_IncludesCompensatedJobs
**Purpose:** Verifies compensated jobs are cleaned up like done jobs.
- Enables cleanup with aggressive settings
- Subprocess fails, triggering compensation
- Waits for job to reach `JSCompensated`
- Verifies job is deleted by cleanup sweeper

---

### Recovery Tests

#### TestCompensation_PanicRecovery
**Purpose:** Tests panic recovery during compensation execution.
- Compensation[2] panics on first call, succeeds on retry
- Verifies:
  - Panic is recovered
  - Job eventually reaches `JSCompensated`
  - Panic count >= 1

#### TestCompensation_DataPersistsDuringCompensation
**Purpose:** Verifies job data updates persist through compensation.
- All transactions update job data with step index
- All compensations update job data with step index
- Verifies final job data contains:
  - Transaction steps: [0, 1, 2]
  - Compensation steps: [2, 1, 0]

#### TestCompensation_ExecutorCrashRecovery
**Purpose:** Tests job recovery after executor "crash" during compensation.
- Transaction fails, transitioning to compensation mode
- Simulates crash by expiring lease
- Another execution picks up the job
- Verifies job continues in compensation mode

#### TestCompensation_MultipleExecutors
**Purpose:** Tests compensation with multiple executor instances.
- Creates two executors with same process
- Transaction[1] fails first 2 times, then succeeds
- Alternates execution between executors
- Verifies job reaches terminal state (done/compensated/error)

---

### Race Condition Tests

#### TestCompensation_Race_TwoExecutorsEnterCompensation
**Purpose:** Tests race condition when two executors try to enter compensation mode.
- Transaction fails, triggering compensation transition
- Both executors attempt to pick up job concurrently
- Verifies:
  - Job is in valid state
  - Run type is `RTCompensation`

#### TestCompensation_Race_ContextTimeoutDuringCompensation
**Purpose:** Tests context timeout handling during compensation.
- Compensation[2] blocks until context timeout on first attempt
- Short execution timeout (200ms)
- Verifies:
  - Job eventually reaches `JSCompensated` after retry
  - Timeout occurred at least once

#### TestCompensation_Race_HeartbeatFailureDuringCompensation
**Purpose:** Tests heartbeat failure during long-running compensation.
- Compensation[2] takes longer than lease duration on first attempt
- Verifies:
  - Job eventually reaches `JSCompensated`
  - Heartbeat mechanism allows recovery

#### TestCompensation_Race_MaxCompCountMidExecution
**Purpose:** Tests max compensation count enforcement during execution.
- All compensations always fail
- `MaxCompensationCount=3`
- Verifies:
  - Job reaches `JSError`
  - Run type is `RTCompensation`
  - `CompCount` equals max (3)

#### TestCompensation_Race_ConcurrentCompensationAndCleanup
**Purpose:** Tests that cleanup doesn't interfere with ongoing compensation.
- Compensation blocks with channel synchronization
- Cleanup runs aggressively while compensation is active
- Verifies job completes properly (not deleted mid-execution)

#### TestCompensation_Race_StressMultipleJobs
**Purpose:** Stress test with multiple concurrent jobs.
- Registers 10 jobs
- Transaction[2] fails for every other job
- 5 workers process concurrently
- Verifies:
  - All jobs reach terminal state
  - Mix of done (5) and compensated (5) jobs
  - Total completed equals 10

---

## Running Tests

```bash
# Run all tests
go test -v ./...

# Run specific test file
go test -v -run "TestCompensation" ./...

# Run with race detector
go test -race -v ./...

# Run specific test
go test -v -run "TestCompensation_BasicFlow" ./...
```

## Test Infrastructure

### Helper Types
- `CompensationTestJobData` - Job data struct for compensation tests
- `CompensationTestProcess` - Configurable process with atomic call counters
- `TestJobData` - Simple job data for executor tests
- `TestProcess` - Basic process for executor tests

### Test State
- `StateCreator()` - Creates test state with PostgreSQL connection
- `state.h.GetJob()` - Retrieves job by ID
- `state.h.SetLeaseExpireTime()` - Expires job lease for testing
- `state.h.WaitJobStatus()` - Waits for job to reach status
- `state.h.WaitAllJobsDone()` - Waits for all jobs to complete

### Utilities
- `testutil.Await()` - Polls condition with timeout
- `testutil.RandomAlphaNum()` - Generates random alphanumeric string
