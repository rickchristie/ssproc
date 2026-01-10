# Implementation Plan: Compensation for Subprocesses

## Overview

This plan implements the Saga pattern compensation feature for ssproc. Compensation allows rollback of partially completed multi-step operations when a failure occurs.

---

## Part 1: Data Model Changes

### 1.1 Add Compensation Field to Subprocess Struct

**File:** `process.go`

```go
type Subprocess[Data JobData] struct {
    Transaction  Execute[Data]
    Compensation Execute[Data]  // NEW - nil means no compensation for this step
}
```

**Impact:** No breaking changes. Existing code can continue to only set `Transaction`.

### 1.2 Add RunCompensation Config to ExecutorConfig

**File:** `executor.go`

```go
type ExecutorConfig struct {
    // ... existing fields ...

    // RunCompensation enables compensation execution when max retries exceeded.
    // Default: false (for backward compatibility)
    RunCompensation bool
}
```

### 1.3 Update Storage.UpdateJob to Include RunType and CompCount

**File:** `storage_pg.go`

The `UpdateJob` method currently updates:
- `job_data`, `status`, `end_ts`, `goroutine_ids`, `exec_count`, `started_ts`, `next_subprocess`

Need to add:
- `run_type`, `comp_count`

This is required for transitioning to compensation mode and tracking compensation retries.

---

## Part 2: Executor Logic Changes

### 2.1 Validation at Executor Start

**File:** `executor.go` in `NewExecutor`

Add validation:
1. If `RunCompensation = true` AND `MaxCompensationCount = 0` → Error
2. If `RunCompensation = true` AND no subprocess has `Compensation` defined → Error

```go
if config.RunCompensation {
    if config.MaxCompensationCount == 0 {
        return nil, errors.New("MaxCompensationCount must be > 0 when RunCompensation is enabled")
    }

    hasCompensation := false
    subprocesses := process.GetSubprocesses()
    for _, sp := range subprocesses {
        if sp.Compensation != nil {
            hasCompensation = true
            break
        }
    }
    if !hasCompensation {
        return nil, fmt.Errorf("RunCompensation is enabled but process %s has no compensation defined", process.Id())
    }
}
```

### 2.2 Modify execute() Method

**File:** `executor.go`

Current flow:
```
TakeOverJob → Check ExecCount → Run subprocesses forward → Mark done/error
```

New flow:
```
TakeOverJob → Check RunType
  → If RTNormal:
      Check ExecCount
        → If exhausted AND RunCompensation enabled:
            Transition to RTCompensation, set next_subprocess to current index
        → If exhausted AND RunCompensation disabled:
            Mark as error
      Run subprocess transaction forward
        → On SRFailed: return (will retry)
        → On SRSuccess: next_subprocess++
        → On SREarlyExitDone: mark done
        → On SREarlyExitError: transition to compensation OR mark error
      If all done: mark done

  → If RTCompensation:
      Check CompCount
        → If exhausted: mark error
      Run compensation backward (decrement next_subprocess)
        → Skip if compensation is nil
        → On SRFailed: return (will retry)
        → On SRSuccess: next_subprocess--
        → On SREarlyExitDone: mark compensated
        → On SREarlyExitError: mark error
      If next_subprocess < 0: mark compensated
```

### 2.3 Key Logic Details

#### Transition from Normal to Compensation

When transitioning to compensation mode:
- Set `run_type = 'compensation'`
- Keep `next_subprocess` at current value (the failed step)
- Do NOT increment `exec_count` when transitioning
- Increment `comp_count` on first compensation pickup

#### Compensation Execution Loop

```go
func (s *Executor[Data]) executeCompensation(ctx context.Context, curJob *Job, jobData Data) error {
    subprocesses := s.process.GetSubprocesses()

    for {
        // Terminal condition: all compensations done
        if curJob.NextSubprocess < 0 {
            break
        }

        select {
        case <-ctx.Done():
            return ContextCanceled
        default:
        }

        // Get compensation for current index
        compensation := subprocesses[curJob.NextSubprocess].Compensation

        // Skip if no compensation defined
        if compensation == nil {
            curJob.NextSubprocess--
            err := s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
            if err != nil {
                return err
            }
            continue
        }

        // Execute compensation
        result := s.executeCompensationStep(ctx, goroutineId, curJob, jobData, compensation)

        switch result {
        case SRFailed:
            return SubprocessFailed
        case SREarlyExitDone:
            return s.updateJobCompensated(ctx, curJob)
        case SREarlyExitError:
            return s.updateJobError(ctx, curJob)
        case SRSuccess:
            curJob.NextSubprocess--
            err := s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
            if err != nil {
                return err
            }
        }
    }

    // All compensations done
    curJob.NextSubprocess = -1  // Terminal value
    return s.updateJobCompensated(ctx, curJob)
}
```

#### Handling SREarlyExitError in Normal Execution

When `SREarlyExitError` is returned during normal execution:
- If `RunCompensation = true`: Transition to compensation mode
- If `RunCompensation = false`: Mark job as error (current behavior)

---

## Part 3: Storage Changes

### 3.1 Update UpdateJob Method

**File:** `storage_pg.go`

Add `run_type` and `comp_count` to the UPDATE query:

```go
query := fmt.Sprintf(`UPDATE %v.%v SET
    job_data = $1, status = $2, end_ts = $3,
    goroutine_ids = $4, exec_count = $5, started_ts = $6,
    next_subprocess = $7, goroutine_heart_beat_ts = $8,
    goroutine_lease_expire_ts = $9, last_update_ts = $10,
    run_type = $11, comp_count = $12
WHERE job_id = $13`,
    s.schema, s.table,
)
```

### 3.2 Update ClearDoneJobs to Include Compensated Jobs

**File:** `storage_pg.go`

Change the status filter from `status=$3` (JSDone only) to `status IN ($3, $4)` (JSDone OR JSCompensated):

```go
query := fmt.Sprintf(
    `DELETE FROM %v.%v
        WHERE job_id in(
            SELECT job_id FROM %v.%v
            WHERE end_ts <= $1 AND process_id = $2 AND status IN ($3, $4)
            ORDER BY end_ts ASC
            LIMIT $5 FOR UPDATE SKIP LOCKED
        )`, s.schema, s.table, s.schema, s.table,
)
res, err := tx.Exec(ctx, query, activeThreshold, processId, JSDone, JSCompensated, maxRowsToDelete)
```

### 3.3 Update validateStatusForUpdate

**File:** `storage_pg.go`

Add check for `JSCompensated`:

```go
func (s *PgStorage) validateStatusForUpdate(job *Job) error {
    if job.Status == JSDone {
        return interr.Wrap(AlreadyDone, true)
    }
    if job.Status == JSCompensated {
        return interr.Wrap(AlreadyCompensated, true)  // NEW error
    }
    if job.Status == JSError {
        return interr.Wrap(AlreadyError, true)
    }
    // ...
}
```

### 3.4 Add New Error for AlreadyCompensated

**File:** `errors.go`

```go
var AlreadyCompensated = errors.New("job status is already compensated")
```

---

## Part 4: Heartbeat Handling

### 4.1 Update Heartbeat to Handle Compensated Status

**File:** `executor.go`

In `pingHeartbeat`, add `AlreadyCompensated` to the list of terminal states:

```go
if errors.Is(err, AlreadyLeased) ||
    errors.Is(err, AlreadyError) ||
    errors.Is(err, AlreadyDone) ||
    errors.Is(err, AlreadyCompensated) {  // NEW
    return
}
```

---

## Part 5: New Helper Methods

### 5.1 Add updateJobCompensated Method

**File:** `executor.go`

```go
func (s *Executor[Data]) updateJobCompensated(ctx context.Context, curJob *Job) error {
    curJob.Status = JSCompensated
    curJob.NextSubprocess = -1  // Terminal value
    curJob.EndTs = s.timeUtil.Now()
    return s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
}
```

### 5.2 Add transitionToCompensation Method

**File:** `executor.go`

```go
func (s *Executor[Data]) transitionToCompensation(ctx context.Context, curJob *Job) error {
    curJob.RunType = RTCompensation
    // next_subprocess stays at current (failed) index
    // exec_count is not incremented during transition
    return s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
}
```

---

## Part 6: Refactored Execute Method

### 6.1 High-Level Structure

```go
func (s *Executor[Data]) execute(parentCtx context.Context, traceId, goroutineId, jobId string) error {
    // ... panic recovery, context setup, TakeOverJob ...

    // Start heartbeat
    go s.pingHeartbeat(ctx, cancel, traceId, goroutineId, jobId)

    // Route based on run type
    if curJob.RunType == RTCompensation {
        return s.executeCompensationMode(ctx, goroutineId, curJob, jobData)
    }
    return s.executeNormalMode(ctx, goroutineId, curJob, jobData)
}

func (s *Executor[Data]) executeNormalMode(ctx context.Context, goroutineId string, curJob *Job, jobData Data) error {
    // Check max execution count
    if curJob.ExecCount >= s.config.MaxExecutionCount {
        if s.config.RunCompensation {
            return s.transitionToCompensation(ctx, curJob)
        }
        return s.updateJobError(ctx, curJob)
    }

    // Increment exec_count, update goroutine_ids, etc.
    curJob.ExecCount++
    // ... existing logic ...

    // Execute subprocesses forward
    // ... existing loop with modifications for SREarlyExitError ...
}

func (s *Executor[Data]) executeCompensationMode(ctx context.Context, goroutineId string, curJob *Job, jobData Data) error {
    // Check max compensation count
    if curJob.CompCount >= s.config.MaxCompensationCount {
        return s.updateJobError(ctx, curJob)
    }

    // Increment comp_count
    curJob.CompCount++
    curJob.GoroutineIds = append(curJob.GoroutineIds, goroutineId)
    if curJob.StartedTs.IsZero() {
        curJob.StartedTs = s.timeUtil.Now()
    }
    err := s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
    if err != nil {
        return err
    }

    // Execute compensations backward
    return s.runCompensationLoop(ctx, goroutineId, curJob, jobData)
}
```

---

## Part 7: Test Plan

### 7.1 Unit Tests (executor_test.go additions)

#### Basic Compensation Tests

| Test Name | Description |
|-----------|-------------|
| `TestCompensation_BasicFlow` | 3 subprocesses, sp[2] fails, compensations run 2→1→0 |
| `TestCompensation_SkipNilCompensation` | sp[1] has no compensation, should be skipped |
| `TestCompensation_AllNilSkipped` | All compensations are nil, job marked compensated |
| `TestCompensation_DisabledByDefault` | Verify RunCompensation=false doesn't trigger compensation |
| `TestCompensation_MaxCountExhausted` | CompCount reaches max, job marked error |

#### Subprocess Result Tests

| Test Name | Description |
|-----------|-------------|
| `TestCompensation_SRSuccess` | Compensation returns success, moves to next |
| `TestCompensation_SRFailed` | Compensation fails, retries on next pickup |
| `TestCompensation_SREarlyExitDone` | Mark compensated immediately |
| `TestCompensation_SREarlyExitError` | Mark error immediately |

#### Trigger Tests

| Test Name | Description |
|-----------|-------------|
| `TestCompensation_TriggerOnMaxExecCount` | ExecCount exhausted triggers compensation |
| `TestCompensation_TriggerOnSREarlyExitError` | SREarlyExitError in normal mode triggers compensation |
| `TestCompensation_NoTriggerOnSREarlyExitDone` | SREarlyExitDone marks done, no compensation |

#### Validation Tests

| Test Name | Description |
|-----------|-------------|
| `TestExecutor_RunCompensation_NoCompensationDefined` | Error at startup |
| `TestExecutor_RunCompensation_MaxCompCountZero` | Error at startup |

#### Cleanup Tests

| Test Name | Description |
|-----------|-------------|
| `TestCleanup_IncludesCompensatedJobs` | Compensated jobs are cleaned up |

### 7.2 Race Condition Tests (compensation_race_test.go)

#### Critical Race Scenarios

| # | Scenario | Expected Behavior |
|---|----------|-------------------|
| 1 | Executor crash during compensation → Lease expires | Another executor picks up in RTCompensation mode |
| 2 | Two executors racing to enter compensation | Only one wins via TryTakeOverJob |
| 3 | Job in compensation + cleanup race | Status is ready, not cleaned |
| 4 | Heartbeat failure during compensation | Same as normal execution |
| 5 | Executor A starts comp[2], crashes. B picks up | B sees run_type=compensation, continues from comp[2] |
| 6 | Two executors race normal→compensation transition | One succeeds due to lease |
| 7 | Compensation succeeds but UpdateJob fails | Job stays ready, next pickup retries |
| 8 | Context timeout during compensation[1] | Job stays ready with next_subprocess=1 |
| 9 | Compensation calls update() then crashes | Job data persisted, retry starts with updated data |
| 10 | MaxCompensationCount reached mid-compensation | Job marked as error |

### 7.3 Stress Tests

```go
func TestCompensation_Stress_ConcurrentExecutors(t *testing.T) {
    // 5 executors, 100 jobs, random failures
    // Verify: all jobs end in done/compensated/error
    // Verify: no stuck jobs, no duplicate processing
}

func TestCompensation_Stress_HighFailureRate(t *testing.T) {
    // 80% failure rate in both transaction and compensation
    // Verify: jobs eventually reach terminal state
}

func TestCompensation_Stress_LeaseExpiry(t *testing.T) {
    // Short lease duration, slow compensation
    // Verify: correct handoff between executors
}
```

### 7.4 Panic Recovery Tests

| Test Name | Description |
|-----------|-------------|
| `TestCompensation_PanicRecovery` | Panic in compensation → SRFailed, job retries |

---

## Part 8: Implementation Order

### Phase 1: Core Infrastructure (No Behavior Change)
1. Add `Compensation` field to `Subprocess` struct
2. Add `RunCompensation` to `ExecutorConfig`
3. Add `AlreadyCompensated` error
4. Update `UpdateJob` to include `run_type` and `comp_count`
5. Update `validateStatusForUpdate` for compensated status
6. Update `pingHeartbeat` to handle compensated status

### Phase 2: Executor Logic
7. Add validation in `NewExecutor` for RunCompensation config
8. Add `updateJobCompensated` helper method
9. Add `transitionToCompensation` helper method
10. Refactor `execute` to route by RunType
11. Implement `executeCompensationMode`
12. Implement `runCompensationLoop`
13. Modify normal execution to trigger compensation on failure

### Phase 3: Cleanup
14. Update `ClearDoneJobs` to include compensated jobs

### Phase 4: Testing
15. Write unit tests for basic compensation flow
16. Write tests for subprocess result handling
17. Write validation tests
18. Write race condition tests
19. Write stress tests
20. Run all existing tests to ensure no regression

---

## Part 9: Files Changed Summary

| File | Changes |
|------|---------|
| `process.go` | Add `Compensation` field to `Subprocess` |
| `executor.go` | Add `RunCompensation` config, validation, compensation execution logic |
| `storage_pg.go` | Update `UpdateJob`, `ClearDoneJobs`, `validateStatusForUpdate` |
| `errors.go` | Add `AlreadyCompensated` error |
| `executor_test.go` | Add compensation unit tests |
| `compensation_race_test.go` | NEW: Race condition tests |
| `compensation_stress_test.go` | NEW: Stress tests |

---

## Part 10: Invariants to Maintain

1. **Backward Compatibility**: All existing tests pass without modification
2. **Idempotency**: Both Transaction and Compensation must be idempotent
3. **next_subprocess Semantics**: Always points to "next to execute"
4. **Terminal Values**:
   - `next_subprocess = len(subprocesses)` for normal completion
   - `next_subprocess = -1` for compensation completion
5. **Count Semantics**:
   - `exec_count` = "How many times job picked up for execution"
   - `comp_count` = "How many times job picked up for compensation"
   - Counts carry over, do not reset between retries
6. **Status During Execution**: Always `ready` until terminal state
7. **No Schema Changes**: Uses existing database columns

---

## Part 10: Edge Cases

| Case | Behavior |
|------|----------|
| subprocess[0] fails, no compensation | Job marked error (no compensations to run) |
| All compensations nil | Job marked compensated after skipping all |
| Compensation at index 0 succeeds | `next_subprocess = -1`, job compensated |
| Job already compensated, executor tries to pick up | `AlreadyCompensated` error, skip |
| Compensation updates job data | Data persisted for next retry if fails |

---

## Appendix: State Machine

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                                                             │
                    │                        ┌──────────┐                         │
                    │                   ┌───►│   done   │                         │
                    │                   │    └──────────┘                         │
                    │                   │                                         │
     register       │   ExecCount++     │    SRSuccess(last) OR SREarlyExitDone   │
         │          │       │           │                                         │
         ▼          │       ▼           │                                         │
    ┌────────┐      │  ┌─────────┐      │                                         │
    │ ready  │──────┴─►│ normal  │──────┤                                         │
    └────────┘         │execution│      │                                         │
         ▲             └─────────┘      │    SRFailed: return, retry on next pickup
         │                  │           │                                         │
         │                  │ ExecCount >= Max                                    │
         │                  │ OR SREarlyExitError                                 │
         │                  │ (when RunCompensation=true)                         │
         │                  ▼                                                     │
         │           ┌──────────────┐   │                                         │
         │           │ compensation │───┤                                         │
         │           │  execution   │   │    SRSuccess: next_subprocess--         │
         │           └──────────────┘   │    nil compensation: skip               │
         │                  │           │                                         │
         │                  │ CompCount >= Max                                    │
         │                  │ OR SREarlyExitError                                 │
         │                  ▼           │    SREarlyExitDone OR all done          │
         │            ┌─────────┐       │           │                             │
         │            │  error  │       │           ▼                             │
         │            └─────────┘       │    ┌─────────────┐                      │
         │                  ▲           │    │ compensated │                      │
         │                  │           │    └─────────────┘                      │
         │                  │           │           ▲                             │
         │                  │           └───────────┘                             │
         │                  │                                                     │
         │                  │ ExecCount >= Max (when RunCompensation=false)       │
         │                  │ OR SREarlyExitError (when RunCompensation=false)    │
         │                  │                                                     │
         └──────────────────┴─────────────────────────────────────────────────────┘
               Lease expiry: another executor picks up job
```
