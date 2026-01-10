Read README.md and understand project code.
The compensation part is still not implemented.

# SPEC: Compensation for Subprocesses

  - No changes in current behavior (except for compensation running).
  - Each subprocess Compensation MUST be IDEMPOTENT, just like the Transaction itself.
  - Compensation is only run if the executor is configured to run compensation, we add new "RunCompensation" config, default to false.
  - If the process at subprocess index 2 failed and retry count is exhausted, then compensation for subprocess index 2 is run first,
  then compensation for subprocess 1, and so on.
  - If subprocess[2] failed, compensation execution starts from Compensation[2].
  - If a subprocess does not have compensation defined, it is skipped without errors during compensation execution, as if the compensation is a no-op and was successful.
  - Compensation execution has a separate retry count configuration. However, it uses the same database fields, so for this upgrade, no database schema changes are required.
  - If any compensation process fails, it retries using the same behavior as the current subprocess execution.
  - If retry count for compensation process is exhausted, then the job is set to error (same state as currently when subprocess failure retry count is exhausted).
  - SREarlyExitError triggers compensation as well. If user wants compensation to skip, they can update job specific field so their compensation logic will skip as well.
    This is by-design. Less complexity in library, more predictable behavior.
  - SREarlyExitDone DOES NOT trigger compensation. The job is marked done immediately. Compensation is only run on failures.
  - We reuse all SubprocessResult semantics:
  ┌──────────────────┬─────────────────┬─────────────────────────────────────────────────┐
  │      Result      │  During Normal  │         During Compensation                     │
  ├──────────────────┼─────────────────┼─────────────────────────────────────────────────┤
  │ SRSuccess        │ Next subprocess │ Next compensation (decrement index)             │
  ├──────────────────┼─────────────────┼─────────────────────────────────────────────────┤
  │ SRFailed         │ Retry or error  │ Retry or error (same behavior)                  │
  ├──────────────────┼─────────────────┼─────────────────────────────────────────────────┤
  │ SREarlyExitDone  │ Mark done       │ Mark compensated (skip remaining compensations) │
  ├──────────────────┼─────────────────┼─────────────────────────────────────────────────┤
  │ SREarlyExitError │ Mark error      │ Mark error (skip remaining compensations)       │
  └──────────────────┴─────────────────┴─────────────────────────────────────────────────┘
  - Add Compensation to Subprocess struct
  type Subprocess[Data JobData] struct {
      Transaction  Execute[Data]
      Compensation Execute[Data]  // NEW - nil means no compensation for this step
  }
  - Compensated jobs are also cleared by cleanup process, same as done jobs.
  - If RunCompensation = true but NO subprocess has Compensation defined - Executor must fail to start, with clear error message (has processId, subprocess index).
  - Scenario: compensation[2] succeeds after 2 retries. Now running compensation[1], comp_count carries over, does not reset.
    This keeps the same logic as exec_count, preventing confusion for the user.
    So comp_count and exec_count semantics are: "How many times have this job has been picked up and tried to be executed/compensated" 
  - next_subprocess retains semantics of "next to execute" during compensation as well.
    E.g. if subprocess[2] failed, next_subprocess = 2, mode turns into compensation, meaning we must run compensation[2] next.
    When compensation[2] succeeds, next_subprocess = 1, we run compensation[1] next, and so on.
    Easier to understand when devs take a look at the job in DB.
  - Terminal value of next_subprocess for JSCompensated is -1.
  - Don't update exec_count in db when changing to compensation.
  - When MaxCompensationCount = 0 and RunCompensation = true - Executor must fail to start, with clear error message.
  - Panic Recovery: Panics in Compensation are recovered and coverted to SRFailed, same as normal execution.
  - Status stays 'ready' during compensation (same as normal execution), only changing to 'compensated' or 'error' upon completion.
  - Logging: Use the same logs/observability as transaction.


Prioritize:
- No breaking change.
- Existing tests MUST still work without changes, as by default compensation is not run.
- Opt-in so people can drop-in upgrade and slowly write compensation for the processes.
- Complete, comprehensive, exhaustive testing including stress testing for race conditions. This library needs to be rock solid, battle tested, no issues.

# TESTS



Race condition scenarios to test (not limited to these, the more tests we have, the better)
Critical scenarios:
1. Executor crash during compensation → Lease expires, another executor picks up in RTCompensation mode
2. Two executors racing to enter compensation → Only one should win via TryTakeOverJob
3. Job in compensation + cleanup race → Status is ready during compensation, shouldn't be cleaned
4. Heartbeat failure during compensation → Same behavior as normal execution

#: 5
  Scenario: Executor A starts compensation[2], crashes. Executor B picks up, continues from compensation[2]
  Expected Behavior: B should see run_type=compensation, next_subprocess=2, continue correctly
  ────────────────────────────────────────
  #: 6
  Scenario: Two executors race to transition from normal→compensation
  Expected Behavior: Only one should succeed due to TryTakeOverJob lease
  ────────────────────────────────────────
  #: 7
  Scenario: Compensation succeeds but UpdateJob fails (DB error)
  Expected Behavior: Job stays ready, next pickup retries the already-succeeded compensation (idempotency concern)
  ────────────────────────────────────────
  #: 8
  Scenario: Context timeout during compensation[1]
  Expected Behavior: Job stays ready with next_subprocess=1, next pickup continues
  ────────────────────────────────────────
  #: 9
  Scenario: Compensation function calls update() then crashes
  Expected Behavior: Job data persisted, compensation retry starts with updated data
  ────────────────────────────────────────
  #: 10
  Scenario: MaxCompensationCount reached mid-compensation
  Expected Behavior: Job marked as error, remaining compensations skipped

**MAKE SURE THAT TESTS**