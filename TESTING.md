# Running Tests

## Setup

Install docker.

```shell
cd test/
./test-infra-build.sh
```

## Running test

After the docker containers are running, run the tests from the main directory:

```shell
./test-infra-run.sh
go clean -testcache
go test ./...
```

## Why we need another process to lock DB usage

Each of our test run in their own pristine database, this allows:
- Isolated test. Each test can build and configure their fixtures without affecting other tests.
- Parallel running of tests, because each test does not affect other tests.
- Easier to create test, because we can focus only on that test without worrying about clashes with other tests.

When running our tests, multiple packages are run in parallel.
- Each package consists of many tests that will also be run in parallel.
- Each package test runs in their own processes.

Because each package test runs in separate processes, these multiple processes need to coordinate database usage.
Once the database is locked for specific test, it must not be used by other tests, in the same process or not.
Coordinating different processes in the same machine means we cannot use mutexes.

## Implementation

The first implementation uses file locking. While this works, it introduces additional burden to the CPU, because each
test has to regularly try to lock a random database, this consumes CPU cycles. We also set a waiting period before the
next try, this further slows down test.

We want a solution where each test will block and wait, and as soon as a database is free, it is immediately
distributed to waiting tests.

The solution is to create a simple locking service that can keep many connections open. It uses channel and mutex to
quickly lock and distribute free databases to any request that's currently waiting. From my test, this is the solution
that significantly reduces CPU usage, allowing more parallelization and quicker test runtime.

So the simple drawing of our test runner is:

```
    pkg1/test1-----(lock-wait)-->DbLockerContainer
              <----(connStr)-----
              --------------------------------(connect)------>TestDbContainer
              ------(unlock)---->DbLockerContainer
```