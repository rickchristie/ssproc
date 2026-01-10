// Package ssproc is a simple multi-transaction process executor with retry and compensation (rollback).
// Once a Job is registered, it guarantees that the Job will be executed at least once.
// ssproc uses Postgres as its main data storage (for storing Job data and metadata).
// You can create your own storage by implementing the Storage interface.
//
// # Quick Start
//
// 1. Create a Process implementation that defines your job data and subprocesses:
//
//	type MyJobData struct {
//	    ID   string
//	    Data string
//	}
//
//	func (d MyJobData) GetJobId() string { return d.ID }
//
//	type MyProcess struct{}
//
//	func (p *MyProcess) Id() string { return "my-process" }
//
//	func (p *MyProcess) GetSubprocesses() []*ssproc.Subprocess[MyJobData] {
//	    return []*ssproc.Subprocess[MyJobData]{
//	        {Transaction: p.step1},
//	        {Transaction: p.step2},
//	    }
//	}
//
//	func (p *MyProcess) step1(ctx context.Context, gid string, data MyJobData, update ssproc.JobDataUpdater[MyJobData]) ssproc.SubprocessResult {
//	    // Do work...
//	    return ssproc.SRSuccess
//	}
//
// 2. Create storage and start the executor:
//
//	storage, err := ssproc.NewPgStorage(ssproc.PgStorageConfig{
//	    ConnStrSelector: ssproc.NewStaticSelector(connStr),
//	    Schema:          "public",
//	    Table:           "ssproc_jobs",
//	})
//
//	executor, err := ssproc.NewExecutor(ctx, &MyProcess{}, storage, ssproc.ExecutorConfig{})
//	executor.Start()
//	defer executor.Stop()
//
// 3. Register jobs using Client:
//
//	client := ssproc.NewClientSimple(storage, &MyProcess{})
//	err := client.Register(ctx, MyJobData{ID: "job-1", Data: "hello"})
//
// # Same-Transaction Job Submission
//
// You can register jobs within your existing database transactions using RegisterTx:
//
//	tx, _ := conn.Begin(ctx)
//	adapter := pgxadapter.NewFromTx(tx)
//	err := client.RegisterTx(ctx, adapter, jobData)
//	tx.Commit(ctx)
//
// This ensures that job registration is atomic with your other database operations.
package ssproc
