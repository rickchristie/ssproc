package ssproc

import (
	"context"
	"time"

	interr "github.com/rickchristie/ssproc/internal/errors"
	"github.com/rickchristie/ssproc/internal/timeutil"
)

// ClientConfig contains configuration for Client.
type ClientConfig[Data JobData] struct {
	Storage  Storage
	Process  Process[Data]
	Location *time.Location
}

// Client is used to register jobs for processing.
type Client[Data JobData] struct {
	storage  Storage
	process  Process[Data]
	utilTime timeutil.Time
}

// NewClient creates a new Client with the given configuration.
func NewClient[Data JobData](cfg ClientConfig[Data]) *Client[Data] {
	location := cfg.Location
	if location == nil {
		location = time.UTC
	}
	return &Client[Data]{
		storage:  cfg.Storage,
		process:  cfg.Process,
		utilTime: timeutil.NewGlobalTime(location),
	}
}

// NewClientSimple creates a new Client with simple parameters.
func NewClientSimple[Data JobData](storage Storage, process Process[Data]) *Client[Data] {
	return NewClient(ClientConfig[Data]{
		Storage: storage,
		Process: process,
	})
}

// Register registers a job for immediate execution.
func (c *Client[Data]) Register(ctx context.Context, data Data) error {
	return c.RegisterStartAfter(ctx, data, c.utilTime.Now())
}

// RegisterStartAfter registers a job for execution after the given time.
func (c *Client[Data]) RegisterStartAfter(ctx context.Context, data Data, startAfter time.Time) error {
	jobId := data.GetJobId()
	serialized, err := c.process.Serialize(data)
	if err != nil {
		return interr.Wrap(err, true)
	}

	return c.storage.RegisterJob(ctx, &Job{
		JobId:                jobId,
		JobData:              serialized,
		ProcessId:            c.process.Id(),
		GoroutineId:          "",
		GoroutineHeartBeatTs: time.Time{},
		Status:               JSReady,
		NextSubprocess:       0,
		ExecCount:            0,
		StartAfterTs:         startAfter,
		CreatedTs:            c.utilTime.Now(),
	})
}

// RegisterTx registers a job within an existing transaction.
func (c *Client[Data]) RegisterTx(ctx context.Context, tx QueryExecutor, data Data) error {
	return c.RegisterTxStartAfter(ctx, tx, data, c.utilTime.Now())
}

// RegisterTxStartAfter registers a job within an existing transaction for execution after the given time.
func (c *Client[Data]) RegisterTxStartAfter(ctx context.Context, tx QueryExecutor, data Data, startAfter time.Time) error {
	jobId := data.GetJobId()
	serialized, err := c.process.Serialize(data)
	if err != nil {
		return interr.Wrap(err, true)
	}

	return c.storage.RegisterJobTx(ctx, tx, &Job{
		JobId:                jobId,
		JobData:              serialized,
		ProcessId:            c.process.Id(),
		GoroutineId:          "",
		GoroutineHeartBeatTs: time.Time{},
		Status:               JSReady,
		NextSubprocess:       0,
		ExecCount:            0,
		StartAfterTs:         startAfter,
		CreatedTs:            c.utilTime.Now(),
	})
}

// SetMockTimeForTest sets a mock time for testing.
func SetClientMockTimeForTest[Data JobData](client *Client[Data], mockTime timeutil.Time) {
	client.utilTime = mockTime
}
