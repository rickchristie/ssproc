package ssproc

import (
	"context"
	"github.com/rickchristie/ssproc/util"
	"time"
)

type Client[Data JobData] struct {
	storage  Storage
	process  Process[Data]
	utilTime util.Time
}

func NewClient[Data JobData](storage Storage, process Process[Data], utilTime util.Time) *Client[Data] {
	return &Client[Data]{
		storage:  storage,
		process:  process,
		utilTime: utilTime,
	}
}

func (c *Client[Data]) Register(ctx context.Context, data Data) error {
	return c.RegisterStartAfter(ctx, data, c.utilTime.Now())
}

func (c *Client[Data]) RegisterStartAfter(ctx context.Context, data Data, startAfter time.Time) error {
	jobId := data.GetJobId()
	serialized, err := c.process.Serialize(data)
	if err != nil {
		return err
	}

	return c.storage.RegisterJob(ctx, &Job{
		JobId:                jobId,
		JobData:              serialized,
		ProcessId:            c.process.Id(),
		GoroutineId:          "",          // empty.
		GoroutineHeartBeatTs: time.Time{}, // nil.
		Status:               JSReady,
		NextSubprocess:       0,                // default.
		ExecCount:            0,                // default.
		StartAfterTs:         startAfter,       // default execute immediately.
		CreatedTs:            c.utilTime.Now(), // default.
	})
}
