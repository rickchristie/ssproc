package ssproc

import (
	"context"
	"rukita.co/main/be/data"
	"rukita.co/main/be/lib/mend"
	"rukita.co/main/be/lib/util"
	"time"
)

type Client[Data JobData] struct {
	storage  Storage
	process  Process[Data]
	utilTime util.Time
}

func NewClient[Data JobData](storage Storage, process Process[Data]) *Client[Data] {
	return &Client[Data]{
		storage:  storage,
		process:  process,
		utilTime: util.NewGlobalTime(data.DefaultTimeZone()),
	}
}

func (c *Client[Data]) Register(ctx context.Context, data Data) error {
	return c.RegisterStartAfter(ctx, data, c.utilTime.Now())
}

func (c *Client[Data]) RegisterStartAfter(ctx context.Context, data Data, startAfter time.Time) error {
	jobId := data.GetJobId()
	serialized, err := c.process.Serialize(data)
	if err != nil {
		return mend.Wrap(err, true)
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
