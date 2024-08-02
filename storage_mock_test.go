package ssproc

import (
	"context"
	"time"
)

var _ Storage = (*StorageMockWrapper)(nil)

type StorageMockWrapper struct {
	Wrapped Storage

	RegisterMock             func(ctx context.Context, job *Job) error
	GetOpenJobCandidatesMock func(ctx context.Context, processId string, maxJobsToReturn int) ([]string, error)
	TryTakeOverJobMock       func(
		ctx context.Context,
		jobId string,
		goroutineId string,
		leaseExpireDuration time.Duration,
	) (
		job *Job,
		err error,
	)
	SendHeartbeatMock func(
		ctx context.Context,
		goroutineId string,
		jobId string,
		leaseExpireDuration time.Duration,
	) error
	GetJobMock        func(ctx context.Context, jobId string) (*Job, error)
	UpdateJobMock     func(ctx context.Context, job *Job, leaseExpireDuration time.Duration) error
	ClearDoneJobsMock func(ctx context.Context, activeThreshold time.Time) error
}

func (s *StorageMockWrapper) GetJob(ctx context.Context, jobId string) (*Job, error) {
	if s.GetJobMock != nil {
		return s.GetJobMock(ctx, jobId)
	}
	return s.Wrapped.GetJob(ctx, jobId)
}

func (s *StorageMockWrapper) RegisterJob(ctx context.Context, job *Job) error {
	if s.RegisterMock != nil {
		return s.RegisterMock(ctx, job)
	}
	return s.Wrapped.RegisterJob(ctx, job)
}

func (s *StorageMockWrapper) GetOpenJobCandidates(
	ctx context.Context,
	processId string,
	maxJobsToReturn int,
) (
	[]string,
	error,
) {
	if s.GetOpenJobCandidatesMock != nil {
		return s.GetOpenJobCandidatesMock(ctx, processId, maxJobsToReturn)
	}
	return s.Wrapped.GetOpenJobCandidates(ctx, processId, maxJobsToReturn)
}

func (s *StorageMockWrapper) TryTakeOverJob(
	ctx context.Context,
	jobId string,
	goroutineId string,
	leaseExpireDuration time.Duration,
) (
	job *Job,
	err error,
) {
	if s.TryTakeOverJobMock != nil {
		return s.TryTakeOverJobMock(ctx, jobId, goroutineId, leaseExpireDuration)
	}
	return s.Wrapped.TryTakeOverJob(ctx, jobId, goroutineId, leaseExpireDuration)
}

func (s *StorageMockWrapper) SendHeartbeat(
	ctx context.Context,
	goroutineId string,
	jobId string,
	leaseExpireDuration time.Duration,
) error {
	if s.SendHeartbeatMock != nil {
		return s.SendHeartbeatMock(ctx, goroutineId, jobId, leaseExpireDuration)
	}
	return s.Wrapped.SendHeartbeat(ctx, goroutineId, jobId, leaseExpireDuration)
}

func (s *StorageMockWrapper) UpdateJob(ctx context.Context, job *Job, leaseExpireDuration time.Duration) error {
	if s.UpdateJobMock != nil {
		return s.UpdateJobMock(ctx, job, leaseExpireDuration)
	}
	return s.Wrapped.UpdateJob(ctx, job, leaseExpireDuration)
}

func (s *StorageMockWrapper) ClearDoneJobs(ctx context.Context, activeThreshold time.Time) error {
	if s.ClearDoneJobsMock != nil {
		return s.ClearDoneJobsMock(ctx, activeThreshold)
	}
	return s.Wrapped.ClearDoneJobs(ctx, activeThreshold)
}
