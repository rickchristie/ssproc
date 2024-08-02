package ssproc

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/sclevine/spec"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"rukita.co/main/be/lib/test"
	"testing"
	"time"
)

func TestRegisterJob(t *testing.T) {
	test.Describe(t, StateCreator, func(t *testing.T, Before, After test.Each, Context spec.G, It spec.S, state test.State) {
		s := state.(*State)
		var ctx context.Context
		var storage *PgStorage

		Before(func() {
			ctx = s.Db.DebugCtx
			storage = s.h.PgStorage
		})

		It("can register and try take over job, makes sure job ID unique", func() {
			jobId := "A1"
			t.Run("insert job, nil values", func(t *testing.T) {
				// Insert JobData.
				insertTime := time.Now()
				job := &Job{
					JobId:          jobId,
					JobData:        test.RandomString(1024),
					ProcessId:      test.RandomString(100),
					Status:         JSReady,
					NextSubprocess: rand.Intn(200),
					CreatedTs:      test.RandomTime(),
					StartAfterTs:   test.RandomTime(),
					EndTs:          test.RandomTime(), // Ignored.
				}
				err := storage.RegisterJob(ctx, job)
				assert.Nil(t, err)

				// Assert inserted JobData.
				found, isValid := s.h.GetJob(t, jobId)
				assert.Equal(t, job.JobId, found.JobId)
				assert.Equal(t, job.JobData, found.JobData)
				assert.Equal(t, job.ProcessId, found.ProcessId)
				assert.Equal(t, job.GoroutineId, found.GoroutineId)
				assert.Equal(t, job.Status, found.Status)
				assert.Equal(t, job.NextSubprocess, found.NextSubprocess)
				assert.Equal(t, RTNormal, found.RunType)
				assert.Empty(t, found.GoroutineIds)
				assert.Equal(t, 0, found.ExecCount)
				assert.Equal(t, 0, found.CompCount)
				assert.Equal(t, job.CreatedTs.UnixMicro(), found.CreatedTs.UnixMicro())
				assert.Equal(t, job.StartAfterTs.UnixMicro(), found.StartAfterTs.UnixMicro())
				assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= insertTime.UnixMicro())
				assert.Equal(t, true, found.GoroutineHeartBeatTs.IsZero())
				assert.Equal(t, true, found.GoroutineLeaseExpireTs.IsZero())
				assert.Equal(t, true, found.StartedTs.IsZero())
				assert.Equal(t, true, found.EndTs.IsZero())

				// Assert these fields are inserted as nil.
				assert.Equal(t, false, isValid["goroutine_heart_beat_ts"])
				assert.Equal(t, false, isValid["goroutine_lease_expire_ts"])
				assert.Equal(t, false, isValid["started_ts"])
				assert.Equal(t, false, isValid["end_ts"])
			})

			t.Run("reject when there are job with the same ID", func(t *testing.T) {
				job := &Job{
					JobId:                jobId,
					JobData:              test.RandomString(1024),
					ProcessId:            test.RandomString(100),
					GoroutineId:          "",
					GoroutineHeartBeatTs: time.Time{},
					Status:               JSReady,
					NextSubprocess:       rand.Intn(200),
					ExecCount:            rand.Intn(200),
					CreatedTs:            test.RandomTime(),
					StartAfterTs:         test.RandomTime(),
					StartedTs:            test.RandomTime(),
					EndTs:                test.RandomTime(),
				}
				err := storage.RegisterJob(ctx, job)
				assert.NotNil(t, err)
				assert.Equal(t, true, errors.Is(err, JobIdAlreadyExist))
			})
		})

		It("register with all non-nil values", func() {
			t := t

			// Insert JobData 1.
			insertTime := time.Now()
			heartBeat := test.RandomTime()
			job := &Job{
				JobId:                  "A1",
				JobData:                test.RandomString(1024),
				ProcessId:              test.RandomString(100),
				GoroutineId:            test.UUIDString(),
				GoroutineHeartBeatTs:   heartBeat,
				GoroutineLeaseExpireTs: heartBeat.Add(1 * time.Second),
				Status:                 JSReady,
				NextSubprocess:         rand.Intn(200),
				RunType:                "ignored",                        // Ignored.
				GoroutineIds:           []string{test.RandomString(200)}, // Ignored.
				ExecCount:              rand.Intn(200),                   // Ignored.
				CompCount:              rand.Intn(200),                   // Ignored.
				CreatedTs:              test.RandomTime(),
				StartAfterTs:           test.RandomTime(),
				StartedTs:              test.RandomTime(),
				EndTs:                  test.RandomTime(),              // Ignored.
				LastUpdateTs:           time.Now().Add(-5 * time.Hour), // Ignored.
			}
			err := storage.RegisterJob(ctx, job)
			assert.Nil(t, err)

			foundJob, _ := s.h.GetJob(t, job.JobId)
			assert.Equal(t, job.JobData, foundJob.JobData)

			// Time fields are all inserted, except EndTs and LastUpdateTs.
			assert.Equal(t, true, foundJob.EndTs.IsZero())
			assert.Equal(t, true, foundJob.LastUpdateTs.UnixMicro() >= insertTime.UnixMicro())

			assert.Equal(t, job.GoroutineHeartBeatTs.UnixMicro(), foundJob.GoroutineHeartBeatTs.UnixMicro())
			assert.Equal(t, job.GoroutineLeaseExpireTs.UnixMicro(), foundJob.GoroutineLeaseExpireTs.UnixMicro())
			assert.Equal(t, job.CreatedTs.UnixMicro(), foundJob.CreatedTs.UnixMicro())
			assert.Equal(t, job.StartedTs.UnixMicro(), foundJob.StartedTs.UnixMicro())
			assert.Equal(t, job.StartAfterTs.UnixMicro(), foundJob.StartAfterTs.UnixMicro())

			// These fields are filled with default values.
			assert.Equal(t, RTNormal, foundJob.RunType)
			assert.NotNil(t, foundJob.GoroutineIds)
			assert.Empty(t, foundJob.GoroutineIds)
			assert.Equal(t, 0, foundJob.ExecCount)
			assert.Equal(t, 0, foundJob.CompCount)

			job.GoroutineHeartBeatTs = foundJob.GoroutineHeartBeatTs
			job.GoroutineLeaseExpireTs = foundJob.GoroutineLeaseExpireTs
			job.CreatedTs = foundJob.CreatedTs
			job.StartedTs = foundJob.StartedTs
			job.StartAfterTs = foundJob.StartAfterTs
			job.EndTs = foundJob.EndTs
			job.LastUpdateTs = foundJob.LastUpdateTs
			job.RunType = foundJob.RunType
			job.GoroutineIds = foundJob.GoroutineIds
			job.ExecCount = foundJob.ExecCount
			job.CompCount = foundJob.CompCount
			assert.Equal(t, job, foundJob)
		})

		It("rejects when process ID is empty", func() {
			job := &Job{
				JobId:          "A1",
				JobData:        test.RandomString(1024),
				Status:         JSReady,
				NextSubprocess: rand.Intn(200),
				CreatedTs:      test.RandomTime(),
				StartAfterTs:   test.RandomTime(),
				EndTs:          test.RandomTime(), // Ignored.
			}
			err := storage.RegisterJob(ctx, job)
			assert.NotNil(t, err)
			assert.Equal(t, "process ID must not be empty!", err.Error())
		})
	})
}

func TestTryTakeOverJob(t *testing.T) {
	test.Describe(t, StateCreator, func(t *testing.T, Before, After test.Each, Context spec.G, It spec.S, state test.State) {
		s := state.(*State)
		var job1 *Job

		Before(func() {
			t := t
			err := s.Storage.RegisterJob(s.h.Ctx, &Job{
				JobId:          "A1",
				JobData:        test.RandomString(1024),
				ProcessId:      test.RandomString(100),
				Status:         JSReady,
				NextSubprocess: rand.Intn(200),
				CreatedTs:      test.RandomTime(),
				StartAfterTs:   test.RandomTime(),
				EndTs:          test.RandomTime(), // Ignored.
			})
			assert.Nil(t, err)

			job1, _ = s.h.GetJob(t, "A1")
		})

		It("take over", func() {
			t.Run("take over successfully", func(t *testing.T) {
				goroutineId := test.UUIDString()
				takeOverTime := time.Now()
				foundJob, err := s.Storage.TryTakeOverJob(
					s.h.Ctx, job1.JobId, goroutineId,
					15*time.Second,
				)
				assert.Nil(t, err)
				assert.Equal(t, job1.JobData, foundJob.JobData)

				// LastUpdateTs is updated.
				assert.Equal(t, true, foundJob.LastUpdateTs.UnixMicro() >= takeOverTime.UnixMicro())

				// Heartbeat and LeaseExpire is updated.
				assert.Equal(t, true, foundJob.GoroutineHeartBeatTs.UnixMicro() >= takeOverTime.UnixMicro())
				assert.Equal(t, foundJob.GoroutineLeaseExpireTs, foundJob.GoroutineHeartBeatTs.Add(15*time.Second))

				// Does not update these fields at all.
				assert.Equal(t, job1.CreatedTs.UnixMicro(), foundJob.CreatedTs.UnixMicro())
				assert.Equal(t, job1.StartedTs.UnixMicro(), foundJob.StartedTs.UnixMicro())
				assert.Equal(t, job1.EndTs.UnixMicro(), foundJob.EndTs.UnixMicro())
				assert.Equal(t, job1.StartAfterTs.UnixMicro(), foundJob.StartAfterTs.UnixMicro())

				// Does not add GoroutineIds.
				assert.Empty(t, foundJob.GoroutineIds)

				// Only these fields are changed.
				job1.GoroutineId = goroutineId
				job1.LastUpdateTs = foundJob.LastUpdateTs
				job1.CreatedTs = foundJob.CreatedTs
				job1.StartedTs = foundJob.StartedTs
				job1.EndTs = foundJob.EndTs
				job1.StartAfterTs = foundJob.StartAfterTs
				job1.GoroutineHeartBeatTs = foundJob.GoroutineHeartBeatTs
				job1.GoroutineLeaseExpireTs = foundJob.GoroutineLeaseExpireTs
				job1.RunType = foundJob.RunType
				job1.GoroutineIds = foundJob.GoroutineIds
				assert.Equal(t, job1, foundJob)

				job1 = foundJob
			})

			t.Run("rejects when job is already leased", func(t *testing.T) {
				goroutineId := test.UUIDString()
				foundJob, err := s.Storage.TryTakeOverJob(
					s.h.Ctx, job1.JobId, goroutineId,
					15*time.Second,
				)
				assert.Nil(t, foundJob)
				assert.NotNil(t, err)
				assert.Equal(t, true, errors.Is(err, AlreadyLeased))
			})

			t.Run("takes over when lease is expired", func(t *testing.T) {
				s.h.SetLeaseExpireTime(t, job1.JobId, time.Now())
				goroutineId := test.UUIDString()
				takeOverTime := time.Now()
				foundJob, err := s.Storage.TryTakeOverJob(
					s.h.Ctx, job1.JobId, goroutineId,
					1*time.Minute,
				)
				assert.Nil(t, err)

				// GoroutineId is updated.
				assert.NotEqual(t, job1, foundJob)
				assert.Equal(t, goroutineId, foundJob.GoroutineId)
				assert.True(t, foundJob.GoroutineHeartBeatTs.UnixMicro() >= takeOverTime.UnixMicro())
				assert.Equal(t, foundJob.GoroutineLeaseExpireTs, foundJob.GoroutineHeartBeatTs.Add(1*time.Minute))
				assert.True(t, foundJob.LastUpdateTs.UnixMicro() >= takeOverTime.UnixMicro())

				// Only these fields have changed.
				job1.GoroutineId = foundJob.GoroutineId
				job1.GoroutineHeartBeatTs = foundJob.GoroutineHeartBeatTs
				job1.GoroutineLeaseExpireTs = foundJob.GoroutineLeaseExpireTs
				job1.LastUpdateTs = foundJob.LastUpdateTs
				assert.Equal(t, job1, foundJob)
			})
		})
	})
}

func TestLockJob(t *testing.T) {
	test.Describe(t, StateCreator, func(t *testing.T, Before, After test.Each, Context spec.G, It spec.S, state test.State) {
		s := state.(*State)
		var jobIdA = "1"
		var jobIdB = "2"

		var lockJobA test.AcquireDbLock = func(t *testing.T) func() {
			tx, ctx, cancel, err := s.Storage.beginTx(s.Db.DebugCtx)
			assert.Nil(t, err)

			found, err := s.Storage.lockJob(ctx, tx, jobIdA)
			assert.Nil(t, err)
			assert.Equal(t, JSReady, found.Status)
			assert.Equal(t, jobIdA, found.JobId)

			return func() {
				defer cancel()
				err = tx.Rollback(s.h.Ctx)
				if err != nil {
					assert.True(t, errors.Is(err, pgx.ErrTxClosed))
				}
			}
		}
		var lockJobB test.AcquireDbLock = func(t *testing.T) func() {
			tx, ctx, cancel, err := s.Storage.beginTx(s.Db.DebugCtx)
			assert.Nil(t, err)

			found, err := s.Storage.lockJob(ctx, tx, jobIdB)
			assert.Nil(t, err)
			assert.Equal(t, JSReady, found.Status)
			assert.Equal(t, jobIdB, found.JobId)

			return func() {
				defer cancel()
				err = tx.Rollback(s.h.Ctx)
				if err != nil {
					assert.True(t, errors.Is(err, pgx.ErrTxClosed))
				}
			}
		}

		Before(func() {
			assert.NotNil(t, lockJobA)
			assert.NotNil(t, lockJobB)

			job := &Job{
				JobId:        jobIdA,
				ProcessId:    "procId",
				Status:       JSReady,
				StartAfterTs: time.Now(),
				CreatedTs:    time.Now(),
			}
			err := s.Storage.RegisterJob(s.Db.DebugCtx, job)
			assert.Nil(t, err)

			job = &Job{
				JobId:        jobIdB,
				ProcessId:    "procId",
				Status:       JSReady,
				StartAfterTs: time.Now(),
				CreatedTs:    time.Now(),
			}
			err = s.Storage.RegisterJob(s.Db.DebugCtx, job)
			assert.Nil(t, err)
		})

		It("locks the same row only", func() {
			test.AssertDbLock(t, lockJobA)
			test.AssertDbLock(t, lockJobB)
			test.AssertDbNotLocked(t, lockJobA, lockJobB)
		})

		It("returns error when the row does not exist", func() {
			t := t
			tx, ctx, cancel, err := s.Storage.beginTx(s.h.Ctx)
			assert.Nil(t, err)

			defer s.Storage.rollback(ctx, cancel, tx)

			job, err := s.Storage.lockJob(s.h.Ctx, tx, "89u92384")
			assert.Nil(t, job)
			assert.NotNil(t, err)
			assert.Equal(t, true, errors.Is(err, pgx.ErrNoRows))
		})
	})
}
