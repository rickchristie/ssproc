package ssproc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPgStorage_FilterJobs_Comprehensive(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	var jobs map[string]*Job
	var baseTime time.Time

	// Create test fixture data
	// Fixture Documentation:
	//
	// Job Fixtures:
	// | JobId | ProcessId | GoroutineId | Status       | ExecCount | RunType       | CreatedTs             | StartedTs             | EndTs                 |
	// |-------|-----------|-------------|--------------|-----------|---------------|-----------------------|-----------------------|-----------------------|
	// | J1    | proc1     | gr1         | ready        | 0         | normal        | 2024-01-01 10:00:00   | 0001-01-01 00:00:00   | 0001-01-01 00:00:00   |
	// | J2    | proc1     | gr2         | done         | 1         | normal        | 2024-01-01 11:00:00   | 2024-01-01 11:05:00   | 2024-01-01 11:10:00   |
	// | J3    | proc2     | gr3         | error        | 3         | normal        | 2024-01-01 12:00:00   | 2024-01-01 12:05:00   | 2024-01-01 12:15:00   |
	// | J4    | proc1     | gr4         | ready        | 2         | compensation  | 2024-01-01 13:00:00   | 2024-01-01 13:05:00   | 0001-01-01 00:00:00   |
	// | J5    | proc2     |             | ready        | 0         | normal        | 2024-01-01 14:00:00   | 0001-01-01 00:00:00   | 0001-01-01 00:00:00   |
	// | J6    | proc1     | gr5         | done         | 1         | normal        | 2024-01-01 15:00:00   | 2024-01-01 15:05:00   | 2024-01-01 15:10:00   |
	baseTime = time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	jobs = s.h.TestJobRows(t, []*TestJobRow{
		{
			Id:           "J1",
			Data:         "data1",
			ProcessId:    "proc1",
			GoroutineId:  "gr1",
			Status:       JSReady,
			ExecCount:    0,
			CreatedTs:    baseTime,
			StartAfterTs: baseTime,
			LastUpdateTs: baseTime,
		},
		{
			Id:           "J2",
			Data:         "data2",
			ProcessId:    "proc1",
			GoroutineId:  "gr2",
			Status:       JSDone,
			ExecCount:    1,
			CreatedTs:    baseTime.Add(1 * time.Hour),
			StartedTs:    baseTime.Add(1*time.Hour + 5*time.Minute),
			EndTs:        baseTime.Add(1*time.Hour + 10*time.Minute),
			StartAfterTs: baseTime.Add(1 * time.Hour),
			LastUpdateTs: baseTime.Add(1 * time.Hour),
		},
		{
			Id:           "J3",
			Data:         "data3",
			ProcessId:    "proc2",
			GoroutineId:  "gr3",
			Status:       JSError,
			ExecCount:    3,
			CreatedTs:    baseTime.Add(2 * time.Hour),
			StartedTs:    baseTime.Add(2*time.Hour + 5*time.Minute),
			EndTs:        baseTime.Add(2*time.Hour + 15*time.Minute),
			StartAfterTs: baseTime.Add(2 * time.Hour),
			LastUpdateTs: baseTime.Add(2 * time.Hour),
		},
		{
			Id:           "J4",
			Data:         "data4",
			ProcessId:    "proc1",
			GoroutineId:  "gr4",
			Status:       JSReady,
			RunType:      RTCompensation,
			ExecCount:    2,
			CreatedTs:    baseTime.Add(3 * time.Hour),
			StartedTs:    baseTime.Add(3*time.Hour + 5*time.Minute),
			StartAfterTs: baseTime.Add(3 * time.Hour),
			LastUpdateTs: baseTime.Add(3 * time.Hour),
		},
		{
			Id:           "J5",
			Data:         "data5",
			ProcessId:    "proc2",
			Status:       JSReady,
			ExecCount:    0,
			CreatedTs:    baseTime.Add(4 * time.Hour),
			StartAfterTs: baseTime.Add(4 * time.Hour),
			LastUpdateTs: baseTime.Add(4 * time.Hour),
		},
		{
			Id:           "J6",
			Data:         "data6",
			ProcessId:    "proc1",
			GoroutineId:  "gr5",
			Status:       JSDone,
			ExecCount:    1,
			CreatedTs:    baseTime.Add(5 * time.Hour),
			StartedTs:    baseTime.Add(5*time.Hour + 5*time.Minute),
			EndTs:        baseTime.Add(5*time.Hour + 10*time.Minute),
			StartAfterTs: baseTime.Add(5 * time.Hour),
			LastUpdateTs: baseTime.Add(5 * time.Hour),
		},
	})

	type expected struct {
		totalRows      int
		jobIds         []string
		verifyAllField bool
	}

	type testRow struct {
		name         string
		filter       FilterJob
		page         int
		itemsPerPage int
		expected     *expected
	}

	runRows := func(t *testing.T, rows []*testRow) {
		for _, r := range rows {
			t.Run(r.name, func(t *testing.T) {
				// First, get all results in a single call
				foundJobs, total, err := s.Storage.FilterJobs(s.h.Ctx, r.filter, 1, 10000)
				assert.Nil(t, err)
				assert.Equal(t, r.expected.totalRows, total)
				assert.Equal(t, len(r.expected.jobIds), len(foundJobs))

				for i, expectedJobId := range r.expected.jobIds {
					assert.Equal(t, expectedJobId, foundJobs[i].JobId)
				}

				// Verify all fields when requested
				if r.expected.verifyAllField {
					assert.Equal(t, 1, len(foundJobs))
					job := foundJobs[0]
					expectedJob := jobs[r.expected.jobIds[0]]

					assert.Equal(t, expectedJob.JobId, job.JobId)
					assert.Equal(t, expectedJob.JobData, job.JobData)
					assert.Equal(t, expectedJob.ProcessId, job.ProcessId)
					assert.Equal(t, expectedJob.GoroutineId, job.GoroutineId)
					assert.Equal(t, expectedJob.Status, job.Status)
					assert.Equal(t, expectedJob.NextSubprocess, job.NextSubprocess)
					assert.Equal(t, expectedJob.RunType, job.RunType)
					assert.Equal(t, expectedJob.ExecCount, job.ExecCount)
					assert.Equal(t, expectedJob.CompCount, job.CompCount)
					assert.Equal(t, expectedJob.CreatedTs.UnixMicro(), job.CreatedTs.UnixMicro())
					assert.Equal(t, expectedJob.StartAfterTs.UnixMicro(), job.StartAfterTs.UnixMicro())
					assert.Equal(t, expectedJob.StartedTs.UnixMicro(), job.StartedTs.UnixMicro())
					assert.Equal(t, expectedJob.EndTs.UnixMicro(), job.EndTs.UnixMicro())
					assert.Equal(t, expectedJob.LastUpdateTs.UnixMicro(), job.LastUpdateTs.UnixMicro())
				}

				// Test pagination by fetching results page by page
				foundPaginated := make([]*Job, 0)
				page := 0
				for {
					pageJobs, _, err := s.Storage.FilterJobs(s.h.Ctx, r.filter, page+1, r.itemsPerPage)
					assert.Nil(t, err)
					if len(pageJobs) == 0 {
						break
					}
					page++
					foundPaginated = append(foundPaginated, pageJobs...)
				}

				// Verify paginated results match the full result set
				assert.Equal(t, len(foundJobs), len(foundPaginated))
				for i := range foundJobs {
					assert.Equal(t, foundJobs[i].JobId, foundPaginated[i].JobId)
				}
			})
		}
	}

	t.Run("filter by exact job ID", func(t *testing.T) {
		runRows(t, []*testRow{
			{
				name: "filter by exact job ID",
				filter: FilterJob{
					JobId: "J2",
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows:      1,
					jobIds:         []string{"J2"},
					verifyAllField: true,
				},
			},
			{
				name: "filter by process ID proc1",
				filter: FilterJob{
					ProcessId: "proc1",
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 4,
					jobIds:    []string{"J6", "J4", "J2", "J1"},
				},
			},
			{
				name: "filter by goroutine ID gr3",
				filter: FilterJob{
					GoroutineId: "gr3",
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 1,
					jobIds:    []string{"J3"},
				},
			},
			{
				name: "filter by status done",
				filter: FilterJob{
					JobStatus: JSDone,
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 2,
					jobIds:    []string{"J6", "J2"},
				},
			},
			{
				name: "filter by exec count < 3",
				filter: FilterJob{
					ExecCountLt: 3,
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 5,
					jobIds:    []string{"J6", "J5", "J4", "J2", "J1"},
				},
			},
			{
				name: "filter by exec count >= 2",
				filter: FilterJob{
					ExecCountGt: 1,
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 2,
					jobIds:    []string{"J4", "J3"},
				},
			},
			{
				name: "filter by exec count <= 1",
				filter: FilterJob{
					ExecCountLt: 2,
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 4,
					jobIds:    []string{"J6", "J5", "J2", "J1"},
				},
			},
			{
				name: "filter by created_ts between 11:00 and 13:00",
				filter: FilterJob{
					CreatedTsGte: baseTime.Add(1 * time.Hour),
					CreatedTsLte: baseTime.Add(3 * time.Hour),
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 3,
					jobIds:    []string{"J4", "J3", "J2"},
				},
			},
			{
				name: "filter by started_ts range",
				filter: FilterJob{
					StartedTsGte: baseTime.Add(2*time.Hour + 5*time.Minute),
					StartedTsLte: baseTime.Add(5*time.Hour + 5*time.Minute),
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 3,
					jobIds:    []string{"J6", "J4", "J3"},
				},
			},
			{
				name: "filter by end_ts range",
				filter: FilterJob{
					EndTsGte: baseTime.Add(1*time.Hour + 10*time.Minute),
					EndTsLte: baseTime.Add(5*time.Hour + 10*time.Minute),
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 3,
					jobIds:    []string{"J6", "J3", "J2"},
				},
			},
			{
				name: "filter by last_update_ts range",
				filter: FilterJob{
					LastUpdateTsGte: baseTime.Add(2 * time.Hour),
					LastUpdateTsLte: baseTime.Add(4 * time.Hour),
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 3,
					jobIds:    []string{"J5", "J4", "J3"},
				},
			},
			{
				name: "filter by process_id and status",
				filter: FilterJob{
					ProcessId: "proc1",
					JobStatus: JSDone,
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 2,
					jobIds:    []string{"J6", "J2"},
				},
			},
			{
				name: "filter by nonexistent process_id",
				filter: FilterJob{
					ProcessId: "nonexistent",
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 0,
					jobIds:    []string{},
				},
			},
			{
				name:         "no filters",
				filter:       FilterJob{},
				page:         1,
				itemsPerPage: 100,
				expected: &expected{
					totalRows: 6,
					jobIds:    []string{"J6", "J5", "J4", "J3", "J2", "J1"},
				},
			},
			{
				name: "filter by run type compensation",
				filter: FilterJob{
					RunType: RTCompensation,
				},
				page:         1,
				itemsPerPage: 10,
				expected: &expected{
					totalRows: 1,
					jobIds:    []string{"J4"},
				},
			},
		})
	})
}
