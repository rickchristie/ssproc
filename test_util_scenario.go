package ssproc

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type TestJobRow struct {
	Id   string
	Data string

	// These fields are optional and has defaults.
	GoroutineId            string
	GoroutineHeartBeatTs   time.Time
	GoroutineLeaseExpireTs time.Time
	Status                 JobStatus
	NextSubprocess         int
	ExecCount              int
	CreatedTs              time.Time
	StartedTs              time.Time
	EndTs                  time.Time
	LastUpdateTs           time.Time
}

func (h *PgTestHelper) TestJobRows(t *testing.T, rows []*TestJobRow) map[string]*Job {
	for _, r := range rows {
		if r.Status == "" {
			r.Status = JSReady
		}
		if r.CreatedTs.IsZero() {
			r.CreatedTs = time.Now()
		}
		if r.LastUpdateTs.IsZero() {
			r.LastUpdateTs = time.Now()
		}
	}

	// Start connection, close when function exits.
	conn, err := pgx.Connect(h.Ctx, h.PgStorage.connStr)
	assert.Nil(t, err)
	defer func(conn *pgx.Conn) {
		err = conn.Close(h.Ctx)
		assert.Nil(t, err)
	}(conn)

	// Start transaction, rollback when function exits.
	tx, err := conn.Begin(h.Ctx)
	assert.Nil(t, err)
	defer h.PgStorage.rollback(h.Ctx, func() {}, tx)

	ret := make(map[string]*Job)
	for _, r := range rows {
		query := fmt.Sprintf(
			`INSERT INTO 
						%v.%v (
							job_id, job_data, goroutine_id, goroutine_heart_beat_ts, status, 
							next_subprocess, exec_count, created_ts, started_ts, 
							end_ts, last_update_ts, goroutine_lease_expire_ts
						)
						VALUES (
							$1, $2, $3, $4, $5, 
							$6, $7, $8, $9, 
							$10, $11, $12
						)`,
			h.PgStorage.schema,
			h.PgStorage.table,
		)
		_, err := tx.Exec(
			h.Ctx, query,

			r.Id, r.Data, r.GoroutineId, h.PgStorage.tsInput(r.GoroutineHeartBeatTs), r.Status,
			r.NextSubprocess, r.ExecCount, h.PgStorage.tsInput(r.CreatedTs), h.PgStorage.tsInput(r.StartedTs),
			h.PgStorage.tsInput(r.EndTs), h.PgStorage.tsInput(r.LastUpdateTs),
			h.PgStorage.tsInput(r.GoroutineLeaseExpireTs),
		)
		assert.Nil(t, err)

		ret[r.Id], _ = h.GetJob(t, r.Id)
	}

	err = tx.Commit(h.Ctx)
	assert.Nil(t, err)

	return ret
}
