package ssproc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"

	"github.com/rickchristie/ssproc/internal/testutil"
)

// State provides test state management.
type State struct {
	Db      *testutil.PgTestSuite
	h       *PgTestHelper
	Storage *PgStorage
}

// Setup initializes the test state.
func (s *State) Setup(t *testing.T) {
	s.Db = testutil.NewPgTestSuite(9191)
	s.Db.Setup(t)
	s.Storage = s.InitPgStorage(t, "public", "ssproc_test_main")
	s.h = &PgTestHelper{
		T:         t,
		Ctx:       context.Background(),
		PgStorage: s.Storage,
	}
}

// TearDown cleans up the test state.
func (s *State) TearDown(t *testing.T) {
	defer s.Db.TearDown(t)
}

// InitPgStorage creates a new PgStorage for testing.
func (s *State) InitPgStorage(t *testing.T, schema, table string) *PgStorage {
	s.InitValidDb(t, schema, table)
	ret, err := NewPgStorage(PgStorageConfig{
		ConnStrSelector: testutil.Randomized([]string{s.Db.ConnString}),
		Schema:          schema,
		Table:           table,
	})
	assert.Nil(t, err)
	return ret
}

// InitValidDb creates the ssproc table.
func (s *State) InitValidDb(t *testing.T, schema, table string) {
	ctx := context.Background()
	conn, err := s.Db.Conn(ctx)
	assert.Nil(t, err)
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	assert.Nil(t, err)

	defer func() {
		err = tx.Rollback(ctx)
		if err != nil {
			assert.Equal(t, true, errors.Is(err, pgx.ErrTxClosed))
		}
	}()

	_, err = tx.Exec(
		ctx,
		fmt.Sprintf(`CREATE TABLE %v.%v (
			job_id text NOT NULL PRIMARY KEY,
			job_data text NOT NULL DEFAULT '',
			process_id text NOT NULL,
			goroutine_id varchar(512) NOT NULL DEFAULT '',
			goroutine_heart_beat_ts timestamp with time zone,
			goroutine_lease_expire_ts timestamp with time zone,
			status varchar(64) NOT NULL DEFAULT 'ready',
			next_subprocess int NOT NULL DEFAULT 0,
			run_type VARCHAR(64) NOT NULL DEFAULT 'normal',
			goroutine_ids text[] NOT NULL DEFAULT '{}'::text[],
			exec_count int NOT NULL DEFAULT 0,
			comp_count int NOT NULL DEFAULT 0,
			created_ts timestamp with time zone NOT NULL,
			start_after_ts timestamp with time zone NOT NULL,
			started_ts timestamp with time zone,
			end_ts timestamp with time zone,
			last_update_ts timestamp with time zone NOT NULL
		);
		CREATE INDEX %v_expired ON %v.%v (process_id, status, start_after_ts, goroutine_lease_expire_ts);
		CREATE INDEX %v_cleanup_idx on %v.%v (process_id, status, end_ts);`,
			schema, table,
			table, schema, table,
			table, schema, table,
		),
	)
	assert.Nil(t, err)

	err = tx.Commit(ctx)
	assert.Nil(t, err)
}

// StateCreator returns a new State for testing.
func StateCreator() *State {
	return &State{}
}
