package ssproc

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"rukita.co/main/be/accessor/db/pg"
	"rukita.co/main/be/accessor/db/pg/pgtest"
	"rukita.co/main/be/lib/mend"
	"rukita.co/main/be/lib/test"
	"rukita.co/main/be/lib/tr"
	"testing"
)

const (
	// TestLeak should be set to true and run manually each time we modify ssproc, this makes sure our modifications
	// doesn't introduce goroutine leaks. This is important, it really helps us catch leaks. Previously there was
	// a bug where when there's an error, context/connection was not cancelled, this helps us catch it.
	TestLeak = false
)

var _ test.State = (*State)(nil)

type State struct {
	Db      *pgtest.Suite
	h       *PgTestHelper
	Storage *PgStorage
}

func (s *State) Setup(t *testing.T) {
	// This locks the database from usage by other tests in all processes.
	s.Db = pgtest.GetDbSuiteForTest("ssproc")
	s.Db.Setup(t)
	s.Storage = s.InitPgStorage(t, "public", "ssproc_test_main")
	s.h = &PgTestHelper{
		T:         t,
		Ctx:       s.Db.DebugCtx,
		PgStorage: s.Storage,
	}
}

func (s *State) TearDown(t *testing.T) {
	// Need to call the tear down, otherwise the database won't be usable in other tests.
	defer s.Db.TearDown(t)
}

func (s *State) NewTx(t *testing.T, name string) *pg.ConnTxHelper {
	tx, err := pg.NewConnTxHelper(
		s.h.Ctx,
		&tr.Trace{TraceId: name},
		s.Db.ConnString,
		mend.NewZerologLogger(name),
		false,
	)
	assert.Nil(t, err)

	return tx
}

func (s *State) InitPgStorage(t *testing.T, schema, table string) *PgStorage {
	s.InitValidDb(t, schema, table)
	ret, err := NewPgStorage(s.Db.ConnString, schema, table)
	assert.Nil(t, err)
	return ret
}

func (s *State) InitValidDb(t *testing.T, schema, table string) {
	conn := s.Db.Conn(s.Db.DebugCtx)
	defer s.Db.CloseConn(conn)

	tx, err := conn.Begin(s.Db.DebugCtx)
	assert.Nil(t, err)

	defer func() {
		err = tx.Rollback(s.Db.DebugCtx)
		if err != nil {
			assert.Equal(t, true, errors.Is(err, pgx.ErrTxClosed))
		}
	}()

	_, err = tx.Exec(
		s.Db.DebugCtx,
		fmt.Sprintf(`CREATE TABLE %v.%v (
				job_id text NOT NULL PRIMARY KEY,
				job_data text NOT NULL DEFAULT '',
				process_id text NOT NULL,
				goroutine_id varchar(128) NOT NULL DEFAULT '',
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
			CREATE INDEX %v_expired ON %v (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
			schema, table,
			table, table,
		),
	)
	assert.Nil(t, err)

	err = tx.Commit(s.Db.DebugCtx)
	assert.Nil(t, err)
}

func StateCreator() test.State {
	return &State{}
}
