package ssproc

import (
	"fmt"
	"github.com/sclevine/spec"
	"github.com/stretchr/testify/assert"
	"rukita.co/main/be/lib/test"
	"testing"
)

func TestSchemaVerifier(t *testing.T) {
	test.Describe(t, StateCreator, func(t *testing.T, Before, After test.Each, Context spec.G, It spec.S, state test.State) {
		s := state.(*State)
		schema := "public"
		table := "schema_verifier"

		type expected struct {
			valid    bool
			errorMsg string
		}

		type testRow struct {
			name             string
			createTableInput string
			expected         *expected
		}

		runRows := func(t *testing.T, rows []*testRow) {
			for _, r := range rows {
				t.Run(r.name, func(t *testing.T) {
					conn := s.Db.Conn(s.Db.DebugCtx)
					defer s.Db.CloseConn(conn)

					_, err := conn.Exec(
						s.Db.DebugCtx,
						fmt.Sprintf(`DROP TABLE IF EXISTS %v.%v CASCADE`, schema, table),
					)
					assert.Nil(t, err)

					_, err = conn.Exec(s.Db.DebugCtx, r.createTableInput)
					assert.Nil(t, err)

					storage, err := NewPgStorage(s.Db.ConnString, schema, table)
					if r.expected.valid == false {
						assert.NotNil(t, err)
						assert.Nil(t, storage)
						assert.Equal(t, r.expected.errorMsg, err.Error())
						return
					}

					// Else schema is valid.
					assert.Nil(t, err)
					assert.NotNil(t, storage)
				})
			}
		}

		It("validates and reject if table schema is not as expected", func() {
			runRows(t, []*testRow{
				{
					name: "schema valid, no errors",
					createTableInput: `CREATE TABLE public.schema_verifier (
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						valid: true,
					},
				},

				// Failure cases, column does not exist.
				{
					name: "column does not exist",
					createTableInput: `CREATE TABLE public.schema_verifier (
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
						comp_count int NOT NULL DEFAULT 0,
						created_ts timestamp with time zone NOT NULL,
						start_after_ts timestamp with time zone NOT NULL,
						started_ts timestamp with time zone,
						end_ts timestamp with time zone,
						last_update_ts timestamp with time zone NOT NULL
					);
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public.schema_verifier: column exec_count does not exist in table",
					},
				},

				// Failure cases, indexes.
				{
					name: "reject when no indexes at all",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL,
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
					);`,
					expected: &expected{
						errorMsg: "public.schema_verifier: no index found! expected 1 primary key index",
					},
				},
				{
					name: "reject when index is not primary key",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL UNIQUE,
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public.schema_verifier: index_name schema_verifier_pkey not found",
					},
				},
				{
					name: "reject when there are multiple indexes",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL PRIMARY KEY,
						job_data text NOT NULL DEFAULT '',
						process_id text NOT NULL,
						goroutine_id varchar(128) NOT NULL DEFAULT '',
						goroutine_heart_beat_ts timestamp with time zone,
						goroutine_lease_expire_ts timestamp with time zone,
						status varchar(64) NOT NULL DEFAULT 'ready' UNIQUE,
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public.schema_verifier: multiple indexes found, expected two indexes",
					},
				},
				{
					name: "reject when index name is the same but not the correct index",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL,
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);
					CREATE INDEX schema_verifier_pkey ON public.schema_verifier (job_id);`,
					expected: &expected{
						errorMsg: "public.schema_verifier: expected index_name schema_verifier_pkey to be: CREATE UNIQUE INDEX schema_verifier_pkey ON public.schema_verifier USING btree (job_id), found: CREATE INDEX schema_verifier_pkey ON public.schema_verifier USING btree (job_id)",
					},
				},

				// Failure cases, default values.
				{
					name: "no default value provided for status",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL PRIMARY KEY,
						job_data text NOT NULL DEFAULT '',
						process_id text NOT NULL,
						goroutine_id varchar(128) NOT NULL DEFAULT '',
						goroutine_heart_beat_ts timestamp with time zone,
						goroutine_lease_expire_ts timestamp with time zone,
						status varchar(64) NOT NULL,
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public:schema_verifier expected column status default value to be 'ready'::character varying, found ",
					},
				},
				{
					name: "default value incorrect",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL PRIMARY KEY,
						job_data text NOT NULL DEFAULT '',
						process_id text NOT NULL,
						goroutine_id varchar(128) NOT NULL DEFAULT '',
						goroutine_heart_beat_ts timestamp with time zone,
						goroutine_lease_expire_ts timestamp with time zone,
						status varchar(64) NOT NULL DEFAULT 'ready1',
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public:schema_verifier expected column status default value to be 'ready'::character varying, found 'ready1'::character varying",
					},
				},
				{
					name: "no default value provided",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL PRIMARY KEY,
						job_data text NOT NULL DEFAULT '',
						process_id text NOT NULL,
						goroutine_id varchar(128) NOT NULL,
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public:schema_verifier expected column goroutine_id default value to be ''::character varying, found ",
					},
				},

				// Failure cases, nullability.
				{
					name: "expected not null, got null",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL PRIMARY KEY,
						job_data text DEFAULT '',
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public.schema_verifier: expected column job_data nullable to be false, found true",
					},
				},
				{
					name: "expected null, got not null",
					createTableInput: `CREATE TABLE public.schema_verifier (
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
						started_ts timestamp with time zone NOT NULL,
						end_ts timestamp with time zone,
						last_update_ts timestamp with time zone NOT NULL
					);
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public.schema_verifier: expected column started_ts nullable to be true, found false",
					},
				},

				// Failure cases, incorrect max length.
				{
					name: "incorrect max length",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL PRIMARY KEY,
						job_data text NOT NULL DEFAULT '',
						process_id text NOT NULL,
						goroutine_id varchar(127) NOT NULL DEFAULT '',
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public:schema_verifier expected column goroutine_id char max length to be 128, found 127",
					},
				},
				{
					name: "incorrect max length",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL PRIMARY KEY,
						job_data text NOT NULL DEFAULT '',
						process_id text NOT NULL,
						goroutine_id varchar(129) NOT NULL DEFAULT '',
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public:schema_verifier expected column goroutine_id char max length to be 128, found 129",
					},
				},

				// Failure cases, data type.
				{
					name: "incorrect data type",
					createTableInput: `CREATE TABLE public.schema_verifier (
						job_id text NOT NULL PRIMARY KEY,
						job_data varchar(1024) NOT NULL DEFAULT '',
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
					CREATE INDEX schema_verifier_expired ON public.schema_verifier (process_id, status, start_after_ts, goroutine_lease_expire_ts);`,
					expected: &expected{
						errorMsg: "public.schema_verifier: expected column job_data dataType to be text, found character varying",
					},
				},
			})
		})

		It("does not return error when table is valid", func() {
			s.InitValidDb(t, schema, table)
			storage, err := NewPgStorage(s.Db.ConnString, schema, table)
			assert.Nil(t, err)
			assert.NotNil(t, storage)
		})
	})
}
