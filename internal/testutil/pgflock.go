package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultPgflockPort = 9191
	lockPassword       = "LegacyCodeIsOneWithNoTest"
)

var client = &http.Client{
	Timeout: 15 * time.Minute,
}

// PgTestSuite manages test database lifecycle using pgflock.
type PgTestSuite struct {
	ConnString string
	testName   string
	port       int
}

// NewPgTestSuite creates a new test suite.
func NewPgTestSuite(port int) *PgTestSuite {
	if port == 0 {
		port = defaultPgflockPort
	}
	return &PgTestSuite{port: port}
}

// Setup acquires a database lock from pgflock.
func (s *PgTestSuite) Setup(t *testing.T) {
	testName := GetTestIdentifier(t)
	baseURL := fmt.Sprintf("http://localhost:%d", s.port)

	resp, err := client.Get(baseURL + "/lock?marker=" +
		url.QueryEscape(testName) +
		"&password=" +
		url.QueryEscape(lockPassword))
	if err != nil {
		t.Fatalf("Failed to acquire database lock: %v", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read lock response: %v", err)
	}

	s.ConnString = string(respBytes)
	s.testName = testName
}

// TearDown releases the database lock.
func (s *PgTestSuite) TearDown(t *testing.T) {
	baseURL := fmt.Sprintf("http://localhost:%d", s.port)

	body := bytes.NewBuffer([]byte(s.ConnString))
	resp, err := client.Post(
		baseURL+"/unlock?marker="+
			url.QueryEscape(s.testName)+
			"&password="+
			url.QueryEscape(lockPassword),
		"text/plain",
		body)
	if err != nil {
		t.Logf("Warning: Failed to unlock database: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Logf("Warning: Unlock returned status %v: %s", resp.Status, string(bodyBytes))
	}
}

// Conn returns a new pgx connection.
func (s *PgTestSuite) Conn(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, s.ConnString)
}

// InitSchema creates the ssproc table schema.
func (s *PgTestSuite) InitSchema(t *testing.T, schema, table string) {
	ctx := context.Background()
	conn, err := s.Conn(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer conn.Close(ctx)

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %v.%v (
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
	CREATE INDEX IF NOT EXISTS %v_expired ON %v.%v (process_id, status, start_after_ts, goroutine_lease_expire_ts);
	CREATE INDEX IF NOT EXISTS %v_cleanup_idx ON %v.%v (process_id, status, end_ts);`,
		schema, table,
		table, schema, table,
		table, schema, table)

	_, err = conn.Exec(ctx, query)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
}

// DropTable drops the test table if it exists.
func (s *PgTestSuite) DropTable(t *testing.T, schema, table string) {
	ctx := context.Background()
	conn, err := s.Conn(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer conn.Close(ctx)

	query := fmt.Sprintf(`DROP TABLE IF EXISTS %v.%v CASCADE`, schema, table)
	_, err = conn.Exec(ctx, query)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

// Randomized creates a Selector that returns random items from the list.
func Randomized[T any](items []T) *RandomSelector[T] {
	return &RandomSelector[T]{items: items}
}

// RandomSelector is a simple selector that returns random items.
type RandomSelector[T any] struct {
	items []T
	idx   int
}

// Get returns the next item.
func (s *RandomSelector[T]) Get() T {
	if len(s.items) == 0 {
		var zero T
		return zero
	}
	item := s.items[s.idx%len(s.items)]
	s.idx++
	return item
}

// Count returns the number of items.
func (s *RandomSelector[T]) Count() int {
	return len(s.items)
}
