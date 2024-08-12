package pgtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/rickchristie/ssproc/pg"
	"github.com/rickchristie/ssproc/plugs"
	"github.com/rickchristie/ssproc/util"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const testDbContainerName = "ssproc-testdb-ct"

var logger = plugs.DefaultLogger("DbSuite")

var client = &http.Client{
	Timeout: 15 * time.Minute,
}

func Close() {
	client.CloseIdleConnections()
}

// GetDbSuiteForTest will use file-locking to force only single test usage of each of our database, it will also
// create Suite.DebugCtx with longer transaction timeout so we can debug easily in tests.
func GetDbSuiteForTest(testName string) *Suite {
	resp, err := client.Get("http://localhost:9191/lock")
	if err != nil {
		panic(err)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	connStr := string(bodyBytes)
	return &Suite{
		TestName:   testName,
		Logger:     plugs.DefaultLogger(testName + "@" + connStr),
		DebugCtx:   pg.TimeoutOverrideForTesting(context.Background(), 30*time.Minute),
		ConnString: connStr,
	}
}

func unlockViaDbLocker(connStr string) {
	body := bytes.NewBuffer([]byte(connStr))
	resp, err := client.Post("http://localhost:9191/unlock", "text/plain", body)
	if err != nil {
		panic(err)
	}

	if resp.StatusCode != http.StatusOK {
		panic(fmt.Errorf("unlock connection %v resulting in status code %v", connStr, resp.Status))
	}
}

type Suite struct {
	TestName   string
	Logger     plugs.Logger
	DebugCtx   context.Context
	ConnString string
	testDbKey  string
}

func (s *Suite) Setup(t *testing.T) {
	// We want to drop databases and recreate them because we're using memory file-system for Docker. This means there's
	// a limitation of the max amount of storage size we can have. Dropping databases in postgres deletes the entire
	// database data directory, somehow vacuuming takes longer, and it also doesn't reduce storage usage. With vacuum,
	// running plenty of tests results in "out of space" errors in the container.
	s.recreateDatabases(t)
}

func (s *Suite) recreateDatabases(t *testing.T) {
	config, err := pgx.ParseConfig(s.ConnString)
	assert.Nil(t, err)
	if err != nil {
		panic(err)
	}

	// Drop database.
	// psql -c "CREATE DATABASE tester${i} WITH ENCODING 'UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8' TEMPLATE=template0;"
	//  psql -c "ALTER DATABASE tester${i} OWNER TO tester;"
	//  psql -d "tester${i}" -c "ALTER SCHEMA public OWNER to tester;"
	s.runCmd(
		t, "docker", "exec", testDbContainerName,
		"psql", "-p", "9090", "-U", "postgres",
		"-c", fmt.Sprintf(`DROP DATABASE IF EXISTS %v;`, config.Database),
	)

	// Create the database.
	s.runCmd(
		t, "docker", "exec", testDbContainerName,
		"psql", "-p", "9090", "-U", "postgres",
		"-c", fmt.Sprintf(`CREATE DATABASE %v WITH ENCODING 'UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8' TEMPLATE=template0;`, config.Database),
	)

	// Alter database owner.
	s.runCmd(
		t, "docker", "exec", testDbContainerName,
		"psql", "-p", "9090", "-U", "postgres",
		"-c", fmt.Sprintf(`ALTER DATABASE %v OWNER TO tester;`, config.Database),
	)

	// Alter schema owner.
	s.runCmd(
		t, "docker", "exec", testDbContainerName,
		"psql", "-p", "9090", "-U", "postgres", "-d", config.Database,
		"-c", `ALTER SCHEMA public OWNER to tester;`,
	)
}

func (s *Suite) runCmd(t *testing.T, command string, args ...string) {
	cmd := exec.Command(command, args...)
	cmd.Stdout = cmd.Stderr
	logger.Info("", fmt.Sprintf("Running command: %v", cmd.String()), nil)

	out, err := cmd.Output()
	logger.Info("", fmt.Sprintf("Command output: %v", string(out)), nil)
	assert.Nil(t, err)
	if err != nil {
		var pErr *exec.ExitError
		if errors.As(err, &pErr) {
			logger.Info(
				"", fmt.Sprintf("Error output: %v", string(pErr.Stderr)),
				map[string]any{"command": cmd.String()},
			)
		}
		panic(err)
	}
}

func (s *Suite) Conn(ctx context.Context) *pgx.Conn {
	conn, err := pgx.Connect(ctx, s.ConnString)
	if err != nil {
		// Panic to stop the test immediately.
		panic(util.Err(err.Error(), false))
	}

	return conn
}

func (s *Suite) CloseConn(conn *pgx.Conn) {
	err := conn.Close(context.Background())
	if err != nil {
		// Panic to stop the test immediately.
		panic(util.WrapErr(err, false))
	}
}

func (s *Suite) executeQuery(query string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn := s.Conn(ctx)
	defer s.CloseConn(conn)

	split := strings.Split(query, ";")
	for _, q := range split {
		if q == "" {
			continue
		}

		// Execute the statements.
		_, err := conn.Exec(ctx, q+";")
		if err != nil {
			// Panic to stop the test immediately.
			panic(util.Err(err.Error()+"\n"+q, false))
		}
	}
}

func (s *Suite) TearDown(t *testing.T) {
	unlockViaDbLocker(s.ConnString)
}
