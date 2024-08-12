package main

import (
	"fmt"
	"os"
	"strconv"
)

const maxDatabaseCount = 25

var testDatabases map[string]bool

func init() {
	// Use all maximum databases by default.
	testDbUsage := maxDatabaseCount
	envDbUsage := os.Getenv("TEST_DB_USAGE")
	var err error
	if envDbUsage != "" {
		testDbUsage, err = strconv.Atoi(envDbUsage)
		if err != nil {
			// No use of continuing if we can't parse the environment variable, wrong config!
			panic("Failed to parse TEST_DB_USAGE env variable!")
		}
	}

	testDatabases = map[string]bool{}
	for i := 1; i <= testDbUsage; i++ {
		connString := fmt.Sprintf("postgresql://tester:LegacyCodeIsOneWithNoTest@localhost:9090/tester%v", i)
		testDatabases[connString] = true
	}
}
