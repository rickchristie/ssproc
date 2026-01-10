// Package testutil provides testing utilities for ssproc.
package testutil

import (
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// GetTestIdentifier returns a unique identifier for the test.
func GetTestIdentifier(t *testing.T) string {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		return t.Name()
	}
	parentDir := filepath.Dir(file)
	pkg := filepath.Base(parentDir)
	grandparentDir := filepath.Dir(parentDir)
	grandparent := filepath.Base(grandparentDir)

	name := t.Name()
	if idx := strings.Index(name, "/"); idx != -1 {
		name = name[:idx]
	}

	if grandparent != "" && grandparent != "." && grandparent != "/" {
		return grandparent + "/" + pkg + "/" + name
	}
	return pkg + "/" + name
}

// RandomString generates a random string of the given length.
func RandomString(length int) string {
	ret := make([]rune, length)
	for i := 0; i < length; i++ {
		ret[i] = rune(32 + rand.Intn(95))
	}
	return string(ret)
}

// RandomAlphaNum generates a random alphanumeric string.
func RandomAlphaNum(length int) string {
	strRunes := []rune("1234567890ABCDEFGHIJLKMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	ret := make([]rune, length)
	for i := 0; i < length; i++ {
		ret[i] = strRunes[rand.Intn(len(strRunes))]
	}

	return string(ret)
}

// Await waits for the event to return true, with timeout.
func Await(timeoutDuration time.Duration, event func() bool) error {
	now := time.Now()
	timeout := time.NewTimer(timeoutDuration)
	for {
		select {
		case <-timeout.C:
			return fmt.Errorf("waiting for an event that did not arrive")
		default:
			if event() {
				timeout.Stop()
				log.Println("Event received after: " + time.Since(now).String())
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
