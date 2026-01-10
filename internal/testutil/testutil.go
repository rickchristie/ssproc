// Package testutil provides testing utilities for ssproc.
package testutil

import (
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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

// UUIDString generates a new UUID string.
func UUIDString() string {
	return uuid.New().String()
}

// Counter is a thread-safe counter that can be used to safely count event occurrences in testing.
type Counter struct {
	mux    sync.Mutex
	events map[string]int
}

// Add will increment event that is identified by the given args.
func (c *Counter) Add(args ...interface{}) {
	id := Identify(args...)

	c.mux.Lock()
	defer c.mux.Unlock()
	if c.events == nil {
		c.events = make(map[string]int)
	}
	c.events[id] = c.events[id] + 1
}

// Get will get the number of times the event is called.
func (c *Counter) Get(args ...interface{}) int {
	id := Identify(args...)

	c.mux.Lock()
	defer c.mux.Unlock()
	return c.events[id]
}

// EventTypeCount return the types of events that's counted.
func (c *Counter) EventTypeCount() int {
	c.mux.Lock()
	defer c.mux.Unlock()
	return len(c.events)
}

// TotalEvents return the total number of all events.
func (c *Counter) TotalEvents() int {
	c.mux.Lock()
	defer c.mux.Unlock()

	ret := 0
	for _, count := range c.events {
		ret += count
	}
	return ret
}

// Reset will reset all counts back to zero.
func (c *Counter) Reset() {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.events = make(map[string]int)
}

// Identify will generate a string identifier representing the given list of arguments.
func Identify(args ...interface{}) string {
	builder := strings.Builder{}
	for _, arg := range args {
		builder.WriteString(";")
		builder.WriteString(strings.ReplaceAll(fmt.Sprintf("%T", arg), ";", "\\;"))
		builder.WriteString(";")
		builder.WriteString(strings.ReplaceAll(fmt.Sprintf("%v", arg), ";", "\\;"))
		builder.WriteString(";")
	}
	return builder.String()
}
