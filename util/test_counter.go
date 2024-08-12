package util

import (
	"fmt"
	"strings"
	"sync"
)

// Counter is a thread-safe counter that can be used to safely count event occurrences in testing. Each event must be
// identifiable with an identifier. The easiest way is to just concat all arguments.
type Counter struct {
	mux    sync.Mutex
	events map[string]int
}

// Add will increment event that is identified by the given args. Pass only primitive (non-structs, non-pointers)
// arguments (ordering matters).
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

// Identify will generate a string identifier representing the given list of arguments. Basically:
//
//	Identify(A,B) == Identify(A,B)
//
// Currently Identify only works with primitive variables. No thought is spent on efficiency as this is only used for
// testing purposes.
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
