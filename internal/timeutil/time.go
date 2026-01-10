// Package timeutil provides time utilities for ssproc.
package timeutil

import (
	"sync/atomic"
	"time"
)

// Time is a wrapping interface for accessing global time.
type Time interface {
	Now() time.Time
}

var _ Time = (*globalTime)(nil)
var _ Time = (*MockTime)(nil)
var _ Time = (*MockGlobalTime)(nil)

type globalTime struct {
	Location *time.Location
}

// NewGlobalTime creates a new global time with the given location.
func NewGlobalTime(location *time.Location) Time {
	if location == nil {
		location = time.UTC
	}
	g := &globalTime{
		Location: location,
	}
	// Check that we can generate time.
	g.Now()
	return g
}

func (g *globalTime) Now() time.Time {
	return time.Now().In(g.Location)
}

// MockGlobalTime is a mock time that can be set to a specific time.
type MockGlobalTime struct {
	Location *time.Location
	value    atomic.Value
}

// NewMockGlobalTime creates a mock time with the given initial time.
func NewMockGlobalTime(location *time.Location, initialTime time.Time) *MockGlobalTime {
	if location == nil {
		location = time.UTC
	}
	m := &MockGlobalTime{
		Location: location,
	}
	m.SetNow(initialTime.In(location))
	return m
}

// SetNow sets the current time.
func (g *MockGlobalTime) SetNow(t time.Time) {
	g.value.Store(t)
}

// Now returns the mock time.
func (g *MockGlobalTime) Now() time.Time {
	return g.value.Load().(time.Time).In(g.Location)
}

// MockTime is a stub-based mock time for testing.
type MockTime struct {
	NowStub func() time.Time
}

// Now returns the mock time.
func (m *MockTime) Now() time.Time {
	return m.NowStub()
}
