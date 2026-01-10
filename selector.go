package ssproc

// Selector defines the interface for selecting endpoints from a pool of
// available targets. Implementations should be thread-safe and efficient
// for concurrent access.
type Selector[T any] interface {
	// Get returns an endpoint from the available pool.
	// This method must be safe for concurrent use across multiple goroutines.
	Get() T

	// Count returns the total number of available endpoints.
	Count() int
}

// StaticSelector is a simple selector that always returns the same value.
type StaticSelector[T any] struct {
	value T
}

// NewStaticSelector creates a selector that always returns the same value.
func NewStaticSelector[T any](value T) *StaticSelector[T] {
	return &StaticSelector[T]{value: value}
}

// Get returns the static value.
func (s *StaticSelector[T]) Get() T {
	return s.value
}

// Count returns 1.
func (s *StaticSelector[T]) Count() int {
	return 1
}

// RoundRobinSelector cycles through values in round-robin order.
type RoundRobinSelector[T any] struct {
	values []T
	idx    int
}

// NewRoundRobinSelector creates a selector that cycles through values.
func NewRoundRobinSelector[T any](values []T) *RoundRobinSelector[T] {
	return &RoundRobinSelector[T]{values: values}
}

// Get returns the next value in round-robin order.
func (s *RoundRobinSelector[T]) Get() T {
	if len(s.values) == 0 {
		var zero T
		return zero
	}
	val := s.values[s.idx%len(s.values)]
	s.idx++
	return val
}

// Count returns the number of values.
func (s *RoundRobinSelector[T]) Count() int {
	return len(s.values)
}
