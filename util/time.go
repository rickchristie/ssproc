package util

import "time"

// Time is a wrapping interface for accessing global time. We wrap access to time so it can be easily tested.
type Time interface {
	Now() time.Time
	NewDateToday() time.Time
	NewDateStartOfThisMonth() time.Time
}

var _ Time = (*globalTime)(nil)
var _ Time = (*MockTime)(nil)

type globalTime struct {
	Location *time.Location
}

func NewGlobalTime(location *time.Location) Time {
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

func (g *globalTime) NewDateToday() time.Time {
	ti := time.Now()
	return time.Date(
		ti.Year(), ti.Month(), ti.Day(),
		0, 0, 0, 0,
		g.Location,
	)
}

func (g *globalTime) NewDateStartOfThisMonth() time.Time {
	ti := time.Now()
	return time.Date(
		ti.Year(), ti.Month(), 1,
		0, 0, 0, 0, g.Location,
	)
}

type MockTime struct {
	NowStub                     func() time.Time
	NewDateTodayStub            func() time.Time
	NewDateStartOfThisMonthStub func() time.Time
}

func (m *MockTime) Now() time.Time {
	return m.NowStub()
}

func (m *MockTime) NewDateToday() time.Time {
	return m.NewDateTodayStub()
}

func (m *MockTime) NewDateStartOfThisMonth() time.Time {
	return m.NewDateStartOfThisMonthStub()
}
