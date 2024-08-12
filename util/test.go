package util

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"testing"
	"time"
)

// RandomString generates simple randomized string using a subset of the ASCII table, specifically ASCII code 32
// (space character) to 126 (~). For testing purposes only.
// http://www.asciitable.com/
func RandomString(length int) string {
	ret := make([]rune, length)
	for i := 0; i < length; i++ {
		ret[i] = rune(32 + rand.Intn(95))
	}
	return string(ret)
}

func RandomTime() time.Time {
	return time.Now().Add(time.Duration(rand.Intn(20000)) * time.Second)
}

func UUIDString() string {
	id, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	return id.String()
}

// AcquireDbLock should contain the logic that acquires the exclusive lock. It returns an unlock function that will
// unlock this particular lock.
type AcquireDbLock func(t *testing.T) func()

// AssertDbLock asserts that the Lock A blocks subsequent attempts of locking until it's unlocked.
func AssertDbLock(t *testing.T, lockA AcquireDbLock) {
	// First acquire the lock.
	unlock := lockA(t)
	defer unlock()

	// Prepare channels.
	funcRun := make(chan struct{})
	gotLock := make(chan struct{})

	// Have another goroutine try to acquire lock.
	go func() {
		// Notify that we've run.
		funcRun <- struct{}{}

		unlock2 := lockA(t)
		defer unlock2()

		gotLock <- struct{}{}
	}()

	// Wait until the other goroutine has run to demonstrate that the other goroutine is blocked until we release.
	<-funcRun

	// The other goroutine will block until we release the lock.
	select {
	case <-time.After(3 * time.Second):
		unlock()
	case <-gotLock:
		assert.Fail(t, "expected lock to block, but another goroutine successfully acquired the lock!")
		return
	}

	// Wait until the other goroutine acquired the lock, then exits, otherwise we'll get deadlock error.
	<-gotLock
}

// AssertDbLockMultipleIndex asserts that Lock A will block Lock B until it is released.
func AssertDbLockMultipleIndex(t *testing.T, lockA AcquireDbLock, lockB AcquireDbLock) {
	// First acquire lock A.
	unlockA := lockA(t)
	defer unlockA()

	// Prepare channels.
	funcRun := make(chan struct{})
	gotLock := make(chan struct{})

	// Have another goroutine try to acquire lock B.
	// We want to test that different queries are also blocked.
	go func() {
		// Notify that we've run.
		funcRun <- struct{}{}

		unlockB := lockB(t)
		defer unlockB()

		gotLock <- struct{}{}
	}()

	// Wait until the other goroutine has run to demonstrate that the other goroutine is blocked until we release.
	<-funcRun

	// The other goroutine will block until we release the lock A.
	select {
	case <-time.After(3 * time.Second):
		unlockA()
	case <-gotLock:
		assert.Fail(t, "expected lock to block, but another goroutine successfully acquired the lock!")
		return
	}

	// Wait until the other goroutine acquired the lock B, then exits, otherwise we'll get deadlock error.
	<-gotLock
}

// AssertDbNotLocked asserts that Lock A does not block Lock B from being acquired.
func AssertDbNotLocked(t *testing.T, lockA AcquireDbLock, lockB AcquireDbLock) {
	// First acquire lock A.
	unlockA := lockA(t)
	defer unlockA()

	// Prepare channels.
	funcRun := make(chan struct{})
	gotLock := make(chan struct{})

	// Have another goroutine try to acquire lock B.
	// We want to test that lock B is unaffected.
	go func() {
		// Notify that we've run.
		funcRun <- struct{}{}

		unlockB := lockB(t)
		defer unlockB()

		gotLock <- struct{}{}
	}()

	// Wait until the other goroutine has run to demonstrate that the other goroutine is blocked until we release.
	<-funcRun

	// The other goroutine is not blocked by lock A.
	select {
	case <-time.After(3 * time.Second):
		assert.Fail(t, "expected lock to not affect each other, but lock B is blocked!")
	case <-gotLock:
		return
	}
}

func Await(timeoutDuration time.Duration, event func() bool) error {
	now := time.Now()
	timeout := time.After(timeoutDuration)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("waiting for an event that did not arrive :(")
		default:
			if event() {
				log.Println("Event received after: " + time.Since(now).String())
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
