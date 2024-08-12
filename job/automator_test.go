package job

import (
	"github.com/rickchristie/ssproc/plugs"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"testing"
	"time"
)

type InsertValueAutomator struct {
	Automator
	sl        []int
	wantPanic bool
}

func (i *InsertValueAutomator) StartInsertValue() {
	i.StartInterval(func() {
		if i.wantPanic {
			i.wantPanic = false
			panic("Hoy hoy hoy...!")
		}
		i.sl = append(i.sl, 1)
	}, 100*time.Millisecond)
}

// TODO: There's probably a better way to test this, without relying on time.
func TestAutomator(t *testing.T) {
	defer goleak.VerifyNone(t)

	logger := plugs.DefaultLogger("testAutomator")
	iv := InsertValueAutomator{
		Automator: NewAutomator("test", false, logger),
		sl:        make([]int, 0),
	}

	// Test started.
	iv.StartInsertValue()
	time.Sleep(1 * time.Second)
	assert.Equal(t, true, len(iv.sl) > 5)

	// Test can be stopped.
	iv.Stop()
	firstStartLength := len(iv.sl)
	time.Sleep(1 * time.Second)
	assert.Equal(t, firstStartLength, len(iv.sl))

	// Test can be restarted.
	iv.StartInsertValue()
	time.Sleep(1 * time.Second)
	assert.Equal(t, true, len(iv.sl)-firstStartLength > 5)

	// Test that panic is recovered and execution is stopped.
	iv.wantPanic = true
	time.Sleep(200 * time.Millisecond)
	secondStartLength := len(iv.sl)
	time.Sleep(1 * time.Second)
	assert.Equal(t, secondStartLength, len(iv.sl))

	// Test can be restarted after panic.
	iv.StartInsertValue()
	time.Sleep(1 * time.Second)
	assert.Equal(t, true, len(iv.sl)-secondStartLength > 5)

	iv.RestartOnPanic = true
	iv.wantPanic = true
	time.Sleep(200 * time.Millisecond)
	thirdStartLength := len(iv.sl)
	time.Sleep(1 * time.Second)
	assert.Equal(t, true, len(iv.sl)-thirdStartLength > 5)

	iv.Stop()
}
