package job

import (
	"github.com/rickchristie/ssproc/plugs"
	"time"
)

type noopPoolMock struct {
}

func (p *noopPoolMock) Submit(func()) {
}

func (p *noopPoolMock) StopAndWait() {
}

func NewNoopNamedWorkerPoolMock(poolId string) *NamedWorkerPool {
	namedWorkerPool := NamedWorkerPool{
		logger:       plugs.DefaultLogger("NamedPool"),
		poolId:       poolId,
		runningTasks: make(map[string]struct{}),
		idleTime:     time.Now(),
		pool:         &noopPoolMock{},
	}
	return &namedWorkerPool
}
