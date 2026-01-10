package ssproc

import (
	"context"
	"encoding/json"
)

// TestJobData is a simple job data for testing.
type TestJobData struct {
	ID      string `json:"id"`
	Counter int    `json:"counter"`
}

func (d TestJobData) GetJobId() string {
	return d.ID
}

// TestProcess is a simple process for testing.
type TestProcess struct {
	id           string
	subprocesses []*Subprocess[TestJobData]
}

func NewTestProcess(id string) *TestProcess {
	return &TestProcess{
		id: id,
		subprocesses: []*Subprocess[TestJobData]{
			{
				Transaction: func(ctx context.Context, goroutineId string, data TestJobData, update JobDataUpdater[TestJobData]) SubprocessResult {
					data.Counter++
					err := update(data)
					if err != nil {
						return SRFailed
					}
					return SRSuccess
				},
			},
		},
	}
}

func (p *TestProcess) Id() string {
	return p.id
}

func (p *TestProcess) GetSubprocesses() []*Subprocess[TestJobData] {
	return p.subprocesses
}

func (p *TestProcess) Serialize(data TestJobData) (string, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *TestProcess) Deserialize(serialized string) (TestJobData, error) {
	var data TestJobData
	err := json.Unmarshal([]byte(serialized), &data)
	return data, err
}
