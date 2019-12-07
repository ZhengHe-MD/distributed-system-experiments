package main

import (
	"context"
	"errors"
	"time"
)

var (
	ErrProcessNotFound = errors.New("process not found")
)

type DistributedEnvironment struct {
	Top      map[int]*Process
	Resource *Resource
}

func (m *DistributedEnvironment) SendRequestToProcess(ctx context.Context, pid int, req Request) (res Response, err error) {
	p, ok := m.Top[pid]
	if !ok {
		err = ErrProcessNotFound
		return
	}
	res = p.HandleRequest(ctx, req)
	return
}

func (m *DistributedEnvironment) IsAbleToUpdateResource(records []int) bool {
	if len(records) != len(m.Top) {
		return false
	}

	for _, record := range records {
		if _, ok := m.Top[record]; !ok {
			return false
		}
	}
	return true
}

func (m *DistributedEnvironment) AddProcess(p *Process) {
	m.Top[p.id] = p
}

func main() {
	processNum := 5

	disEnv := &DistributedEnvironment{
		Top:      make(map[int]*Process),
		Resource: NewResource(),
	}

	// TODO: cancel
	ctx := context.Background()

	for i := 1; i <= processNum; i++ {
		p := NewProcess(i, disEnv)
		disEnv.AddProcess(p)
	}

	for _, p := range disEnv.Top {
		p.Run(ctx)
	}

	time.Sleep(5 * time.Second)

	for !disEnv.Resource.TryLock() {
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)

	for _, p := range disEnv.Top {
		p.__printInternalState()
	}
}
