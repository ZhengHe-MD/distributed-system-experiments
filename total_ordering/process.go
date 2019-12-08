package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type ConsumptionLog struct {
	TS  int
	PID int
}

func (m ConsumptionLog) String() string {
	return fmt.Sprintf("[%d %d]", m.PID, m.TS)
}

type Process struct {
	mu           sync.Mutex
	id           int              // processID
	ts           int              // logical lock
	holding      bool             // signals whether this process is holding resource
	pts          map[int]int      // keep the largest timestamp received from other processes
	requestQueue []Request        // resource requests queue, sorted by the total order defined in paper
	logs         []ConsumptionLog // resource consuming logs, used to check this algorithm
	environment  *DistributedEnvironment
}

func NewProcess(id int, env *DistributedEnvironment) *Process {
	rand.Seed(time.Now().Unix())

	pts := make(map[int]int)
	for i := 1; i <= len(env.Top); i++ {
		pts[i] = 0
	}

	return &Process{
		id:          id,
		pts:         pts,
		environment: env,
	}
}

func (m *Process) Run(ctx context.Context) {
	go m.startRandomRequestLoop(ctx)
	go m.checkAndConsumeResourceLoop(ctx)
	go m.checkAndReleaseResourceLoop(ctx)
}

func (m *Process) startRandomRequestLoop(ctx context.Context) {
	for {
		m.mu.Lock()

		if rand.Float64() < RequestSendingProb {
			req := Request{
				Type:    RequestTypeConsume,
				FromPID: m.id,
				TS:      m.ts,
			}

			m._enqueue(ctx, req)

			for pid := range m.environment.Top {
				if pid == m.id {
					continue
				}
				m.__printf("send req %v to P[%d]", req, pid)
				go m.sendConsumeRequest(ctx, pid, req)
			}
			// event: send resource request
			m._tick(ctx, 0)
		}

		m.mu.Unlock()
		time.Sleep(SendRandomRequestInterval)
	}
}

func (m *Process) sendConsumeRequest(ctx context.Context, pid int, req Request) {
	res, err := m.environment.SendRequestToProcess(ctx, pid, req)

	m.mu.Lock()
	if err != nil {
		// NOTE: code should not reach here
		m.__panicf("err %v res %v", err, res)
	} else {
		// event: receive response
		m._tick(ctx, res.TS)
		m._updatePTSByResponse(ctx, res)
		m.__printf("receive ack %v", res)
	}
	m.mu.Unlock()
}

func (m *Process) checkAndConsumeResourceLoop(ctx context.Context) {
	for {
		time.Sleep(CheckAndConsumeResourceInterval)
		m.mu.Lock()

		if len(m.requestQueue) == 0 {
			m.mu.Unlock()
			continue
		}

		req := m.requestQueue[0]
		if req.FromPID == m.id && m._isAbleToUpdateResource(ctx, req.TS) && !m.holding && m.environment.Resource.TryLock() {
			m.__printf("consume resource")
			m.holding = true

			m.logs = append(m.logs, ConsumptionLog{
				TS:  req.TS,
				PID: req.FromPID,
			})

			for pid := range m.environment.Top {
				if pid == m.id {
					continue
				}
				rreq := Request{
					Type:    RequestTypeRelease,
					FromPID: m.id,
					TS:      m.ts,
				}
				m.__printf("send release request %v to P[%d]", req, pid)
				go m.sendReleaseRequest(ctx, pid, rreq)
			}

			// event: send release request
			m._tick(ctx, 0)
		}
		m.mu.Unlock()
	}
}

func (m *Process) sendReleaseRequest(ctx context.Context, pid int, req Request) {
	res, err := m.environment.SendRequestToProcess(ctx, pid, req)

	m.mu.Lock()
	// sanity check
	if err != nil {
		// NOTE: should not reach here
		m.__panicf("err %v res %v", err, res)
	} else {
		// event: receive release response
		m._tick(ctx, res.TS)
		m._updatePTSByResponse(ctx, res)
	}
	m.mu.Unlock()
}

func (m *Process) checkAndReleaseResourceLoop(ctx context.Context) {
	for {
		m.mu.Lock()

		if len(m.requestQueue) > 0 && m.holding {
			var req Request
			var idx int
			for i, request := range m.requestQueue {
				if request.FromPID == m.id {
					req = request
					idx = i
					break
				}
			}

			if req.FromPID == m.id && m._isAbleToUpdateResource(ctx, req.TS) {
				m.requestQueue = append(m.requestQueue[:idx], m.requestQueue[idx+1:]...)
				m.__printf("release resource")
				m.holding = false
				m.environment.Resource.Unlock()
			}
		}

		m.mu.Unlock()

		time.Sleep(CheckAndReleaseResourceInterval)
	}
}

func (m *Process) HandleRequest(ctx context.Context, req Request) Response {
	m.mu.Lock()
	defer m.mu.Unlock()

	// event: receive new request
	m._tick(ctx, req.TS)

	switch req.Type {
	case RequestTypeConsume:
		return m._handleConsumeRequest(ctx, req)
	case RequestTypeRelease:
		return m._handleReleaseRequest(ctx, req)
	default:
		return m._handle404(ctx, req)
	}
}

func (m *Process) _handleConsumeRequest(ctx context.Context, req Request) Response {
	m._enqueue(ctx, req)
	m.__printf("ack request %v", req)
	return Response{
		Type:    ResponseTypeAckConsume,
		FromPID: m.id,
		TS:      m.ts,
	}
}

func (m *Process) _handleReleaseRequest(ctx context.Context, req Request) Response {
	for i, request := range m.requestQueue {
		// NOTE: find the earliest resource request from req.PID
		if request.FromPID == req.FromPID {
			m.requestQueue = append(m.requestQueue[:i], m.requestQueue[i+1:]...)
			m.logs = append(m.logs, ConsumptionLog{
				TS:  request.TS,
				PID: request.FromPID,
			})

			m.__printf("ack release %v", req)
			return Response{
				FromPID: m.id,
				TS:      m.ts,
				Type:    ResponseTypeAckRelease,
			}
		}
	}
	log.Panicf("Process %d: err req %v not found in requestQueue at %v", m.id, req, m.ts)
	// NOTE: unreachable, make ide happy
	return Response{}
}

func (m *Process) _handle404(ctx context.Context, req Request) Response {
	m.__panicf("err unknown request type %v", req.Type)
	// NOTE: unreachable, make ide happy
	return Response{}
}

// NOTE: thread-unsafe
func (m *Process) _tick(ctx context.Context, externalTimestamp int) {
	later := m.ts
	if externalTimestamp > m.ts {
		later = externalTimestamp
	}

	m.ts = later + 1
	return
}

func (m *Process) _enqueue(ctx context.Context, req Request) {
	m.requestQueue = append(m.requestQueue, req)
	sort.SliceStable(m.requestQueue, func(i, j int) bool {
		if m.requestQueue[i].TS != m.requestQueue[j].TS {
			return m.requestQueue[i].TS < m.requestQueue[j].TS
		}
		return m.requestQueue[i].FromPID < m.requestQueue[j].FromPID
	})
}

func (m *Process) _updatePTSByResponse(ctx context.Context, res Response) {
	if res.TS > m.pts[res.FromPID] {
		m.pts[res.FromPID] = res.TS
	}
}

func (m *Process) _updatePTSByRequest(ctx context.Context, req Request) {
	if req.TS > m.pts[req.FromPID] {
		m.pts[req.FromPID] = req.TS
	}
}

func (m *Process) _isAbleToUpdateResource(ctx context.Context, ts int) bool {
	for pid, pts := range m.pts {
		if pid == m.id {
			continue
		}
		if pts <= ts {
			return false
		}
	}
	return true
}

func (m *Process) __printf(format string, args ...interface{}) {
	log.Printf(m.__prefixLogFormat(format), args...)
}

func (m *Process) __panicf(format string, args ...interface{}) {
	log.Panicf(m.__prefixLogFormat(format), args...)
}

func (m *Process) __prefixLogFormat(format string) string {
	return fmt.Sprintf("P[%d] T[%d] %s", m.id, m.ts, format)
}

// NOTE: for debugging purpose
func (m *Process) __printInternalState() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.__printf("requestQueue %v", m.requestQueue)
	m.__printf("logs %v", m.logs)
}
