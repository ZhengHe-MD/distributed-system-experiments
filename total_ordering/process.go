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
	TS        int
	PID       int
	ConsumeID int
}

func (m ConsumptionLog) String() string {
	return fmt.Sprintf("[%d %d]", m.ConsumeID, m.PID)
}

type Process struct {
	mu              sync.Mutex
	id              int
	ts              int
	currConsumeID   int
	requestQueue    []Request
	succAckInfo     map[int][]int
	succReleaseInfo []int
	logs            []ConsumptionLog
	environment     *DistributedEnvironment
}

func NewProcess(id int, env *DistributedEnvironment) *Process {
	rand.Seed(time.Now().Unix())
	return &Process{
		id:          id,
		succAckInfo: make(map[int][]int),
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
			m._tick(ctx, 0)
			m.currConsumeID += 1

			request := Request{
				Type:      RequestTypeConsume,
				FromPID:   m.id,
				ConsumeID: m.currConsumeID,
				TS:        m.ts,
			}

			for pid := range m.environment.Top {
				if pid == m.id {
					m.succAckInfo[request.ConsumeID] = append(
						m.succAckInfo[request.ConsumeID], pid)
					continue
				}

				m.__printf("send request %v to P[%d]", request, pid)
				m._enqueue(ctx, request)

				go func(receiverID int) {
					m.mu.Lock()
					m._tick(ctx, 0)
					requestCopy := request.Copy()
					requestCopy.TS = m.ts
					m.mu.Unlock()

					res, err := m.environment.SendRequestToProcess(ctx, receiverID, requestCopy)

					m.mu.Lock()
					if err != nil {
						// NOTE: code should not reach here
						m.__panicf("err %v res %v", err, res)
					} else {
						m._tick(ctx, res.TS)
						m.__printf("receive ack %v", res)
						m.succAckInfo[request.ConsumeID] = append(
							m.succAckInfo[request.ConsumeID], res.FromPID)
					}
					m.mu.Unlock()
				}(pid)
			}
		}

		m.mu.Unlock()

		time.Sleep(SendRandomRequestInterval)
	}
}

func (m *Process) checkAndConsumeResourceLoop(ctx context.Context) {
	for {
		m.mu.Lock()

		if len(m.requestQueue) > 0 {
			req := m.requestQueue[0]

			if req.FromPID == m.id && m.environment.HasReceiveAllResponse(m.succAckInfo[req.ConsumeID]) && m.environment.Resource.TryLock() {
				m._tick(ctx, 0)
				m.__printf("consume resource")
				// new event: consume resource

				m.logs = append(m.logs, ConsumptionLog{
					TS:        req.TS,
					PID:       req.FromPID,
					ConsumeID: req.ConsumeID,
				})

				for pid := range m.environment.Top {
					if pid == m.id {
						m.succReleaseInfo = append(m.succReleaseInfo, pid)
						continue
					}

					go func(receiverID int) {
						var request Request
						m.mu.Lock()
						// event: send release request
						m._tick(ctx, 0)
						request = Request{
							Type:      RequestTypeRelease,
							FromPID:   m.id,
							ConsumeID: req.ConsumeID,
							TS:        m.ts,
						}
						m.__printf("send release request %v to P[%d]", request, receiverID)
						m.mu.Unlock()

						res, err := m.environment.SendRequestToProcess(ctx, receiverID, request)

						m.mu.Lock()
						// sanity check
						if err != nil || res.ConsumeID != req.ConsumeID {
							// NOTE: should not reach here
							m.__panicf("err %v res %v", err, res)
						} else {
							m._tick(ctx, res.TS)
							m.succReleaseInfo = append(m.succReleaseInfo, receiverID)
						}
						m.mu.Unlock()
					}(pid)
				}
			}
		}
		m.mu.Unlock()

		time.Sleep(CheckAndConsumeResourceInterval)
	}
}

func (m *Process) checkAndReleaseResourceLoop(ctx context.Context) {
	for {
		m.mu.Lock()

		if len(m.requestQueue) > 0 {
			var req Request
			var idx int
			for i, request := range m.requestQueue {
				if request.FromPID == m.id {
					req = request
					idx = i
					break
				}
			}

			if req.FromPID == m.id && m.environment.HasReceiveAllResponse(m.succReleaseInfo) {
				// new event: release resource
				m._tick(ctx, 0)
				m.succReleaseInfo = nil
				delete(m.succAckInfo, req.TS)
				m.requestQueue = append(m.requestQueue[:idx], m.requestQueue[idx+1:]...)
				m.__printf("release resource")
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

	// event: new request
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
		Type:      ResponseTypeAckConsume,
		FromPID:   m.id,
		ConsumeID: req.ConsumeID,
		TS:        m.ts,
	}
}

func (m *Process) _handleReleaseRequest(ctx context.Context, req Request) Response {
	for i, request := range m.requestQueue {
		// NOTE: find the earliest resource request from req.PID
		if request.FromPID == req.FromPID {
			if request.ConsumeID != req.ConsumeID {
				m.__panicf("invalid data")
			}

			m.requestQueue = append(m.requestQueue[:i], m.requestQueue[i+1:]...)
			m.logs = append(m.logs, ConsumptionLog{
				TS:        request.TS,
				PID:       request.FromPID,
				ConsumeID: request.ConsumeID,
			})

			m.__printf("ack release %v", req)
			return Response{
				FromPID:   m.id,
				TS:        m.ts,
				Type:      ResponseTypeAckRelease,
				ConsumeID: req.ConsumeID,
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
