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
	mu             sync.Mutex
	id             int
	requestQueue   []Request
	acknowledgeMap map[int][]int
	releaseList    []int
	environment    *DistributedEnvironment
	timestamp      int
	currConsumeID  int

	_order []ConsumptionLog
}

func NewProcess(id int, env *DistributedEnvironment) *Process {
	rand.Seed(time.Now().Unix())
	return &Process{
		id:             id,
		requestQueue:   nil,
		acknowledgeMap: make(map[int][]int),
		releaseList:    nil,
		environment:    env,
		timestamp:      0,
		currConsumeID:  0,
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
			clog := ConsumptionLog{
				TS:        m.timestamp,
				PID:       m.id,
				ConsumeID: m.currConsumeID,
			}

			for pid := range m.environment.Top {
				if pid == m.id {
					m.acknowledgeMap[clog.ConsumeID] = append(
						m.acknowledgeMap[clog.ConsumeID], pid)
					continue
				}

				log.Printf("Process %d: send request %v to process %d at %d", m.id, clog, pid, m.timestamp)

				request := Request{
					Type:      RequestTypeConsume,
					FromPID:   clog.PID,
					ConsumeID: clog.ConsumeID,
					TS:        clog.TS,
				}

				m._enqueue(ctx, request)

				go func(receiverID int) {
					m.mu.Lock()
					m._tick(ctx, 0)
					requestCopy := request.Copy()
					requestCopy.TS = m.timestamp
					m.mu.Unlock()

					res, err := m.environment.SendRequestToProcess(ctx, receiverID, requestCopy)

					m.mu.Lock()
					if err != nil {
						// NOTE: code should not reach here
						log.Panicf("Process %d: err %v res %v at %d", m.id, err, res, m.timestamp)
					} else {
						m._tick(ctx, res.TS)
						log.Printf("Process %d: receive ack from process %d at %d", m.id, res.FromPID, m.timestamp)
						m.acknowledgeMap[clog.ConsumeID] = append(
							m.acknowledgeMap[clog.ConsumeID], res.FromPID)
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

			if req.FromPID == m.id && m.environment.HasReceiveAllResponse(m.acknowledgeMap[req.ConsumeID]) && m.environment.Resource.TryLock() {
				m._tick(ctx, 0)
				log.Printf("Process %d: consume resource at %d", m.id, m.timestamp)
				// new event: consume resource

				m._order = append(m._order, ConsumptionLog{
					TS:        req.TS,
					PID:       req.FromPID,
					ConsumeID: req.ConsumeID,
				})

				for pid := range m.environment.Top {
					if pid == m.id {
						m.releaseList = append(m.releaseList, pid)
						continue
					}

					go func(receiverID int) {
						var request Request
						m.mu.Lock()
						// event: send release request
						m._tick(ctx, 0)
						log.Printf("Process %d: send release request to process %d at %d",
							m.id, receiverID, m.timestamp)
						request = Request{
							Type:      RequestTypeRelease,
							FromPID:   m.id,
							ConsumeID: req.ConsumeID,
							TS:        m.timestamp,
						}
						m.mu.Unlock()

						res, err := m.environment.SendRequestToProcess(ctx, receiverID, request)

						m.mu.Lock()
						// sanity check
						if err != nil || res.ConsumeID != req.ConsumeID {
							// NOTE: should not reach here
							log.Panicf("Process %d: err %v at %d", m.id, err, m.timestamp)
						} else {
							m._tick(ctx, res.TS)
							m.releaseList = append(m.releaseList, receiverID)
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

			if req.FromPID == m.id && m.environment.HasReceiveAllResponse(m.releaseList) {
				// new event: release resource
				m._tick(ctx, 0)
				m.releaseList = nil
				delete(m.acknowledgeMap, req.TS)
				m.requestQueue = append(m.requestQueue[:idx], m.requestQueue[idx+1:]...)
				log.Printf("Process %d: release resource at %d", m.id, m.timestamp)
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

	log.Printf("Process %d: ack request %v at %d", m.id, req, m.timestamp)
	return Response{
		Type:      ResponseTypeAckConsume,
		FromPID:   m.id,
		ConsumeID: req.ConsumeID,
		TS:        m.timestamp,
	}
}

func (m *Process) _handleReleaseRequest(ctx context.Context, req Request) Response {
	for i, request := range m.requestQueue {
		// NOTE: find the earliest resource request from req.PID
		if request.FromPID == req.FromPID {
			if request.ConsumeID != req.ConsumeID {
				log.Panicf("Process %d: invalid data", m.id)
			}

			m.requestQueue = append(m.requestQueue[:i], m.requestQueue[i+1:]...)
			m._order = append(m._order, ConsumptionLog{
				TS:        request.TS,
				PID:       request.FromPID,
				ConsumeID: request.ConsumeID,
			})

			log.Printf("Process %d: ack release %v at %d", m.id, req, m.timestamp)
			return Response{
				FromPID:   m.id,
				TS:        m.timestamp,
				Type:      ResponseTypeAckRelease,
				ConsumeID: req.ConsumeID,
			}
		}
	}
	log.Panicf("Process %d: err req %v not found in requestQueue at %v", m.id, req, m.timestamp)
	// NOTE: unreachable, make ide happy
	return Response{}
}

func (m *Process) _handle404(ctx context.Context, req Request) Response {
	log.Panicf("Process %d: err unkown request type %v", m.id, req.Type)
	// NOTE: unreachable, make ide happy
	return Response{}
}

// NOTE: thread-unsafe
func (m *Process) _tick(ctx context.Context, externalTimestamp int) {
	later := m.timestamp
	if externalTimestamp > m.timestamp {
		later = externalTimestamp
	}

	m.timestamp = later + 1
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

func (m *Process) _printInternalState() {
	log.Printf("Process %d: requestQueue %v", m.id, m.requestQueue)
	log.Printf("Process %d: _order %v", m.id, m._order)
}

func (m *Process) __printInternalState() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m._printInternalState()
}
