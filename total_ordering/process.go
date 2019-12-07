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

var (
	checkAndConsumeResourceInterval = 1000 * time.Millisecond
	checkAndReleaseResourceInterval = 1000 * time.Millisecond
	sendRandomRequestInterval       = 1000 * time.Millisecond
)

type Message struct {
	Timestamp int
	ProcessID int
	ConsumeID int
}

func (m Message) String() string {
	return fmt.Sprintf("[%d %d]", m.Timestamp, m.ProcessID)
}

type Process struct {
	mu             sync.Mutex
	id             int
	requestQueue   []Message
	acknowledgeMap map[int][]int
	releaseList    []int
	environment    *DistributedEnvironment
	timestamp      int
	currConsumeID  int

	holdingResource bool

	_order []Message
}

func NewProcess(id int, env *DistributedEnvironment) *Process {
	rand.Seed(time.Now().Unix())
	return &Process{
		id:              id,
		requestQueue:    nil,
		acknowledgeMap:  make(map[int][]int),
		releaseList:     nil,
		environment:     env,
		timestamp:       0,
		currConsumeID:   0,
		holdingResource: false,
	}
}

func (m *Process) Run(ctx context.Context) {
	go m.sendRandomRequest(ctx)
	go m.checkAndConsumeResource(ctx)
	go m.checkAndReleaseResource(ctx)
}

func (m *Process) sendRandomRequest(ctx context.Context) {
	for {
		//log.Printf("Process %d: inside sendRandomRequest", m.id)
		m.mu.Lock()
		//log.Printf("Process %d: inside sendRandomRequest locked", m.id)

		if rand.Float64() < 0.8 {
			m._tick(ctx, 0)
			m.currConsumeID += 1
			msg := Message{
				Timestamp: m.timestamp,
				ProcessID: m.id,
				ConsumeID: m.currConsumeID,
			}

			for pid := range m.environment.Top {
				if pid == m.id {
					m.acknowledgeMap[msg.ConsumeID] = append(
						m.acknowledgeMap[msg.ConsumeID], pid)
					continue
				}

				log.Printf("Process %d: send request %v to process %d at %d", m.id, msg, pid, m.timestamp)

				m._enqueue(ctx, msg)

				go func(receiverID int) {
					m.mu.Lock()
					m._tick(ctx, 0)
					m.mu.Unlock()

					res, err := m.environment.SendRequestToProcess(ctx, receiverID, ResourceReq{
						Timestamp: msg.Timestamp,
						ProcessID: msg.ProcessID,
						IsRelease: false,
						ConsumeID: msg.ConsumeID,
					})

					m.mu.Lock()
					if err != nil || !res.Acknowledged {
						m._tick(ctx, 0)
						log.Printf("Process %d: err %v res %v at %d", m.id, err, res, m.timestamp)
					} else {
						m._tick(ctx, res.Timestamp)
						log.Printf("Process %d: receive ack from process %d at %d", m.id, res.ProcessID, m.timestamp)
						m.acknowledgeMap[msg.ConsumeID] = append(
							m.acknowledgeMap[msg.ConsumeID], res.ProcessID)
					}
					m.mu.Unlock()
				}(pid)
			}
		}

		m.mu.Unlock()

		time.Sleep(sendRandomRequestInterval)
	}
}

func (m *Process) checkAndConsumeResource(ctx context.Context) {
	for {
		//log.Printf("Process %d: inside checkAndConsumeResource", m.id)
		m.mu.Lock()
		//log.Printf("Process %d: inside checkAndConsumeResource locked", m.id)

		if len(m.requestQueue) > 0 && (!m.holdingResource) {
			req := m.requestQueue[0]

			if req.ProcessID == m.id && m.environment.HasReceiveAllResponse(m.acknowledgeMap[req.ConsumeID]) && m.environment.Resource.TryLock() {
				m._tick(ctx, 0)
				log.Printf("Process %d: consume resource at %d", m.id, m.timestamp)
				//m._printInternalState()
				// new event: consume resource
				m.holdingResource = true

				m._order = append(m._order, Message{
					Timestamp: req.Timestamp,
					ProcessID: req.ProcessID,
					ConsumeID: req.ConsumeID,
				})

				for pid := range m.environment.Top {
					if pid == m.id {
						m.releaseList = append(m.releaseList, pid)
						continue
					}

					go func(receiverID int) {
						var timestamp int
						var pid int
						m.mu.Lock()
						m._tick(ctx, 0)
						log.Printf("Process %d: send release request to process %d at %d",
							m.id, receiverID, m.timestamp)
						timestamp = m.timestamp
						pid = m.id
						m.mu.Unlock()

						res, err := m.environment.SendRequestToProcess(ctx, receiverID, ResourceReq{
							Timestamp: timestamp,
							ProcessID: pid,
							IsRelease: true,
							ConsumeID: req.ConsumeID,
						})

						m.mu.Lock()
						// new event: receive response
						if err != nil || !res.Released || (res.ToConsumeID != req.ConsumeID) {
							m._tick(ctx, 0)
							log.Printf("Process %d: err %v at %d", m.id, err, m.timestamp)
						} else {
							m._tick(ctx, res.Timestamp)
							m.releaseList = append(m.releaseList, receiverID)
						}
						m.mu.Unlock()
					}(pid)
				}
			}
		}
		m.mu.Unlock()

		time.Sleep(checkAndConsumeResourceInterval)
	}
}

func (m *Process) checkAndReleaseResource(ctx context.Context) {
	for {
		//log.Printf("Process %d: inside checkAndReleaseResource", m.id)

		m.mu.Lock()

		if len(m.requestQueue) > 0 && m.holdingResource {
			var req Message
			var idx int
			for i, request := range m.requestQueue {
				if request.ProcessID == m.id {
					req = request
					idx = i
					break
				}
			}

			if req.ProcessID == m.id && m.environment.HasReceiveAllResponse(m.releaseList) {
				// new event: release resource
				m._tick(ctx, 0)
				m.releaseList = nil
				delete(m.acknowledgeMap, req.Timestamp)
				m.requestQueue = append(m.requestQueue[:idx], m.requestQueue[idx+1:]...)
				m.holdingResource = false
				log.Printf("Process %d: release resource at %d", m.id, m.timestamp)
				m.environment.Resource.Unlock()
			}
		}

		m.mu.Unlock()

		time.Sleep(checkAndReleaseResourceInterval)
	}
}

func (m *Process) HandleRequest(ctx context.Context, req ResourceReq) ResourceRes {
	m.mu.Lock()
	defer m.mu.Unlock()

	// new request means new event
	m._tick(ctx, req.Timestamp)

	if req.IsRelease {
		for i, request := range m.requestQueue {
			// NOTE: find the earliest resource request from req.ProcessID
			if request.ProcessID == req.ProcessID {

				if request.ConsumeID != req.ConsumeID {
					log.Panicf("Process %d: invalid data", m.id)
				}

				m.requestQueue = append(m.requestQueue[:i], m.requestQueue[i+1:]...)
				m._order = append(m._order, Message{
					Timestamp: request.Timestamp,
					ProcessID: request.ProcessID,
					ConsumeID: request.ConsumeID,
				})
				log.Printf("Process %d: ack release %v at %d", m.id, req, m.timestamp)
				return ResourceRes{
					ProcessID:   m.id,
					Timestamp:   m.timestamp,
					Released:    true,
					ToConsumeID: req.ConsumeID,
				}
			}
		}
		log.Panicf("Process %d: err req %v not found in requestQueue at %v", m.id, req, m.timestamp)
	}

	m._enqueue(ctx, Message{
		Timestamp: req.Timestamp,
		ProcessID: req.ProcessID,
		ConsumeID: req.ConsumeID,
	})

	log.Printf("Process %d: ack request %v at %d", m.id, req, m.timestamp)

	return ResourceRes{
		Timestamp:    m.timestamp,
		ProcessID:    m.id,
		Acknowledged: true,
		ToConsumeID:  req.ConsumeID,
	}
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

func (m *Process) _enqueue(ctx context.Context, msg Message) {
	m.requestQueue = append(m.requestQueue, msg)
	sort.SliceStable(m.requestQueue, func(i, j int) bool {
		if m.requestQueue[i].Timestamp < m.requestQueue[j].Timestamp {
			return true
		} else if m.requestQueue[i].Timestamp > m.requestQueue[j].Timestamp {
			return false
		} else {
			return m.requestQueue[i].ProcessID < m.requestQueue[j].ProcessID
		}
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
