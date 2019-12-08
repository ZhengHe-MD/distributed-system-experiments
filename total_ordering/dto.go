package main

import "fmt"

const (
	RequestTypeConsume = iota + 1
	RequestTypeRelease
)

const (
	ResponseTypeAckConsume = iota + 1
	ResponseTypeAckRelease
)

type RequestType int

func (m RequestType) String() string {
	switch m {
	case RequestTypeConsume:
		return "consume"
	case RequestTypeRelease:
		return "release"
	default:
		return "invalid"
	}
}

type ResponseType int

func (m ResponseType) String() string {
	switch m {
	case ResponseTypeAckConsume:
		return "ack-consume"
	case ResponseTypeAckRelease:
		return "ack-release"
	default:
		return "invalid"
	}
}

type Request struct {
	Type    RequestType
	FromPID int
	TS      int
}

func (m Request) String() string {
	return fmt.Sprintf("[%d %d]",
		m.FromPID, m.TS)
}

func (m Request) Copy() (copy Request) {
	copy = m
	return
}

type Response struct {
	Type    ResponseType
	FromPID int
	TS      int
}

func (m Response) String() string {
	return fmt.Sprintf("[%d %d]",
		m.FromPID, m.TS)
}
