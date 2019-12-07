package main

import "time"

var (
	CheckAndConsumeResourceInterval = 1000 * time.Millisecond
	CheckAndReleaseResourceInterval = 1000 * time.Millisecond
	SendRandomRequestInterval       = 1000 * time.Millisecond

	RequestSendingProb = 0.8
)
