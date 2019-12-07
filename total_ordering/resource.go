package main

type Resource struct {
	Mutex
}

func NewResource() *Resource {
	return &Resource{}
}
