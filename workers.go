package main

type Worker interface {

}

type SimpleWorker struct {}

type PriorityWorker struct {
	MinPriority int
	MaxPriority *int
}

type SpecificTaskWorker struct {
	TaskSlug string
}
