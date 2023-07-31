package concurrency

import (
	"context"
	"sync"
)

type CoordinatorInput interface {
	Key() string
}

type CoordinatorOutput interface {
}

type CoordinatorJob struct {
	waitCount     int
	context       context.Context
	cancelContext context.CancelFunc

	// Results
	done     bool
	statusCv sync.Cond
}

type Coordinator struct {
	In chan CoordinatorInput

	// Jobs to be processed
	jobs       map[string]any
	queuedJobs []*any
}

type Provider interface {
	Compare()
}
