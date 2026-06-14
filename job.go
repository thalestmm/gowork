package gowork

import (
	"github.com/thalestmm/gowork/internal/registry"
)

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

type Job = registry.Job

var (
	Register      = registry.Register
	ResetRegistry = registry.ResetRegistry
)
