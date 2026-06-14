package ping

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/thalestmm/gowork"
)

type Job struct {
	Message string `json:"message"`
}

func (j *Job) Slug() string { return "ping" }

func (j *Job) Params() json.RawMessage {
	raw, err := json.Marshal(j)
	if err != nil {
		return nil
	}
	return raw
}

func (j *Job) ParseParams(raw json.RawMessage) error {
	return json.Unmarshal(raw, j)
}

func (j *Job) Execute(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	slog.Info("ping job executed", "message", j.Message)
	return nil
}

func init() {
	gowork.Register("ping", func() gowork.Job { return &Job{} })
}
