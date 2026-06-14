package jobs

import (
	"context"
	"encoding/json"
	"log"
)

type PingJob struct {
	Message string `json:"message"`
}

func (j *PingJob) Slug() string { return "ping" }

func (j *PingJob) Params() json.RawMessage {
	raw, err := json.Marshal(j)
	if err != nil {
		return nil
	}
	return raw
}

func (j *PingJob) ParseParams(raw json.RawMessage) error {
	return json.Unmarshal(raw, j)
}

func (j *PingJob) Execute(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Printf("ping job: %s", j.Message)
	return nil
}

func init() {
	Register("ping", func() Job { return &PingJob{} })
}
