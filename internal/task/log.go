package task

import "time"

// TaskLogResult is the safe, bounded view of one task run log.
type TaskLogResult struct {
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modifiedAt"`
	Lines      []string  `json:"lines"`
}
