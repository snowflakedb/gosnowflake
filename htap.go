package gosnowflake

type queryContextEntry struct {
	ID        int   `json:"id"`
	Timestamp int64 `json:"timestamp"`
	Priority  int   `json:"priority"`
	Context   any   `json:"context,omitempty"`
}
