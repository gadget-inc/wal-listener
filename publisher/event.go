package publisher

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ihippik/wal-listener/v2/config"
)

// Event structure for publishing to the NATS server.
type Event struct {
	ID         uuid.UUID      `json:"id"`
	Schema     string         `json:"schema"`
	Table      string         `json:"table"`
	Action     string         `json:"action"`
	Data       map[string]any `json:"data"`
	DataOld    map[string]any `json:"dataOld"`
	EventTime  time.Time      `json:"commitTime"`
	LSN        int64          `json:"lsn"`
	PrimaryKey []string       `json:"primaryKey"`
}

// SubjectName creates subject name from the prefix, schema and table name. Also using topic map from cfg.
func (e *Event) SubjectName(cfg *config.Config) string {
	topic := fmt.Sprintf("%s_%s", e.Schema, e.Table)

	if cfg.Listener.ParsedTopicsMap != nil {
		for pattern, replacement := range cfg.Listener.ParsedTopicsMap {
			if pattern.MatchString(topic) {
				topic = pattern.ReplaceAllString(topic, replacement)
				break
			}
		}
	}

	topic = cfg.Publisher.Topic + "." + cfg.Publisher.TopicPrefix + topic

	return topic
}
