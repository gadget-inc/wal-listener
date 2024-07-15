package publisher

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ihippik/wal-listener/v2/config"
)

// Event structure for publishing to the publisher.
type Event struct {
	ID         uuid.UUID      `json:"id"`
	Schema     string         `json:"schema"`
	Table      string         `json:"table"`
	Action     string         `json:"action"`
	PrimaryKey []interface{}  `json:"primaryKey"`
	Data       map[string]any `json:"data"`
	DataOld    map[string]any `json:"dataOld"`
	EventTime  time.Time      `json:"commitTime"`
}

// SubjectName creates subject name from the prefix, schema and table name. Also using topic map from cfg.
func (e *Event) SubjectName(cfg *config.Config) string {
	name := fmt.Sprintf("%s_%s", e.Schema, e.Table)

	if cfg.Listener.TopicsMap != nil {
		for pattern, replacement := range cfg.Listener.TopicsMap {
			if pattern.MatchString(name) {
				name = pattern.ReplaceAllString(name, replacement)
				break
			}
		}
	}

	name = cfg.Publisher.Topic + "." + cfg.Publisher.TopicPrefix + name

	return name
}
