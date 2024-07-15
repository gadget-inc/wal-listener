package publisher

import (
	"context"
	"fmt"
	"strings"

	"github.com/goccy/go-json"
)

// GooglePubSubPublisher represent Pub/Sub publisher.
type GooglePubSubPublisher struct {
	pubSubConnection *PubSubConnection
	enableOrdering   bool
}

// NewGooglePubSubPublisher create new instance of GooglePubSubPublisher.
func NewGooglePubSubPublisher(pubSubConnection *PubSubConnection, enableOrdering bool) *GooglePubSubPublisher {
	return &GooglePubSubPublisher{
		pubSubConnection: pubSubConnection,
		enableOrdering:   enableOrdering,
	}
}

// Publish send events, implements eventPublisher.
func (p *GooglePubSubPublisher) Publish(ctx context.Context, topic string, event *Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	var orderingKey string
	if p.enableOrdering {
		if len(event.PrimaryKey) > 0 {
			strKeys := make([]string, len(event.PrimaryKey))
			for i, key := range event.PrimaryKey {
				strKeys[i] = fmt.Sprintf("%v", key)
			}
			orderingKey = strings.Join(strKeys, "-")
		}
	}

	return p.pubSubConnection.Publish(ctx, topic, body, orderingKey)
}

func (p *GooglePubSubPublisher) Close() error {
	return p.pubSubConnection.Close()
}
