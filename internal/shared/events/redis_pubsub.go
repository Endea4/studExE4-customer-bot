package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func PublishLocationUpdate(ctx context.Context, rdb *redis.Client, channel string, data LocationUpdatedData) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal location event: %w", err)
	}
	return rdb.Publish(ctx, channel, payload).Err()
}

type ManagedSubscription struct {
	sub *redis.PubSub
	Ch  <-chan *redis.Message
}

func (m *ManagedSubscription) Close() error {
	return m.sub.Close()
}

func Subscribe(ctx context.Context, rdb *redis.Client, channel string) (*ManagedSubscription, error) {
	sub := rdb.Subscribe(ctx, channel)
	if err := sub.Ping(ctx); err != nil {
		return nil, fmt.Errorf("subscribe to %s: %w", channel, err)
	}
	return &ManagedSubscription{sub: sub, Ch: sub.Channel()}, nil
}
