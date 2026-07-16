package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type OutboxEntry struct {
	AggregateType string    `bson:"aggregate_type"`
	AggregateID   string    `bson:"aggregate_id"`
	EventType     string    `bson:"event_type"`
	Topic         string    `bson:"topic"`
	Payload       []byte    `bson:"payload"`
	Status        string    `bson:"status"`
	CreatedAt     time.Time `bson:"created_at"`
	PublishedAt   *time.Time `bson:"published_at,omitempty"`
}

func OutboxCollection(db *mongo.Database) *mongo.Collection {
	return db.Collection("outbox")
}

func WriteOutboxEntry(ctx context.Context, db *mongo.Database, topic, eventType, aggregateType, aggregateID string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}
	entry := OutboxEntry{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		Topic:         topic,
		Payload:       payload,
		Status:        "pending",
		CreatedAt:     time.Now(),
	}
	_, err = OutboxCollection(db).InsertOne(ctx, entry)
	if err != nil {
		return fmt.Errorf("insert outbox entry: %w", err)
	}
	return nil
}

func StartOutboxPublisher(ctx context.Context, db *mongo.Database, producer sarama.SyncProducer, pollInterval time.Duration) {
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}}).SetBatchSize(100)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cursor, err := OutboxCollection(db).Find(ctx, bson.M{"status": "pending"}, opts)
			if err != nil {
				log.Printf("outbox poll error: %v", err)
				continue
			}

			var entries []OutboxEntry
			if err := cursor.All(ctx, &entries); err != nil {
				cursor.Close(ctx)
				log.Printf("outbox decode error: %v", err)
				continue
			}
			cursor.Close(ctx)

			for _, entry := range entries {
				msg := &sarama.ProducerMessage{
					Topic: entry.Topic,
					Key:   sarama.StringEncoder(entry.AggregateID),
					Value: sarama.ByteEncoder(entry.Payload),
				}
				partition, offset, err := producer.SendMessage(msg)
				if err != nil {
					log.Printf("outbox publish error (topic=%s id=%s): %v", entry.Topic, entry.AggregateID, err)
					continue
				}

				now := time.Now()
				_, err = OutboxCollection(db).UpdateOne(ctx,
					bson.M{
						"aggregate_type": entry.AggregateType,
						"aggregate_id":   entry.AggregateID,
						"event_type":     entry.EventType,
						"status":         "pending",
					},
					bson.M{
						"$set": bson.M{
							"status":       "published",
							"published_at": now,
						},
					},
				)
				if err != nil {
					log.Printf("outbox mark published error: %v", err)
				}

				log.Printf("outbox published: topic=%s type=%s id=%s partition=%d offset=%d",
					entry.Topic, entry.EventType, entry.AggregateID, partition, offset)
			}
		}
	}
}

func EnsureOutboxIndexes(ctx context.Context, db *mongo.Database) error {
	_, err := OutboxCollection(db).Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "created_at", Value: 1}}},
		{Keys: bson.D{{Key: "aggregate_type", Value: 1}, {Key: "aggregate_id", Value: 1}}},
	})
	return err
}
