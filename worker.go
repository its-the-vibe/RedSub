package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/redis/go-redis/v9"
)

const (
	// maxPublishRetries is the number of Pub/Sub publish attempts before a
	// message is moved to the dead-letter list.
	maxPublishRetries = 5
	// maxPublishBackoff caps the exponential back-off between publish retries.
	maxPublishBackoff = 30 * time.Second
)

// deadLetterList returns the Redis key used to store messages that could not
// be published after exhausting all retries.
func deadLetterList(redisList string) string {
	return redisList + ":failed"
}

// runWorker connects to Redis and GCP Pub/Sub, then continuously pops messages
// from the configured Redis list using BLPOP and publishes them to the mapped
// Pub/Sub topic. It returns when ctx is cancelled or an unrecoverable error occurs.
func runWorker(ctx context.Context, cfg *Config, redisPassword string, mapping QueueMapping) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		Password: redisPassword,
	})
	defer rdb.Close()

	pubsubClient, err := pubsub.NewClient(ctx, cfg.GCP.ProjectID)
	if err != nil {
		return fmt.Errorf("creating pubsub client: %w", err)
	}
	defer pubsubClient.Close()

	topic := pubsubClient.Topic(mapping.PubSubTopic)
	defer topic.Stop()

	log.Printf("worker started: redis list %q -> pubsub topic %q", mapping.RedisList, mapping.PubSubTopic)

	for {
		select {
		case <-ctx.Done():
			log.Println("worker stopped")
			return nil
		default:
			result, err := rdb.BLPop(ctx, 5*time.Second, mapping.RedisList).Result()
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				if err == redis.Nil {
					// Timeout, no message available; just continue
					continue
				}
				log.Printf("BLPOP error for list %q: %v; retrying in 1s", mapping.RedisList, err)
				time.Sleep(time.Second)
				log.Printf("tick")
				continue
			}

			// BLPop returns [key, value]; the payload is at index 1.
			payload := result[1]

			if !publishWithRetry(ctx, rdb, topic, mapping, payload) {
				return nil
			}
		}
	}
}

// publishWithRetry attempts to publish payload to the Pub/Sub topic up to
// maxPublishRetries times with exponential back-off. If all attempts fail the
// message is pushed to the dead-letter Redis list so it is not lost.
// It returns false only when ctx is cancelled (caller should stop the worker).
func publishWithRetry(ctx context.Context, rdb *redis.Client, topic *pubsub.Topic, mapping QueueMapping, payload string) bool {
	backoff := time.Second

	for attempt := 1; attempt <= maxPublishRetries; attempt++ {
		res := topic.Publish(ctx, &pubsub.Message{Data: []byte(payload)})
		_, err := res.Get(ctx)
		if err == nil {
			log.Printf("published message from list %q to topic %q", mapping.RedisList, mapping.PubSubTopic)
			return true
		}

		if ctx.Err() != nil {
			// Context cancelled; re-queue the message so it is not lost.
			requeue(rdb, mapping.RedisList, payload)
			return false
		}

		log.Printf("publish attempt %d/%d failed for list %q to topic %q: %v",
			attempt, maxPublishRetries, mapping.RedisList, mapping.PubSubTopic, err)

		if attempt < maxPublishRetries {
			select {
			case <-ctx.Done():
				requeue(rdb, mapping.RedisList, payload)
				return false
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxPublishBackoff {
				backoff = maxPublishBackoff
			}
		}
	}

	// All retries exhausted: move to dead-letter list to prevent data loss.
	dl := deadLetterList(mapping.RedisList)
	log.Printf("moving undeliverable message from list %q to dead-letter list %q", mapping.RedisList, dl)
	requeue(rdb, dl, payload)
	return true
}

// requeue pushes payload back onto the tail of redisList using a short-lived
// background context so it still works when the main context is cancelled.
func requeue(rdb *redis.Client, redisList, payload string) {
	reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.RPush(reqCtx, redisList, payload).Err(); err != nil {
		log.Printf("failed to re-queue message to list %q: %v (message may be lost)", redisList, err)
	}
}
