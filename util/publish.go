package util

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"io"
	"log"
	"sync/atomic"
)

func Publish(w io.Writer, ctx context.Context, topicID string, client *pubsub.Client, msg string) error {
	// projectID := "my-project-id"
	// topicID := "my-topic"
	// msg := "Hello World"

	t := client.Topic(topicID)
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(msg),
	})
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("pubsub: result.Get: %w", err)
	}
	fmt.Fprintf(w, "Published a message; msg ID: %v\n", id)
	return nil
}

func NewTopic(ctx context.Context, client *pubsub.Client, topicID string) {
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	fmt.Printf("Topic %v created.\n", topic)
}

func PullMsgs(w io.Writer, ctx context.Context, client *pubsub.Client, projectID string, subID string) error {
	// projectID := "my-project-id"
	// subID := "my-sub"

	sub := client.Subscription(subID)

	var received int32
	err := sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		fmt.Fprintf(w, "Got message: %q\n", string(msg.Data))
		atomic.AddInt32(&received, 1)
		msg.Ack()
	})
	if err != nil {
		log.Printf("sub.Receive: %w", err)
		return fmt.Errorf("sub.Receive: %w", err)
	}
	fmt.Fprintf(w, "Received %d messages\n", received)

	return nil
}
