package main

import (
	"GCP_Pubsub/util"
	"cloud.google.com/go/pubsub"
	"context"
	"log"
	"os"
	"os/signal"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	log.Println("starting server")
	// Sets your Google Cloud Platform project ID.

	// Creates a client.
	client, err := pubsub.NewClient(ctx, util.ProjectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	defer client.Close()
	err = util.PullMsgs(os.Stdout, ctx, client, util.ProjectID, util.SubID)
	if err != nil {
		log.Fatalf("Failed to publish message to the  client: %v", err)
	}
	<-terminate
	log.Println("system closing---")
	cancel()
	log.Println("closed---")
}
