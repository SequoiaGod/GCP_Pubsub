package main

import (
	"GCP_Pubsub/util"
	"bufio"
	"cloud.google.com/go/pubsub"
	"context"
	"log"
	"os"
	"os/signal"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	log.Println("Enter text to publish to Pubsub:  ")
	reader := bufio.NewReader(os.Stdin)
	done := make(chan struct{})
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	log.Println("starting server")
	// Sets your Google Cloud Platform project ID.
	go func() {
		<-terminate
		log.Println("system closing---")
		cancel()
		close(done)
		log.Println("closed---")
	}()
	// Creates a client.
	client, err := pubsub.NewClient(ctx, util.ProjectID)
	defer client.Close()
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	for {
		select {
		case <-done:
			return
		default:
			input, err := reader.ReadString('\n')

			if err != nil {
				log.Println("error input", err)
				continue
			}
			if input == "" {
				continue
			}
			input = input[:len(input)-1]
			log.Println(input)
			err = util.Publish(os.Stdout, ctx, util.PopicID, client, input)
			if err != nil {
				log.Fatalf("Failed to publish message to the  client: %v", err)
			}
		}

	}

}
