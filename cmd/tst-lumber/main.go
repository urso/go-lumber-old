package main

import (
	"log"

	"github.com/urso/go-lumber/v2/server"
)

func main() {
	s, err := server.ListenAndServe(":5044")
	if err != nil {
		log.Fatal(err)
	}

	defer s.Close()
	log.Println("tcp server up")

	for batch := range s.ReceiveChan() {
		log.Printf("Received batch of %v events\n", len(batch.Events))
		batch.ACK()
	}
}
