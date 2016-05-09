package main

import (
	"fmt"
	"log"

	"github.com/urso/go-lumber/v2/server"
)

func main() {
	s, err := server.ListenAndServe("localhost:5044")
	if err != nil {
		log.Fatal(err)
	}

	defer s.Close()
	fmt.Println("tcp server up")

	for batch := range s.ReceiveChan() {
		batch.ACK()

		fmt.Printf("Received batch of %v events\n", len(batch.Events))
	}
}
