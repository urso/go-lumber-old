package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/urso/go-lumber/server"
)

func main() {
	s, err := server.ListenAndServe(":5044")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("tcp server up")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		_ = s.Close()
	}()

	for batch := range s.ReceiveChan() {
		log.Printf("Received batch of %v events\n", len(batch.Events))
		batch.ACK()
	}
}
