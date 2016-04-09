package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/urso/go-lumber/v2/server"
)

//sample implementation of using the lumberjack server
func main() {
	tcpBindAddress := "localhost:5044"
	s, err := server.NewLumberJack(tcpBindAddress, 90*time.Second)
	if err != nil {
		fmt.Println("failed to create tcp server: ", err)
		os.Exit(1)
	}
	fmt.Println("tcp server up")

	defer s.Close()

	ha := server.Handlers{Success: success, Failure: failure, Process: process}
	server.AcceptConnections(s, &ha)
}

func success(ack server.AckHandler) bool {
	server.SendAck(ack)
	return true
}

func failure(ack server.AckHandler) bool {
	log.Println("Unable to process incoming packets")
	return true
}

func process(events []*server.Message) bool {
	for _, event := range events {
		fmt.Println(event.Doc)
	}

	return true
}
