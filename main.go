package main

import (
	"fmt"
	"os"
	"time"

	ljack "github.com/urso/go-lumber/lumberjack"
	"github.com/elastic/libbeat/logp"
)


//sample implementation of using the lumberjack server
func main() {
	tcpBindAddress := "localhost:5044"
	server, err := ljack.NewLumberJack(tcpBindAddress,  90*time.Second)
	if err != nil {
		fmt.Println("failed to create tcp server: ", err)
		os.Exit(1)
	}
	fmt.Println("tcp server up")

	defer server.Close()

	ha := ljack.Handlers{Success: success, Failure: failure, Process: process}
	ljack.AcceptConnections(server, &ha)
}

func success (ack ljack.AckHandler) bool {
	ljack.SendAck(ack)
	return true;
}

func failure (ack ljack.AckHandler) bool {
	logp.Critical("Unable to process incoming packets")
	return true
}

func process (events []*ljack.Message) bool {
	for _, event := range events {
		fmt.Println(event.Doc)
	}

	return true;
}