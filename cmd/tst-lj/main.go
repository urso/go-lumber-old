package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/urso/go-lumber/server"
)

func main() {
	bind := flag.String("bind", ":5044", "[host]:port to listen on")
	v1 := flag.Bool("v1", false, "Enable protocol version v1")
	v2 := flag.Bool("v2", false, "Enable protocol version v2")
	es := flag.Bool("es", false, "Enable simplified es bulk protocol")
	esSplit := flag.Int("split", 2048, "Batch split limit for es bulk events")
	esSilent := flag.Bool("silent", false, "If enabled send empty response")
	quiet := flag.Bool("q", false, "Quiet")
	flag.Parse()

	s, err := server.ListenAndServe(*bind,
		server.V1(*v1),
		server.V2(*v2),
		server.ES(*es),
		server.Split(*esSplit),
		server.Silent(*esSilent),
	)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	log.Println("tcp server up")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		_ = s.Close()
	}()

	debug := !*quiet
	for batch := range s.ReceiveChan() {
		if debug {
			log.Printf("Received batch of %v events\n", len(batch.Events))
		}
		batch.ACK()
	}
}
