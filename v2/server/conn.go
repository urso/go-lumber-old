package server

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/urso/go-lumber/lj"
	"github.com/urso/go-lumber/server"
)

type conn struct {
	cb        server.Eventer
	client    net.Conn
	reader    *reader
	writer    *writer
	keepalive time.Duration

	signal chan struct{}
	ch     chan *lj.Batch

	stopGuard sync.Once
}

func newConn(
	cb server.Eventer,
	client net.Conn,
	to, keepalive time.Duration,
	decoder jsonDecoder,
) *conn {
	conn := &conn{
		cb:        cb,
		client:    client,
		reader:    newReader(client, to, decoder),
		writer:    newWriter(client, to),
		keepalive: keepalive,
		signal:    make(chan struct{}),
		ch:        make(chan *lj.Batch),
	}
	return conn
}

func (c *conn) Run() {
	defer close(c.ch)

	// start async routine for returning ACKs to client.
	// Sends ACK of 0 every 'keepalive' seconds to signal
	// client the batch still being in pipeline
	go c.ackLoop()

	if err := c.handle(); err != nil {
		log.Println(err)
	}
}

func (c *conn) Stop() {
	c.stopGuard.Do(func() {
		close(c.signal)
		_ = c.client.Close()
	})
}

func (c *conn) handle() error {
	log.Printf("Start client handler")
	defer log.Printf("client handler stopped")
	defer c.Stop()

	for {
		// 1. read data into batch
		b, err := c.reader.readBatch()
		if err != nil {
			return err
		}

		// read next batch if empty batch has been received
		if b == nil {
			continue
		}

		// 2. push batch to ACK queue
		select {
		case <-c.signal:
			return nil
		case c.ch <- b:
		}

		// 3. push batch to server receive queue:
		if err := c.cb.OnEvents(b); err != nil {
			return nil
		}
	}
}

func (c *conn) ackLoop() {
	log.Println("start client ack loop")
	defer log.Println("client ack loop stopped")

	// drain queue on shutdown.
	// Stop ACKing batches in case of error, forcing client to reconnect
	defer func() {
		log.Println("drain ack loop")
		for range c.ch {
		}
	}()

	for {
		select {
		case <-c.signal: // return on client/server shutdown
			log.Println("receive client connection close signal")
			return
		case b, open := <-c.ch:
			if !open {
				return
			}
			if err := c.waitACK(b); err != nil {
				return
			}
		}
	}
}

func (c *conn) waitACK(batch *lj.Batch) error {
	n := len(batch.Events)
	for {
		select {
		case <-c.signal:
			return nil
		case <-batch.Await():
			// send ack
			return c.writer.ACK(n)
		case <-time.After(c.keepalive):
			if err := c.writer.ACK(0); err != nil {
				return err
			}
		}
	}
}
