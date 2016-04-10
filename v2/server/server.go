package server

import (
	"bufio"
	"compress/zlib"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/urso/go-lumber/v2/protocol"
)

type Server struct {
	listener net.Listener
	opts     options

	ch chan *Batch

	done chan struct{}
	wg   sync.WaitGroup
}

type Batch struct {
	Events []interface{}
	ack    chan struct{}
}

type Option func(*options) error

type options struct {
	timeout   time.Duration
	keepalive time.Duration
	decoder   jsonDecoder
	tls       *tls.Config
}

type conn struct {
	server    *Server
	c         net.Conn
	reader    *reader
	to        time.Duration
	keepalive time.Duration

	signal chan struct{}
	ch     chan *Batch
}

type reader struct {
	in   *bufio.Reader
	conn net.Conn
	buf  []byte
	opts options
}

type jsonDecoder func([]byte, interface{}) error

var (
	// ErrProtocolError is returned if an protocol error was detected in the
	// conversation with lumberjack server.
	ErrProtocolError = errors.New("lumberjack protocol error")
)

func JSONDecoder(decoder func([]byte, interface{}) error) Option {
	return func(opt *options) error {
		opt.decoder = decoder
		return nil
	}
}

func Timeout(to time.Duration) Option {
	return func(opt *options) error {
		if to < 0 {
			return errors.New("timeouts must not be negative")
		}
		opt.timeout = to
		return nil
	}
}

func TLS(tls *tls.Config) Option {
	return func(opt *options) error {
		opt.tls = tls
		return nil
	}
}

func applyOptions(opts []Option) (options, error) {
	o := options{
		decoder:   json.Unmarshal,
		timeout:   30 * time.Second,
		keepalive: 3 * time.Second,
		tls:       nil,
	}

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return o, err
		}
	}
	return o, nil
}

func NewWithListener(l net.Listener, opts ...Option) (*Server, error) {
	o, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}

	s := &Server{
		listener: l,
		done:     make(chan struct{}),
		ch:       make(chan *Batch, 128),
		opts:     o,
	}

	s.wg.Add(1)
	go s.run()

	return s, nil
}

func ListenAndServeWith(
	binder func(network, addr string) (net.Listener, error),
	addr string,
	opts ...Option,
) (*Server, error) {
	l, err := binder("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewWithListener(l, opts...)
}

func ListenAndServe(addr string, opts ...Option) (*Server, error) {
	binder := net.Listen
	o, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}

	if o.tls != nil {
		binder = func(network, addr string) (net.Listener, error) {
			return tls.Listen(network, addr, o.tls)
		}
	}

	return ListenAndServeWith(binder, addr, opts...)
}

func (s *Server) Close() error {
	close(s.done)
	err := s.listener.Close()
	s.wg.Wait()
	close(s.ch)
	return err
}

func (s *Server) Receive() *Batch {
	select {
	case <-s.done:
		return nil
	case b := <-s.ch:
		return b
	}
}

func (s *Server) ReceiveChan() <-chan *Batch {
	return s.ch
}

func (s *Server) run() {
	defer s.wg.Done()

	for {
		client, err := s.listener.Accept()
		if err != nil {
			break
		}

		conn := newConn(s, client, s.opts.timeout, s.opts.keepalive)
		s.wg.Add(1)
		go conn.run()
	}
}

func newConn(server *Server, c net.Conn, to, keepalive time.Duration) *conn {
	conn := &conn{
		server:    server,
		c:         c,
		to:        to,
		keepalive: keepalive,
		reader:    newReader(c, server.opts),
		signal:    make(chan struct{}),
		ch:        make(chan *Batch),
	}
	return conn
}

func (c *conn) run() {
	go func() {
		defer c.server.wg.Done()
		defer close(c.signal)

		if err := c.handle(); err != nil {
			log.Print(err)
		}
	}()

	go c.ackLoop()

	select {
	case <-c.signal: // client connection closed
	case <-c.server.done: // handle server shutdown
	}
	_ = c.c.Close()
}

func (c *conn) handle() error {
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
		case <-c.server.done:
			return nil
		case c.ch <- b:
		}

		// 3. push batch to server receive queue:
		select {
		case <-c.server.done:
			return nil
		case c.server.ch <- b:
		}
	}
}

func (c *conn) ackLoop() {
	// drain queue on shutdown.
	// Stop ACKing batches in case of error, forcing client to reconnect
	defer func() {
		for range c.ch {
		}
	}()

	for {
		select {
		case <-c.signal: // return on client/server shutdown
			return
		case b := <-c.ch:
			if err := c.waitACK(b); err != nil {
				log.Printf("Stop ack loop due to error: %v", err)
				return
			}
		}
	}
}

func (c *conn) waitACK(batch *Batch) error {
	n := len(batch.Events)
	for {
		select {
		case <-c.signal:
			return nil
		case <-batch.ack:
			// send ack
			return c.sendACK(n)
		case <-time.After(c.keepalive):
			if err := c.sendACK(0); err != nil {
				return err
			}
		}
	}
}

func (c *conn) sendACK(n int) error {
	var buf [6]byte
	buf[0] = protocol.CodeVersion
	buf[1] = protocol.CodeACK
	binary.BigEndian.PutUint32(buf[2:], uint32(n))

	if err := c.c.SetWriteDeadline(time.Now().Add(c.to)); err != nil {
		return err
	}

	tmp := buf[:]
	for len(tmp) > 0 {
		n, err := c.c.Write(tmp)
		if err != nil {
			return err
		}
		tmp = tmp[n:]
	}
	return nil
}

func newReader(c net.Conn, opts options) *reader {
	r := &reader{
		in:   bufio.NewReader(c),
		conn: c,
		buf:  make([]byte, 0, 64),
		opts: opts,
	}
	return r
}

func (r *reader) readBatch() (*Batch, error) {
	// 1. read window size
	var win [6]byte
	_, err := io.ReadFull(r.in, win[:])
	if err != nil {
		return nil, err
	}

	if win[0] != protocol.CodeVersion && win[1] != protocol.CodeWindowSize {
		return nil, ErrProtocolError
	}

	count := int(binary.BigEndian.Uint32(win[2:]))
	if count == 0 {
		return nil, nil
	}

	if err := r.conn.SetReadDeadline(time.Now().Add(r.opts.timeout)); err != nil {
		return nil, err
	}

	events := make([]interface{}, 0, count)
	events, err = r.readEvents(r.in, events)
	if events == nil || err != nil {
		return nil, err
	}

	batch := &Batch{
		Events: events,
		ack:    make(chan struct{}),
	}
	return batch, nil
}

func (r *reader) readEvents(in io.Reader, events []interface{}) ([]interface{}, error) {
	for len(events) < cap(events) {
		var hdr [2]byte
		_, err := io.ReadFull(r.in, hdr[:])
		if err != nil {
			return nil, err
		}

		if hdr[0] != protocol.CodeVersion {
			return nil, ErrProtocolError
		}

		switch hdr[1] {
		case 'J':
			event, err := r.readJSONEvent(in)
			if err != nil {
				return nil, err
			}
			events = append(events, event)
		case 'C':
			events, err = r.readCompressed(in, events)
			if err != nil {
				return nil, err
			}
		default:
			return nil, ErrProtocolError
		}
	}
	return events, nil
}

func (r *reader) readJSONEvent(in io.Reader) (interface{}, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(in, hdr[:]); err != nil {
		return nil, err
	}

	payloadSz := int(binary.BigEndian.Uint32(hdr[4:]))
	if payloadSz > len(r.buf) {
		r.buf = make([]byte, payloadSz)
	}

	buf := r.buf[:payloadSz]
	if _, err := io.ReadFull(in, buf); err != nil {
		return nil, err
	}

	var event interface{}
	err := r.opts.decoder(buf, &event)
	return event, err
}

func (r *reader) readCompressed(in io.Reader, events []interface{}) ([]interface{}, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(in, hdr[:]); err != nil {
		return nil, err
	}

	payloadSz := binary.BigEndian.Uint32(hdr[:])
	reader, err := zlib.NewReader(io.LimitReader(in, int64(payloadSz)))
	if err != nil {
		log.Printf("Failed to initialize zlib reader %v", err)
		return nil, err
	}

	events, err = r.readEvents(reader, events)
	if err != nil {
		_ = reader.Close()
		return nil, err
	}

	if err := reader.Close(); err != nil {
		log.Printf("Failed to close zlib reader with %v", err)
		return nil, err
	}

	return events, nil
}
