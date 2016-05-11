package server

import (
	"crypto/tls"
	"io"
	"log"
	"net"
	"sync"

	"github.com/urso/go-lumber/lj"
	v1 "github.com/urso/go-lumber/v1/server"
	v2 "github.com/urso/go-lumber/v2/server"
)

type Server struct {
	ch chan *lj.Batch

	done chan struct{}
	wg   sync.WaitGroup

	netListener net.Listener
	l1, l2      *muxListener
	sv1         *v1.Server
	sv2         *v2.Server
}

func NewWithListener(l net.Listener, opts ...Option) (*Server, error) {
	return newServer(l, opts...)
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
	s, err := NewWithListener(l, opts...)
	if err != nil {
		l.Close()
	}
	return s, err
}

func ListenAndServe(addr string, opts ...Option) (*Server, error) {
	o, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}

	binder := net.Listen
	if o.tls != nil {
		binder = func(network, addr string) (net.Listener, error) {
			return tls.Listen(network, addr, o.tls)
		}
	}

	return ListenAndServeWith(binder, addr, opts...)
}

func (s *Server) Close() error {
	close(s.done)
	s.sv1.Close()
	s.sv2.Close()
	err := s.netListener.Close()
	s.wg.Wait()
	close(s.ch)
	return err
}

func (s *Server) ReceiveChan() <-chan *lj.Batch {
	return s.ch
}

func (s *Server) Receive() *lj.Batch {
	select {
	case <-s.done:
		return nil
	case b := <-s.ch:
		return b
	}
}

func newServer(l net.Listener, opts ...Option) (*Server, error) {
	cfg, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}

	ch := cfg.ch
	if ch == nil {
		ch = make(chan *lj.Batch, 128)
	}

	l1 := newMuxListener(l)
	l2 := newMuxListener(l)

	sv1, err := v1.NewWithListener(l1, v1.Timeout(cfg.timeout), v1.Channel(ch))
	if err != nil {
		return nil, err
	}

	sv2, err := v2.NewWithListener(l2,
		v2.Channel(ch),
		v2.Timeout(cfg.timeout),
		v2.Keepalive(cfg.keepalive),
		v2.JSONDecoder(cfg.decoder))
	if err != nil {
		sv1.Close()
		return nil, err
	}

	s := &Server{
		ch:          ch,
		netListener: l,
		l1:          l1,
		l2:          l2,
		sv1:         sv1,
		sv2:         sv2,
		done:        make(chan struct{}),
	}
	s.wg.Add(1)
	go s.run()

	return s, nil
}

func (s *Server) run() {
	defer s.wg.Done()
	for {
		client, err := s.netListener.Accept()
		if err != nil {
			break
		}

		s.handle(client)
	}
}

func (s *Server) handle(client net.Conn) {
	// read first byte and decide multiplexer

	sig := make(chan struct{})

	go func() {
		var buf [1]byte
		if _, err := io.ReadFull(client, buf[:]); err != nil {
			return
		}
		close(sig)

		conn := newMuxConn(buf[0], client)
		switch buf[0] {
		case '1':
			s.l1.ch <- conn
		case '2':
			s.l2.ch <- conn
		default:
			log.Printf("Unsupported protocol version: %v", buf[0])
			conn.Close()
			return
		}
	}()

	go func() {
		select {
		case <-sig:
		case <-s.done:
			// close connection if server being shut down
			client.Close()
		}
	}()
}
