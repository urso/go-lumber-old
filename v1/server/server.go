package server

import (
	"errors"
	"net"

	"github.com/urso/go-lumber/server"
)

type Server struct {
	*server.Server
}

var (
	// ErrProtocolError is returned if an protocol error was detected in the
	// conversation with lumberjack server.
	ErrProtocolError = errors.New("lumberjack protocol error")
)

func NewWithListener(l net.Listener, opts ...Option) (*Server, error) {
	return newServer(opts, func(cfg server.Config) (*server.Server, error) {
		return server.NewWithListener(l, cfg)
	})
}

func ListenAndServeWith(
	binder func(network, addr string) (net.Listener, error),
	addr string,
	opts ...Option,
) (*Server, error) {
	return newServer(opts, func(cfg server.Config) (*server.Server, error) {
		return server.ListenAndServeWith(binder, addr, cfg)
	})
}

func ListenAndServe(addr string, opts ...Option) (*Server, error) {
	return newServer(opts, func(cfg server.Config) (*server.Server, error) {
		return server.ListenAndServe(addr, cfg)
	})
}

func newServer(
	opts []Option,
	mk func(cfg server.Config) (*server.Server, error),
) (*Server, error) {
	o, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}

	mkRW := func(client net.Conn) (server.BatchReader, server.ACKWriter, error) {
		r := newReader(client, o.timeout)
		w := newWriter(client, o.timeout)
		return r, w, nil
	}

	cfg := server.Config{
		TLS:     o.tls,
		Handler: server.DefaultHandler(0, mkRW),
		Channel: o.ch,
	}

	s, err := mk(cfg)
	return &Server{s}, err
}
