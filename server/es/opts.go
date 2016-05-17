package es

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/urso/go-lumber/lj"
)

type Option func(*options) error

type options struct {
	timeout time.Duration
	tls     *tls.Config
	ch      chan *lj.Batch
	split   int
	silent  bool
}

type jsonDecoder func([]byte, interface{}) error

func Channel(c chan *lj.Batch) Option {
	return func(opt *options) error {
		opt.ch = c
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

func Split(n int) Option {
	return func(opt *options) error {
		if n <= 0 {
			return errors.New("batch split must be >0")
		}
		opt.split = n
		return nil
	}
}

func Silent(b bool) Option {
	return func(opt *options) error {
		opt.silent = b
		return nil
	}
}

func applyOptions(opts []Option) (options, error) {
	o := options{
		split: 2048,
	}

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return o, err
		}
	}
	return o, nil
}
