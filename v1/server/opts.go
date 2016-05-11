package server

import (
	"crypto/tls"
	"errors"
	"time"
)

type Option func(*options) error

type options struct {
	timeout time.Duration
	tls     *tls.Config
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
		timeout: 30 * time.Second,
		tls:     nil,
	}

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return o, err
		}
	}
	return o, nil
}
