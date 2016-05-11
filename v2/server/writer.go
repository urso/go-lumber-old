package server

import (
	"encoding/binary"
	"net"
	"time"

	"github.com/urso/go-lumber/v2/protocol"
)

type writer struct {
	c  net.Conn
	to time.Duration
}

func newWriter(c net.Conn, to time.Duration) *writer {
	return &writer{c: c, to: to}
}

func (w *writer) ACK(n int) error {
	var buf [6]byte
	buf[0] = protocol.CodeVersion
	buf[1] = protocol.CodeACK
	binary.BigEndian.PutUint32(buf[2:], uint32(n))

	if err := w.c.SetWriteDeadline(time.Now().Add(w.to)); err != nil {
		return err
	}

	tmp := buf[:]
	for len(tmp) > 0 {
		n, err := w.c.Write(tmp)
		if err != nil {
			return err
		}
		tmp = tmp[n:]
	}
	return nil
}