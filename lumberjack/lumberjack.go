package lumberjack

import (
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/elastic/beats/libbeat/common/streambuf"
)

type Lumberjack struct {
	net.Listener
	timeout   time.Duration
	err       error
	handshake func(net.Conn)
}

type Message struct {
	Code   uint8
	Size   uint32
	Seq    uint32
	Events []*Message
	Doc    Document
}

type AckHandler struct {
	seq    uint32
	client  net.Conn

}

type Document map[string]interface{}

type Success func(h AckHandler) bool

type Failure func(h AckHandler) bool

type Process func(events []*Message) bool

type Handlers struct {
	Success   Success
	Failure   Failure
	Process   Process
}

func NewLumberJack(bind string, to time.Duration) (*Lumberjack, error) {
	tcpListener, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, err
	}

	server := &Lumberjack{Listener: tcpListener, timeout: to}
	server.handshake = func(client net.Conn) {}

	return server, nil
}

func AcceptConnections(server *Lumberjack, ha *Handlers) {
	for {
		client, err := server.Accept()
		if err != nil {
			fmt.Println("failed to accept client: ", err)
		} else {
			fmt.Println("Connection established with ", client.RemoteAddr())
		}

		go run(client, ha)
	}
}

func run(client net.Conn, ha *Handlers) {
	defer client.Close()

	fmt.Println("serve new client")

	var N uint32
	buf := streambuf.New(nil)
	for {
		ackHandler := AckHandler{N, client}
		msg, err := recvMessage(buf, client)
		if err != nil {
			ha.Failure(ackHandler)
			//fmt.Println("failed to read message from client: ", err)
			return
		}

		switch msg.Code {
		case 'W':
			N = msg.Size
		case 'C':
			events := msg.Events
			ha.Success(ackHandler)
			ha.Process(events)
		}

	}
}

func recvMessage(buf *streambuf.Buffer, client net.Conn) (*Message, error) {
	for {
		msg, err := readMessage(buf)
		if msg != nil || (err != nil && err != streambuf.ErrNoMoreBytes) {
			return msg, err
		}

		buffer := make([]byte, 1024)
		n, err := client.Read(buffer)
		if err != nil {
			return nil, err
		}
		buf.Write(buffer[:n])
	}
}

func readMessage(buf *streambuf.Buffer) (*Message, error) {
	if !buf.Avail(2) {
		return nil, nil
	}
	version, _ := buf.ReadNetUint8At(0)
	if version != '2' {
		return nil, errors.New("version error")
	}

	code, _ := buf.ReadNetUint8At(1)
	switch code {
	case 'W':
		if !buf.Avail(6) {
			return nil, nil
		}
		size, _ := buf.ReadNetUint32At(2)
		buf.Advance(6)
		err := buf.Err()
		buf.Reset()
		return &Message{Code: code, Size: size}, err
	case 'C':
		if !buf.Avail(6) {
			return nil, nil
		}
		len, _ := buf.ReadNetUint32At(2)

		if !buf.Avail(int(len) + 6) {
			return nil, nil
		}
		buf.Advance(6)

		tmp, _ := buf.Collect(int(len))
		buf.Reset()

		dataBuf := streambuf.New(nil)
		// decompress data
		decomp, err := zlib.NewReader(streambuf.NewFixed(tmp))
		if err != nil {
			return nil, err
		}
		// dataBuf.ReadFrom(streambuf.NewFixed(tmp))
		dataBuf.ReadFrom(decomp)
		decomp.Close()

		// unpack data
		dataBuf.Fix()
		var events []*Message
		for dataBuf.Len() > 0 {
			version, _ := dataBuf.ReadNetUint8()
			if version != '2' {
				return nil, errors.New("version error 2")
			}

			code, _ := dataBuf.ReadNetUint8()
			if code != 'J' {
				return nil, errors.New("expected json data frame")
			}

			seq, _ := dataBuf.ReadNetUint32()
			payloadLen, _ := dataBuf.ReadNetUint32()
			jsonRaw, _ := dataBuf.Collect(int(payloadLen))

			var doc interface{}
			err = json.Unmarshal(jsonRaw, &doc)
			if err != nil {
				return nil, err
			}

			events = append(events, &Message{
				Code: code,
				Seq:  seq,
				Doc:  doc.(map[string]interface{}),
			})
		}

		return &Message{Code: 'C', Events: events}, nil
	default:
		return nil, errors.New("unknown code")
	}
}

func SendAck(ack AckHandler) {
	buf := streambuf.New(nil)
	buf.WriteByte('2')
	buf.WriteByte('A')
	buf.WriteNetUint32(ack.seq)
	ack.client.Write(buf.Bytes())
}
