// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	bs "bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	//   gzip "github.com/klauspost/pgzip"
	"io"
	"io/ioutil"
	"net"

	"github.com/surgemq/message"
)

var (
	CompressLevel int
)

func getConnectMessage(conn io.Closer) (*message.ConnectMessage, error) {
	buf, err := getMessageBuffer(conn)
	if err != nil {
		//Log.Debugc(func() string{ return fmt.Sprintf("Receive error: %v", err)})
		return nil, err
	}

	msg := message.NewConnectMessage()

	_, err = msg.Decode(buf)
	//Log.Debugc(func() string{ return fmt.Sprintf("Received: %s", msg)})
	return msg, err
}

func getConnackMessage(conn io.Closer) (*message.ConnackMessage, error) {
	buf, err := getMessageBuffer(conn)
	if err != nil {
		//Log.Debugc(func() string{ return fmt.Sprintf("Receive error: %v", err)})
		return nil, err
	}

	msg := message.NewConnackMessage()

	_, err = msg.Decode(buf)
	//Log.Debugc(func() string{ return fmt.Sprintf("Received: %s", msg)})
	return msg, err
}

func writeMessage(conn io.Closer, msg message.Message) error {
	buf := make([]byte, msg.Len())
	_, err := msg.Encode(buf)
	if err != nil {
		//Log.Debugc(func() string{ return fmt.Sprintf("Write error: %v", err)})
		return err
	}
	//Log.Debugc(func() string{ return fmt.Sprintf("Writing: %s", msg)})

	return writeMessageBuffer(conn, buf)
}

func getMessageBuffer(c io.Closer) ([]byte, error) {
	if c == nil {
		return nil, ErrInvalidConnectionType
	}

	conn, ok := c.(net.Conn)
	if !ok {
		return nil, ErrInvalidConnectionType
	}

	var (
		// the message buffer
		buf []byte

		// tmp buffer to read a single byte
		b []byte = make([]byte, 1)

		// total bytes read
		l int = 0
	)

	// Let's read enough bytes to get the message header (msg type, remaining length)
	for {
		// If we have read 5 bytes and still not done, then there's a problem.
		if l > 5 {
			return nil, fmt.Errorf("connect/getMessage: 4th byte of remaining length has continuation bit set")
		}

		n, err := conn.Read(b[0:])
		if err != nil {
			//Log.Debugc(func() string{ return fmt.Sprintf("Read error: %v", err)})
			return nil, err
		}

		// Technically i don't think we will ever get here
		if n == 0 {
			continue
		}

		buf = append(buf, b...)
		l += n

		// Check the remlen byte (1+) to see if the continuation bit is set. If so,
		// increment cnt and continue reading. Otherwise break.
		if l > 1 && b[0] < 0x80 {
			break
		}
	}

	// Get the remaining length of the message
	remlen, _ := binary.Uvarint(buf[1:])
	buf = append(buf, make([]byte, remlen)...)

	for l < len(buf) {
		n, err := conn.Read(buf[l:])
		if err != nil {
			return nil, err
		}
		l += n
	}

	return buf, nil
}

func writeMessageBuffer(c io.Closer, b []byte) error {
	if c == nil {
		return ErrInvalidConnectionType
	}

	conn, ok := c.(net.Conn)
	if !ok {
		return ErrInvalidConnectionType
	}

	_, err := conn.Write(b)
	return err
}

// Copied from http://golang.org/src/pkg/net/timeout_test.go
func isTimeout(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}

func Gzip(bytes []byte) (compressed_bytes []byte, err error) {
	var res bs.Buffer
	gz, err := gzip.NewWriterLevel(&res, CompressLevel)
	if err != nil {
		Log.Error(func() string { return fmt.Sprintf("make zip writer err: %s\n", err) })
		return nil, err
	}

	_, err = gz.Write(bytes)
	if err != nil {
		Log.Error(func() string { return fmt.Sprintf("write zip err: %s\n", err) })
		return nil, err
	}
	gz.Close()
	compressed_bytes = res.Bytes()

	return compressed_bytes, nil
}

func Gunzip(compressed_bytes []byte) (bytes []byte, err error) {
	gz, err := gzip.NewReader(bs.NewReader(compressed_bytes))
	if err != nil {
		Log.Error(func() string { return fmt.Sprintf("unzip reader err: %s\n", err) })
		return nil, err
	}
	//   defer gz.Close()   // 如果用pgzip，这里需要用defer
	gz.Close() // 如果用内建gzip，这里不要用defer

	bytes, err = ioutil.ReadAll(gz)
	if err != nil {
		Log.Error(func() string { return fmt.Sprintf("unzip err: %s\n", err) })
		return nil, err
	}
	return bytes, nil
}
