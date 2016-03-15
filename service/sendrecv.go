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
	"fmt"
	"io"
	"net"
	"time"

	"github.com/surgemq/message"
)

type netReader interface {
	io.Reader
	SetReadDeadline(t time.Time) error
}

type timeoutReader struct {
	d    time.Duration
	conn netReader
}

func (r timeoutReader) Read(b []byte) (int, error) {
	if err := r.conn.SetReadDeadline(time.Now().Add(r.d)); err != nil {
		return 0, err
	}
	return r.conn.Read(b)
}

// receiver() reads data from the network, and writes the data into the incoming buffer
func (this *service) receiver() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			Log.Errorc(func() string { return fmt.Sprintf("(%s) Recovering from panic: %v", this.cid(), r) })
		}

		this.wgStopped.Done()

		Log.Debugc(func() string { return fmt.Sprintf("(%s) Stopping receiver", this.cid()) })
	}()

	//   Log.Debugc(func() string{ return fmt.Sprintf("(%s) Starting receiver", this.cid())})

	this.wgStarted.Done()

	switch conn := this.conn.(type) {
	case net.Conn:
		//Log.Debugc(func() string{ return fmt.Sprintf("server/handleConnection: Setting read deadline to %d", time.Second*time.Duration(this.keepAlive))})
		keepAlive := time.Second * time.Duration(this.keepAlive)
		r := timeoutReader{
			d:    keepAlive + (keepAlive / 2),
			conn: conn,
		}

		for {
			_, err := this.in.ReadFrom(r)
			//       Log.Errorc(func() string{ return fmt.Sprintf("this.sess is: %v", this.sess)})
			//       Log.Errorc(func() string{ return fmt.Sprintf("this.sessMgr is: %v", this.sessMgr)})

			if err != nil {
				if err != io.EOF {
					Log.Infoc(func() string { return fmt.Sprintf("(%s) error reading from connection: %v", this.cid(), err) })
				}
				return
			}
		}

	//case *websocket.Conn:
	//	Log.Errorc(func() string{ return fmt.Sprintf("(%s) Websocket: %v", this.cid(), ErrInvalidConnectionType)})

	default:
		Log.Errorc(func() string { return fmt.Sprintf("(%s) %v", this.cid(), ErrInvalidConnectionType) })
	}
}

// sender() writes data from the outgoing buffer to the network
func (this *service) sender() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			Log.Errorc(func() string { return fmt.Sprintf("(%s) Recovering from panic: %v", this.cid(), r) })
		}

		this.wgStopped.Done()

		Log.Debugc(func() string { return fmt.Sprintf("(%s) Stopping sender", this.cid()) })
	}()

	//   Log.Debugc(func() string{ return fmt.Sprintf("(%s) Starting sender", this.cid())})

	this.wgStarted.Done()

	switch conn := this.conn.(type) {
	case net.Conn:
		for {
			_, err := this.out.WriteTo(conn)

			if err != nil {
				if err != io.EOF {
					Log.Errorc(func() string { return fmt.Sprintf("(%s) error writing data: %v", this.cid(), err) })
				}
				return
			}
		}
	default:
		Log.Errorc(func() string { return fmt.Sprintf("(%s) Invalid connection type", this.cid()) })
	}
}

// writeMessage() writes a message to the outgoing buffer
func (this *service) writeMessage(msg message.Message) (int, error) {
	var (
		l   int = msg.Len()
		n   int
		err error
		buf []byte
	)

	if this.out == nil {
		return 0, ErrBufferNotReady
	}

	// This is to serialize writes to the underlying buffer. Multiple goroutines could
	// potentially get here because of calling Publish() or Subscribe() or other
	// functions that will send messages. For example, if a message is received in
	// another connetion, and the message needs to be published to this client, then
	// the Publish() function is called, and at the same time, another client could
	// do exactly the same thing.
	//
	// Not an ideal fix though. If possible we should remove mutex and be lockfree.
	// Mainly because when there's a large number of goroutines that want to publish
	// to this client, then they will all block. However, this will do for now.
	//
	// FIXME: Try to find a better way than a mutex...if possible.
	this.wmu.Lock()
	defer this.wmu.Unlock()
	buf = make([]byte, l)
	n, err = msg.Encode(buf[0:])
	if err != nil {
		return 0, err
	}

	//m, err = this.out.WriteCommit(n)
	ok := this.out.WriteBuffer(&buf)
	if !ok {
		return 0, err
	}
	//}

	this.outStat.increment(int64(n))

	return n, nil
}
