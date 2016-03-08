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
	Log.Infoc(func() string {
		return fmt.Sprintf("(%s) receiver开始", this.cid())
	})
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("(%s) Recovering from panic: %v", this.cid(), r)
			})
		}

		this.wgStopped.Done()

		Log.Debugc(func() string {
			return fmt.Sprintf("(%s) Stopping receiver", this.cid())
		})
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

			/*if err != nil {
				Log.Infoc(func() string { return fmt.Sprintf("(%s) error reading from connection: %v", this.cid(), err) })
				//         if err != io.EOF {
				//         }
				return
			}*/
			if err != nil {

				if err == io.EOF {
					Log.Errorc(func() string {
						return fmt.Sprintf("(%s) error reading from connection: %v", this.cid(), err)
					})
					return
				} else {
					Log.Infoc(func() string {
						return fmt.Sprintf("(%s) info reading from connection: %v", this.cid(), err)
					})
				}

				continue
			} else {
				Log.Infoc(func() string {
					return fmt.Sprintf("(%s)向ringbuffer些数据成功！", this.cid())
				})
			}
		}

	//case *websocket.Conn:
	//	Log.Errorc(func() string{ return fmt.Sprintf("(%s) Websocket: %v", this.cid(), ErrInvalidConnectionType)})

	default:
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) %v", this.cid(), ErrInvalidConnectionType)
		})
	}
}

// sender() writes data from the outgoing buffer to the network
func (this *service) sender() {
	Log.Infoc(func() string {
		return fmt.Sprintf("(%s) sender开始", this.cid())
	})
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("(%s) Recovering from panic: %v", this.cid(), r)
			})
		}

		this.wgStopped.Done()

		Log.Debugc(func() string {
			return fmt.Sprintf("(%s) Stopping sender", this.cid())
		})
	}()

	//   Log.Debugc(func() string{ return fmt.Sprintf("(%s) Starting sender", this.cid())})

	this.wgStarted.Done()

	switch conn := this.conn.(type) {
	case net.Conn:
		for {
			fmt.Println("this.out=", this.out)
			_, err := this.out.WriteTo(conn)

			if err != nil {
				if err != io.EOF {
					Log.Errorc(func() string {
						return fmt.Sprintf("(%s) error writing data: %v", this.cid(), err)
					})
				}
				return
			}
		}

	//case *websocket.Conn:
	//	Log.Errorc(func() string{ return fmt.Sprintf("(%s) Websocket not supported", this.cid())})

	default:
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) Invalid connection type", this.cid())
		})
	}
}

// peekMessageSize() reads, but not commits, enough bytes to determine the size of
// the next message and returns the type and size.
func (this *service) peekMessageSize() (message.Message, int, error) {
	Log.Infoc(func() string {
		return fmt.Sprintf("(%s) peekMessageSize开始", this.cid())
	})
	var (
		b   []byte
		err error
		cnt int = 2
	)

	if this.in == nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) peekMessageSize this.in is nil", this.cid())
		})
		err = ErrBufferNotReady
		return nil, 0, err
	}

	// Let's read enough bytes to get the message header (msg type, remaining length)
	//for {
	// If we have read 5 bytes and still not done, then there's a problem.
	//if cnt > 5 {
	//	return 0, 0, fmt.Errorf("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
	//}

	// Peek cnt bytes from the input buffer.
	b, err = this.in.ReadWait(cnt)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) peekMessageSize this.in.ReadWait falure", this.cid())
		})
		return nil, 0, err
	}
	//fmt.Println("b=", b)
	//// If not enough bytes are returned, then continue until there's enough.
	//if len(b) < cnt {
	//	continue
	//}

	// If we got enough bytes, then check the last byte to see if the continuation
	// bit is set. If so, increment cnt and continue peeking
	/*if b[cnt-1] >= 0x80 {
		cnt++
	} else {
		break
	}*/
	//for {
	//	if cnt > 5 {
	//		return 0, 0, fmt.Errorf("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
	//	}
	//
	//	if b[cnt-1] >= 0x80 {
	//		cnt++
	//	} else {
	//		break
	//	}
	//}
	//}

	// Get the remaining length of the message
	//remlen, m := binary.Uvarint(b[1:cnt])

	// Total message length is remlen + 1 (msg type) + m (remlen bytes)
	//total := int(remlen) + 1 + m

	mtype := message.MessageType(b[0] >> 4)

	//return mtype, total, err
	var msg message.Message
	msg, err = mtype.New()
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) peekMessageSize mtype.New() falure", this.cid())
		})
		return nil, 0, err
	}
	Log.Infoc(func() string {
		return fmt.Sprintf("(%s) 开始创建对象(%s)", this.cid(), msg.Name())
	})
	_, err = msg.Decode(b)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("(%s) peekMessageSize msg.Decode falure", this.cid())
		})
		return nil, 0, err
	}
	Log.Infoc(func() string {
		return fmt.Sprintf("(%s) peekMessageSize结束(%s)", this.cid(), msg.Name())
	})
	return msg, len(b), err
}

// peekMessage() reads a message from the buffer, but the bytes are NOT committed.
// This means the buffer still thinks the bytes are not read yet.
/*func (this *service) peekMessage(mtype message.MessageType, total int) (message.Message, int, error) {
	var (
		b    []byte
		err  error
		i, n int
		msg  message.Message
	)

	if this.in == nil {
		return nil, 0, ErrBufferNotReady
	}

	// Peek until we get total bytes
	for i = 0; ; i++ {
		// Peek remlen bytes from the input buffer.
		b, err = this.in.ReadWait(total)
		if err != nil && err != ErrBufferInsufficientData {
			return nil, 0, err
		}

		// If not enough bytes are returned, then continue until there's enough.
		if len(b) >= total {
			break
		}
	}

	msg, err = mtype.New()
	if err != nil {
		return nil, 0, err
	}

	n, err = msg.Decode(b)
	return msg, n, err
}*/

// readMessage() reads and copies a message from the buffer. The buffer bytes are
// committed as a result of the read.
func (this *service) readMessage(mtype message.MessageType, total int) (message.Message, int, error) {
	var (
		b   []byte
		err error
		n   int
		msg message.Message
	)

	if this.in == nil {
		err = ErrBufferNotReady
		return nil, 0, err
	}

	if len(this.intmp) < total {
		this.intmp = make([]byte, total)
	}

	// Read until we get total bytes
	l := 0
	for l < total {
		n, err = this.in.Read(this.intmp[l:])
		l += n
		Log.Debugc(func() string {
			return fmt.Sprintf("read %d bytes, total %d", n, l)
		})
		if err != nil {
			return nil, 0, err
		}
	}

	b = this.intmp[:total]

	msg, err = mtype.New()
	if err != nil {
		return msg, 0, err
	}

	n, err = msg.Decode(b)
	return msg, n, err
}

// writeMessage() writes a message to the outgoing buffer
func (this *service) writeMessage(msg message.Message) (int, error) {
	var (
		l    int = msg.Len()
		m, n int
		err  error
		buf  []byte
		wrap bool
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

	buf, wrap, err = this.out.WriteWait(l)
	if err != nil {
		return 0, err
	}

	if wrap {
		if len(this.outtmp) < l {
			this.outtmp = make([]byte, l)
		}

		n, err = msg.Encode(this.outtmp[0:])
		if err != nil {
			return 0, err
		}

		m, err = this.out.Write(this.outtmp[0:n])
		if err != nil {
			return m, err
		}
	} else {
		n, err = msg.Encode(buf[0:])
		if err != nil {
			return 0, err
		}

		m, err = this.out.WriteCommit(n)
		if err != nil {
			return 0, err
		}
	}

	this.outStat.increment(int64(m))

	return m, nil
}
