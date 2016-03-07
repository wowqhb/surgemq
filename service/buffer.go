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
	"bufio"
	"fmt"
	"io"
	"sync/atomic"
	//"github.com/surgemq/message"
	"encoding/binary"
	"math"
	"sync"
)

var (
	bufcnt            int64
	DefaultBufferSize int64

	DeviceInBufferSize  int64
	DeviceOutBufferSize int64

	MasterInBufferSize  int64
	MasterOutBufferSize int64
)

const (
	smallReadBlockSize    = 512
	defaultReadBlockSize  = 8192
	defaultWriteBlockSize = 8192
)

/**
2016.03.03 修改
bingbuffer结构体
*/
type buffer struct {
	readIndex  int64        //读序号
	writeIndex int64        //写序号
	ringBuffer []*ByteArray //环形buffer指针数组
	bufferSize int64        //初始化环形buffer指针数组大小
	mask       int64        //掩码：bufferSize-1
	done       int64        //是否完成
	rcond      *sync.Cond
	wcond      *sync.Cond
}

type ByteArray struct {
	bArray []byte
}

func (this *buffer) ReadCommit(index int64) {
	this.rcond.L.Lock()
	defer this.rcond.L.Unlock()
	this.ringBuffer[index] = nil
	this.rcond.Broadcast()
	this.wcond.Broadcast()
}

func (this *buffer) Len() int {
	cpos := this.GetCurrentReadIndex()
	ppos := this.GetCurrentWriteIndex()
	return int(ppos - cpos)
}

/**
2016.03.03 添加
初始化ringbuffer
参数bufferSize：初始化环形buffer指针数组大小
*/
func newBuffer(size int64) (*buffer, error) {
	if size < 0 {
		return nil, bufio.ErrNegativeCount
	}
	if size == 0 {
		size = DefaultBufferSize
	}
	if !powerOfTwo64(size) {
		fmt.Printf("Size must be power of two. Try %d.", roundUpPowerOfTwo64(size))
		return nil, fmt.Errorf("Size must be power of two. Try %d.", roundUpPowerOfTwo64(size))
	}

	return &buffer{
		readIndex:  int64(0),                 //读序号
		writeIndex: int64(0),                 //写序号
		ringBuffer: make([]*ByteArray, size), //环形buffer指针数组
		bufferSize: size,                     //初始化环形buffer指针数组大小
		mask:       size - 1,
		rcond:      sync.NewCond(new(sync.Mutex)),
		wcond:      sync.NewCond(new(sync.Mutex)),
	}, nil
}

/**
2016.03.03 添加
获取当前读序号
*/
func (this *buffer) GetCurrentReadIndex() int64 {
	return atomic.LoadInt64(&this.readIndex)
}

/**
2016.03.03 添加
获取当前写序号
*/
func (this *buffer) GetCurrentWriteIndex() int64 {
	return atomic.LoadInt64(&this.writeIndex)
}

/**
2016.03.03 添加
读取ringbuffer指定的buffer指针，返回该指针并清空ringbuffer该位置存在的指针内容，以及将读序号加1
*/
func (this *buffer) ReadBuffer() ([]byte, int64, bool) {
	this.rcond.L.Lock()
	defer func() {
		this.wcond.Broadcast()
		this.rcond.L.Unlock()
	}()

	for {
		readIndex := atomic.LoadInt64(&this.readIndex)
		writeIndex := atomic.LoadInt64(&this.writeIndex)
		switch {
		case readIndex >= writeIndex:
			this.rcond.Wait()
		case writeIndex-readIndex > this.bufferSize:
			this.rcond.Wait()
		default:
			if readIndex == math.MaxInt64 {
				atomic.StoreInt64(&this.readIndex, int64(0))
			} else {
				atomic.AddInt64(&this.readIndex, int64(1))
			}

			index := readIndex & this.mask

			p_ := this.ringBuffer[index]
			//this.ringBuffer[index] = nil
			if p_ == nil {
				return nil, -1, false
			}
			p := p_.bArray

			if p == nil {
				return nil, -1, false
			}

			return p, index, true
		}
	}

}

/**
2016.03.03 添加
写入ringbuffer指针，以及将写序号加1
*/
func (this *buffer) WriteBuffer(in []byte) bool {
	this.wcond.L.Lock()
	defer func() {
		this.rcond.Broadcast()
		this.wcond.L.Unlock()
	}()

	for {
		readIndex := atomic.LoadInt64(&this.readIndex)
		writeIndex := atomic.LoadInt64(&this.writeIndex)
		switch {
		case writeIndex-readIndex < 0:
			this.wcond.Wait()
		default:
			index := writeIndex & this.mask
			if writeIndex == math.MaxInt64 {
				atomic.StoreInt64(&this.writeIndex, int64(0))
			} else {
				atomic.AddInt64(&this.writeIndex, int64(1))
			}
			if this.ringBuffer[index] == nil {
				this.ringBuffer[index] = &ByteArray{bArray: in}
				return true
			} else {
				return false
			}
		}
	}

}

/**
2016.03.03 修改
完成
*/
func (this *buffer) Close() error {
	atomic.StoreInt64(&this.done, 1)

	this.wcond.L.Lock()
	this.rcond.Broadcast()
	this.wcond.L.Unlock()

	this.rcond.L.Lock()
	this.wcond.Broadcast()
	this.rcond.L.Unlock()

	return nil
}

/*

/**
2016.03.03 修改
向ringbuffer中写数据（从connection的中向ringbuffer中写）--生产者
*/
func (this *buffer) ReadFrom(r io.Reader) (int64, error) {
	defer this.Close()
	total := int64(0)
	//for {

	//if this.isDone() {
	//	return total, io.EOF
	//}
	b := make([]byte, int64(5))
	n, err := r.Read(b[0:1])

	if n > 0 {
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	/**************************/
	cnt := 1

	// Let's read enough bytes to get the message header (msg type, remaining length)
	for {
		// If we have read 5 bytes and still not done, then there's a problem.
		if cnt > 4 {
			return 0, fmt.Errorf("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
		}

		Log.Infoc(func() string {
			return fmt.Sprintf("sendrecv/peekMessageSize: %d=========", cnt)
		})
		// Peek cnt bytes from the input buffer.

		_, err := r.Read(b[cnt:(cnt + 1)])
		//fmt.Println(b)
		if err != nil {
			return 0, err
		}
		// If we got enough bytes, then check the last byte to see if the continuation
		// bit is set. If so, increment cnt and continue peeking
		if b[cnt] >= 0x80 {
			cnt++
		} else {
			break
		}
	}

	// Get the remaining length of the message
	remlen, m := binary.Uvarint(b[1:])
	//Log.Infoc(func() string {
	//	return fmt.Sprintf("b[cnt:(cnt + 1)]==end")
	//})
	// Total message length is remlen + 1 (msg type) + m (remlen bytes)

	total = int64(remlen) + int64(1) + int64(m)
	//Log.Infoc(func() string {
	//	return fmt.Sprintf("remlen===n===totle: %d===%d===%d", remlen, m, total)
	//})
	//mtype := message.MessageType(b[0] >> 4)
	/****************/
	//var msg message.Message
	//
	//msg, err = mtype.New()
	//if err != nil {
	//	return 0, err
	//}
	b_ := make([]byte, int64(remlen))
	_, err = r.Read(b_[0:])
	if err != nil {
		fmt.Println("写入buffer失败，total：%d", total)
		return total, err
	}
	b__ := make([]byte, 0, total)
	fmt.Println(b_)
	b__ = append(b__, b[0:1+m]...)
	b__ = append(b__, b_[0:]...)
	fmt.Println(b__)
	//n, err = msg.Decode(b)
	//if err != nil {
	//	return 0, err
	//}

	/*************************/

	if !this.WriteBuffer(b__) {
		return total, err
	}

	return total, nil
	//}
}

/**
2016.03.03 修改
*/
func (this *buffer) WriteTo(w io.Writer) (int64, error) {
	defer this.Close()
	total := int64(0)
	//for {
	//if this.isDone() {
	//	return total, io.EOF
	//}
	p, index, ok := this.ReadBuffer()
	defer this.ReadCommit(index)
	if !ok {
		return total, io.EOF
	}

	Log.Debugc(func() string {
		return fmt.Sprintf("defer this.ReadCommit(%s)", index)
	})
	Log.Debugc(func() string {
		return fmt.Sprintf("WriteTo函数》》读取*p：" + string(p))
	})

	Log.Debugc(func() string {
		return fmt.Sprintf(" WriteTo(w io.Writer)(7)")
	})
	//
	//Log.Errorc(func() string {
	//	return fmt.Sprintf("msg::" + msg.Name())
	//})
	//
	//p := make([]byte, msg.Len())
	//_, err := msg.Encode(p)
	//if err != nil {
	//	Log.Errorc(func() string {
	//		return fmt.Sprintf("msg.Encode(p)")
	//	})
	//	return total, io.EOF
	//}
	// There's some data, let's process it first
	if len(p) > 0 {
		n, err := w.Write(p)
		total += int64(n)
		Log.Debugc(func() string {
			return fmt.Sprintf("Wrote %d bytes, totaling %d bytes", n, total)
		})

		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("w.Write(p) error")
			})
			return total, err
		}
	}

	return total, nil
	//}
}

/**
2016.03.03 修改
*/
func (this *buffer) isDone() bool {
	if atomic.LoadInt64(&this.done) == 1 {
		return true
	}

	return false
}

func powerOfTwo64(n int64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func roundUpPowerOfTwo64(n int64) int64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}
