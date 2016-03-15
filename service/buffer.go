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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var (
	bufcnt int64
	DefaultBufferSize int64

	DeviceInBufferSize int64
	DeviceOutBufferSize int64

	MasterInBufferSize int64
	MasterOutBufferSize int64
)

type sequence struct {
	// The current position of the producer or consumer
	cursor,

	// The previous known position of the consumer (if producer) or producer (if consumer)
	gate,

	// These are fillers to pad the cache line, which is generally 64 bytes
	p2, p3, p4, p5, p6, p7 int64
}

func newSequence() *sequence {
	return &sequence{}
}

func (this *sequence) get() int64 {
	return atomic.LoadInt64(&this.cursor)
}

func (this *sequence) set(seq int64) {
	atomic.StoreInt64(&this.cursor, seq)
}

type buffer struct {
	id         int64

	readIndex  int64 //读序号
	writeIndex int64 //写序号
	buf        []*[]byte

	size       int64
	mask       int64

	done       int64

	pcond      *sync.Cond
	ccond      *sync.Cond
}

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
		id:         atomic.AddInt64(&bufcnt, 1),
		readIndex:  int64(0),
		writeIndex: int64(0),
		buf:        make([]*[]byte, size),
		size:       size,
		mask:       size - 1,
		pcond:      sync.NewCond(new(sync.Mutex)),
		ccond:      sync.NewCond(new(sync.Mutex)),
	}, nil
}

/**
获取当前读序号
*/
func (this *buffer) GetCurrentReadIndex() int64 {
	return atomic.LoadInt64(&this.readIndex)
}

/**
获取当前写序号
*/
func (this *buffer) GetCurrentWriteIndex() int64 {
	return atomic.LoadInt64(&this.writeIndex)
}

func (this *buffer) ID() int64 {
	return this.id
}

func (this *buffer) Close() error {
	atomic.StoreInt64(&this.done, 1)

	this.pcond.L.Lock()
	this.ccond.Broadcast()
	this.pcond.L.Unlock()

	this.ccond.L.Lock()
	this.pcond.Broadcast()
	this.ccond.L.Unlock()

	return nil
}

/**
读取ringbuffer指定的buffer指针，返回该指针并清空ringbuffer该位置存在的指针内容，以及将读序号加1
*/
func (this *buffer) ReadBuffer() (p *[]byte, ok bool) {
	this.ccond.L.Lock()
	defer func() {
		this.pcond.Broadcast()
		this.ccond.L.Unlock()
		time.Sleep(5 * time.Microsecond)
	}()
	ok = false
	p = nil
	readIndex := this.GetCurrentReadIndex()
	writeIndex := this.GetCurrentWriteIndex()
	for {
		if this.isDone() {
			return nil, false
		}
		writeIndex = this.GetCurrentWriteIndex()
		if readIndex >= writeIndex {
			//fmt.Println("read wait")
			this.pcond.Broadcast()
			this.ccond.Wait()
		} else {
			break
		}
	}
	index := readIndex & this.mask //替代求模
	p = this.buf[index]
	this.buf[index] = nil
	atomic.AddInt64(&this.readIndex, int64(1))
	if p != nil {
		ok = true
	}
	return p, ok
}

/**
写入ringbuffer指针，以及将写序号加1
*/
func (this *buffer) WriteBuffer(in *[]byte) (ok bool) {
	this.pcond.L.Lock()
	defer func() {
		this.ccond.Broadcast()
		this.pcond.L.Unlock()
		time.Sleep(5 * time.Microsecond)
	}()
	ok = false
	readIndex := this.GetCurrentReadIndex()
	writeIndex := this.GetCurrentWriteIndex()
	for {
		if this.isDone() {
			return false
		}
		readIndex = this.GetCurrentReadIndex()
		if writeIndex >= readIndex && writeIndex - readIndex >= this.size {
			//fmt.Println("write wait")
			this.ccond.Broadcast()
			this.pcond.Wait()
			//time.Sleep(1 * time.Millisecond)
		} else {
			break
		}

	}
	index := writeIndex & this.mask //替代求模
	this.buf[index] = in
	atomic.AddInt64(&this.writeIndex, int64(1))
	ok = true
	return ok
}

func (this *buffer) ReadFrom(r io.Reader) (int64, error) {
	defer this.Close()

	total := int64(0)

	for {
		time.Sleep(5 * time.Millisecond)
		if this.isDone() {
			return total, io.EOF
		}

		var write_bytes []byte

		b := make([]byte, 5)
		n, err := r.Read(b[0:1])
		if err != nil {
			return total, io.EOF
		}
		total += int64(n)
		max_cnt := 1
		for {
			if this.isDone() {
				return total, io.EOF
			}
			// If we have read 5 bytes and still not done, then there's a problem.
			if max_cnt > 4 {
				return 0, fmt.Errorf("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
			}
			_, err := r.Read(b[max_cnt:(max_cnt + 1)])

			//fmt.Println(b)
			if err != nil {
				return total, err
			}
			if b[max_cnt] >= 0x80 {
				max_cnt++
			} else {
				break
			}
		}
		remlen, m := binary.Uvarint(b[1 : max_cnt + 1])
		remlen_tmp := int64(remlen)
		total_tmp := remlen_tmp + int64(1) + int64(m)

		write_bytes = make([]byte, 0, total_tmp)
		write_bytes = append(write_bytes, b[0:m + 1]...)
		nlen := int64(0)
		times := 0
		cnt_ := 32
		for nlen < remlen_tmp {
			if this.isDone() {
				return total, io.EOF
			}
			if times > 100 {
				return total, io.EOF
			} else {
				times = 0
			}
			times++
			tmpm := remlen_tmp - nlen

			var b_ []byte
			if tmpm < int64(cnt_) {
				b_ = make([]byte, tmpm)
			} else {
				b_ = make([]byte, cnt_)
			}

			//b_ := make([]byte, remlen)
			n, err = r.Read(b_[0:])

			if err != nil {
				/*Log.Errorc(func() string {
					return fmt.Sprintf("从conn读取数据失败(%s)(0)", err)
				})
				time.Sleep(5 * time.Millisecond)
				continue*/
				return total, err
			}
			write_bytes = append(write_bytes, b_[0:]...)
			nlen += int64(n)
			total += int64(n)
		}

		ok := this.WriteBuffer(&write_bytes)

		if !ok {
			return total, errors.New("write ringbuffer failed")
		}
	}
}

func (this *buffer) WriteTo(w io.Writer) (int64, error) {
	defer this.Close()

	total := int64(0)

	for {
		if this.isDone() {
			return total, io.EOF
		}

		p, ok := this.ReadBuffer()
		if !ok {
			return total, errors.New("read buffer failed")
		}
		// There's some data, let's process it first
		if len(*p) > 0 {
			n, err := w.Write(*p)
			total += int64(n)
			//Log.Debugc(func() string{ return fmt.Sprintf("Wrote %d bytes, totaling %d bytes", n, total)})

			if err != nil {
				return total, err
			}
		}

	}
}

func (this *buffer) isDone() bool {
	if atomic.LoadInt64(&this.done) == 1 {
		return true
	}

	return false
}

func ringCopy(dst, src []byte, start int64) int {
	n := len(src)

	i, l := 0, 0

	for n > 0 {
		l = copy(dst[start:], src[i:])
		i += l
		n -= l

		if n > 0 {
			start = 0
		}
	}

	return i
}

func powerOfTwo64(n int64) bool {
	return n != 0 && (n & (n - 1)) == 0
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
