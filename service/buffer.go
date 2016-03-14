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
	bufcnt            int64
	DefaultBufferSize int64

	DeviceInBufferSize  int64
	DeviceOutBufferSize int64

	MasterInBufferSize  int64
	MasterOutBufferSize int64
)

const (
	smallRWBlockSize      = 512
	defaultReadBlockSize  = 8192
	defaultWriteBlockSize = 8192
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
	id int64

	readIndex  int64 //读序号
	writeIndex int64 //写序号
	buf        []*[]byte
	//tmp []byte

	size int64
	mask int64

	done int64

	//pseq *sequence
	//cseq *sequence

	pcond *sync.Cond
	ccond *sync.Cond

	//cwait int64
	//pwait int64

	//readblocksize  int
	//writeblocksize int
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

	/*var readblocksize, writeblocksize int
	if size <= 8192 {
		readblocksize = smallRWBlockSize
		writeblocksize = smallRWBlockSize
	} else {
		readblocksize = defaultReadBlockSize
		writeblocksize = defaultWriteBlockSize
	}*/

	/*if size < int64(2*readblocksize) {
		fmt.Printf("Size must at least be %d. Try %d.", 2*readblocksize, 2*readblocksize)
		return nil, fmt.Errorf("Size must at least be %d. Try %d.", 2*readblocksize, 2*readblocksize)
	}*/

	return &buffer{
		id:         atomic.AddInt64(&bufcnt, 1),
		readIndex:  int64(0),
		writeIndex: int64(0),
		buf:        make([]*[]byte, size),
		size:       size,
		mask:       size - 1,
		//pseq:           newSequence(),
		//cseq:           newSequence(),
		//readblocksize:  readblocksize,
		//writeblocksize: writeblocksize,
		pcond: sync.NewCond(new(sync.Mutex)),
		ccond: sync.NewCond(new(sync.Mutex)),
		//cwait:          0,
		//pwait:          0,
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
		this.ccond.L.Unlock()
	}()
	ok = false
	p = nil
	readIndex := this.GetCurrentReadIndex()
	writeIndex := this.GetCurrentWriteIndex()
	for {
		writeIndex = this.GetCurrentWriteIndex()
		if readIndex >= writeIndex {
			fmt.Println("read wait")
			this.pcond.Broadcast()
			//this.ccond.Wait()
			time.Sleep(2 * time.Millisecond)
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
		this.pcond.L.Unlock()
	}()
	ok = false
	readIndex := this.GetCurrentReadIndex()
	writeIndex := this.GetCurrentWriteIndex()
	for {
		readIndex = this.GetCurrentReadIndex()
		if writeIndex >= readIndex && writeIndex-readIndex >= this.size {
			fmt.Println("write wait")
			this.ccond.Broadcast()
			this.pcond.Wait()
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
		remlen, m := binary.Uvarint(b[1 : max_cnt+1])
		remlen_tmp := int64(remlen)
		total_tmp := remlen_tmp + int64(1) + int64(m)

		write_bytes = make([]byte, 0, total_tmp)
		write_bytes = append(write_bytes, b[0:m+1]...)
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

		/*if err != ErrBufferInsufficientData && err != nil {
			return total, err
		}*/
	}
}

/*func (this *buffer) Read(p []byte) (int, error) {
	if this.isDone() && this.Len() == 0 {
		//Log.Debugc(func() string{ return fmt.Sprintf("isDone and len = %d", this.Len())})
		return 0, io.EOF
	}

	pl := int64(len(p))

	for {
		cpos := this.cseq.get()
		ppos := this.pseq.get()
		cindex := cpos & this.mask

		// If consumer position is at least len(p) less than producer position, that means
		// we have enough data to fill p. There are two scenarios that could happen:
		// 1. cindex + len(p) < buffer size, in this case, we can just copy() data from
		//    buffer to p, and copy will just copy enough to fill p and stop.
		//    The number of bytes copied will be len(p).
		// 2. cindex + len(p) > buffer size, this means the data will wrap around to the
		//    the beginning of the buffer. In thise case, we can also just copy data from
		//    buffer to p, and copy will just copy until the end of the buffer and stop.
		//    The number of bytes will NOT be len(p) but less than that.
		if cpos+pl < ppos {
			n := copy(p, this.buf[cindex:])

			this.cseq.set(cpos + int64(n))
			this.pcond.L.Lock()
			this.pcond.Broadcast()
			this.pcond.L.Unlock()

			return n, nil
		}

		// If we got here, that means there's not len(p) data available, but there might
		// still be data.

		// If cpos < ppos, that means there's at least ppos-cpos bytes to read. Let's just
		// send that back for now.
		if cpos < ppos {
			// n bytes available
			b := ppos - cpos

			// bytes copied
			var n int

			// if cindex+n < size, that means we can copy all n bytes into p.
			// No wrapping in this case.
			if cindex+b < this.size {
				n = copy(p, this.buf[cindex:cindex+b])
			} else {
				// If cindex+n >= size, that means we can copy to the end of buffer
				n = copy(p, this.buf[cindex:])
			}

			this.cseq.set(cpos + int64(n))
			this.pcond.L.Lock()
			this.pcond.Broadcast()
			this.pcond.L.Unlock()
			return n, nil
		}

		// If we got here, that means cpos >= ppos, which means there's no data available.
		// If so, let's wait...

		this.ccond.L.Lock()
		for ppos = this.pseq.get(); cpos >= ppos; ppos = this.pseq.get() {
			if this.isDone() {
				return 0, io.EOF
			}

			this.cwait++
			this.ccond.Wait()
		}
		this.ccond.L.Unlock()
	}
}*/

/*func (this *buffer) Write(p []byte) (int, error) {
	if this.isDone() {
		return 0, io.EOF
	}

	start, _, err := this.waitForWriteSpace(len(p))
	if err != nil {
		return 0, err
	}

	// If we are here that means we now have enough space to write the full p.
	// Let's copy from p into this.buf, starting at position ppos&this.mask.
	total := ringCopy(this.buf, p, int64(start)&this.mask)

	this.pseq.set(start + int64(len(p)))
	this.ccond.L.Lock()
	this.ccond.Broadcast()
	this.ccond.L.Unlock()

	return total, nil
}*/

// Description below is copied completely from bufio.Peek()
//   http://golang.org/pkg/bufio/#Reader.Peek
// Peek returns the next n bytes without advancing the reader. The bytes stop being valid
// at the next read call. If Peek returns fewer than n bytes, it also returns an error
// explaining why the read is short. The error is bufio.ErrBufferFull if n is larger than
// b's buffer size.
// If there's not enough data to peek, error is ErrBufferInsufficientData.
// If n < 0, error is bufio.ErrNegativeCount
/*func (this *buffer) ReadPeek(n int) ([]byte, error) {
	if int64(n) > this.size {
		return nil, bufio.ErrBufferFull
	}

	if n < 0 {
		return nil, bufio.ErrNegativeCount
	}

	cpos := this.cseq.get()
	ppos := this.pseq.get()

	// If there's no data, then let's wait until there is some data
	this.ccond.L.Lock()
	for ; cpos >= ppos; ppos = this.pseq.get() {
		if this.isDone() {
			return nil, io.EOF
		}

		this.cwait++
		this.ccond.Wait()
	}
	this.ccond.L.Unlock()

	// m = the number of bytes available. If m is more than what's requested (n),
	// then we make m = n, basically peek max n bytes
	m := ppos - cpos
	err := error(nil)

	if m >= int64(n) {
		m = int64(n)
	} else {
		err = ErrBufferInsufficientData
	}

	// There's data to peek. The size of the data could be <= n.
	if cpos+m <= ppos {
		cindex := cpos & this.mask

		// If cindex (index relative to buffer) + n is more than buffer size, that means
		// the data wrapped
		if cindex+m > this.size {
			// reset the tmp buffer
			this.tmp = this.tmp[0:0]

			l := len(this.buf[cindex:])
			this.tmp = append(this.tmp, this.buf[cindex:]...)
			this.tmp = append(this.tmp, this.buf[0:m-int64(l)]...)
			return this.tmp, err
		} else {
			return this.buf[cindex : cindex+m], err
		}
	}

	return nil, ErrBufferInsufficientData
}*/

// Wait waits for for n bytes to be ready. If there's not enough data, then it will
// wait until there's enough. This differs from ReadPeek or Readin that Peek will
// return whatever is available and won't wait for full count.
/*func (this *buffer) ReadWait(n int) ([]byte, error) {
	if int64(n) > this.size {
		return nil, bufio.ErrBufferFull
	}

	if n < 0 {
		return nil, bufio.ErrNegativeCount
	}

	cpos := this.cseq.get()
	ppos := this.pseq.get()

	// This is the magic read-to position. The producer position must be equal or
	// greater than the next position we read to.
	next := cpos + int64(n)

	// If there's no data, then let's wait until there is some data
	this.ccond.L.Lock()
	for ; next > ppos; ppos = this.pseq.get() {
		if this.isDone() {
			return nil, io.EOF
		}

		this.ccond.Wait()
	}
	this.ccond.L.Unlock()

	// If we are here that means we have at least n bytes of data available.
	cindex := cpos & this.mask

	// If cindex (index relative to buffer) + n is more than buffer size, that means
	// the data wrapped
	if cindex+int64(n) > this.size {
		// reset the tmp buffer
		this.tmp = this.tmp[0:0]

		l := len(this.buf[cindex:])
		this.tmp = append(this.tmp, this.buf[cindex:]...)
		this.tmp = append(this.tmp, this.buf[0:n-l]...)
		return this.tmp[:n], nil
	}

	return this.buf[cindex : cindex+int64(n)], nil
}*/

// Commit moves the cursor forward by n bytes. It behaves like Read() except it doesn't
// return any data. If there's enough data, then the cursor will be moved forward and
// n will be returned. If there's not enough data, then the cursor will move forward
// as much as possible, then return the number of positions (bytes) moved.
/*func (this *buffer) ReadCommit(n int) (int, error) {
	if int64(n) > this.size {
		return 0, bufio.ErrBufferFull
	}

	if n < 0 {
		return 0, bufio.ErrNegativeCount
	}

	cpos := this.cseq.get()
	ppos := this.pseq.get()

	// If consumer position is at least n less than producer position, that means
	// we have enough data to fill p. There are two scenarios that could happen:
	// 1. cindex + n < buffer size, in this case, we can just copy() data from
	//    buffer to p, and copy will just copy enough to fill p and stop.
	//    The number of bytes copied will be len(p).
	// 2. cindex + n > buffer size, this means the data will wrap around to the
	//    the beginning of the buffer. In thise case, we can also just copy data from
	//    buffer to p, and copy will just copy until the end of the buffer and stop.
	//    The number of bytes will NOT be len(p) but less than that.
	if cpos+int64(n) <= ppos {
		this.cseq.set(cpos + int64(n))
		this.pcond.L.Lock()
		this.pcond.Broadcast()
		this.pcond.L.Unlock()
		return n, nil
	}

	return 0, ErrBufferInsufficientData
}*/

// WaitWrite waits for n bytes to be available in the buffer and then returns
// 1. the slice pointing to the location in the buffer to be filled
// 2. a boolean indicating whether the bytes available wraps around the ring
// 3. any errors encountered. If there's error then other return values are invalid
/*func (this *buffer) WriteWait(n int) ([]byte, bool, error) {
	start, cnt, err := this.waitForWriteSpace(n)
	if err != nil {
		return nil, false, err
	}

	pstart := start & this.mask
	if pstart+int64(cnt) > this.size {
		return this.buf[pstart:], true, nil
	}

	return this.buf[pstart : pstart+int64(cnt)], false, nil
}*/

/*func (this *buffer) WriteCommit(n int) (int, error) {
	start, cnt, err := this.waitForWriteSpace(n)
	if err != nil {
		return 0, err
	}

	// If we are here then there's enough bytes to commit
	this.pseq.set(start + int64(cnt))

	this.ccond.L.Lock()
	this.ccond.Broadcast()
	this.ccond.L.Unlock()

	return cnt, nil
}*/

/*func (this *buffer) waitForWriteSpace(n int) (int64, int, error) {
	if this.isDone() {
		return 0, 0, io.EOF
	}

	// The current producer position, remember it's a forever inreasing int64,
	// NOT the position relative to the buffer
	ppos := this.pseq.get()

	// The next producer position we will get to if we write len(p)
	next := ppos + int64(n)

	// For the producer, gate is the previous consumer sequence.
	gate := this.pseq.gate

	wrap := next - this.size

	// If wrap point is greater than gate, that means the consumer hasn't read
	// some of the data in the buffer, and if we read in additional data and put
	// into the buffer, we would overwrite some of the unread data. It means we
	// cannot do anything until the customers have passed it. So we wait...
	//
	// Let's say size = 16, block = 4, ppos = 0, gate = 0
	//   then next = 4 (0+4), and wrap = -12 (4-16)
	//   _______________________________________________________________________
	//   | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
	//   -----------------------------------------------------------------------
	//    ^                ^
	//    ppos,            next
	//    gate
	//
	// So wrap (-12) > gate (0) = false, and gate (0) > ppos (0) = false also,
	// so we move on (no waiting)
	//
	// Now if we get to ppos = 14, gate = 12,
	// then next = 18 (4+14) and wrap = 2 (18-16)
	//
	// So wrap (2) > gate (12) = false, and gate (12) > ppos (14) = false aos,
	// so we move on again
	//
	// Now let's say we have ppos = 14, gate = 0 still (nothing read),
	// then next = 18 (4+14) and wrap = 2 (18-16)
	//
	// So wrap (2) > gate (0) = true, which means we have to wait because if we
	// put data into the slice to the wrap point, it would overwrite the 2 bytes
	// that are currently unread.
	//
	// Another scenario, let's say ppos = 100, gate = 80,
	// then next = 104 (100+4) and wrap = 88 (104-16)
	//
	// So wrap (88) > gate (80) = true, which means we have to wait because if we
	// put data into the slice to the wrap point, it would overwrite the 8 bytes
	// that are currently unread.
	//
	if wrap > gate || gate > ppos {
		var cpos int64
		this.pcond.L.Lock()
		for cpos = this.cseq.get(); wrap > cpos; cpos = this.cseq.get() {
			if this.isDone() {
				return 0, 0, io.EOF
			}

			this.pwait++
			this.pcond.Wait()
		}

		this.pseq.gate = cpos
		this.pcond.L.Unlock()
	}

	return ppos, n, nil
}*/

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
