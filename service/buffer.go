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
	/*smallRWBlockSize      = 512
	  defaultReadBlockSize  = 8192
	  defaultWriteBlockSize = 8192*/
	smallRWBlockSize      = 64
	defaultReadBlockSize  = 64
	defaultWriteBlockSize = 64
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

type ByteArray struct {
	bArray []byte
}

type buffer struct {
	id int64

	//buf []byte
	buf []ByteArray //环形buffer指针数组
	//tmp []byte
	tmp  []ByteArray //环形buffer指针数组--临时
	size int64
	mask int64

	done int64

	pseq *sequence
	cseq *sequence

	pcond *sync.Cond
	ccond *sync.Cond

	cwait int64
	pwait int64

	readblocksize  int
	writeblocksize int
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

	var readblocksize, writeblocksize int
	if size <= 64 {
		readblocksize = smallRWBlockSize
		writeblocksize = smallRWBlockSize
	} else {
		readblocksize = defaultReadBlockSize
		writeblocksize = defaultWriteBlockSize
	}

	//if size < int64(2*readblocksize) {
	//	fmt.Printf("Size must at least be %d. Try %d.", 2*readblocksize, 2*readblocksize)
	//	return nil, fmt.Errorf("Size must at least be %d. Try %d.", 2*readblocksize, 2*readblocksize)
	//}

	return &buffer{
		id:             atomic.AddInt64(&bufcnt, 1),
		buf:            make([]ByteArray, size),
		size:           size,
		mask:           size - 1,
		pseq:           newSequence(),
		cseq:           newSequence(),
		readblocksize:  readblocksize,
		writeblocksize: writeblocksize,
		pcond:          sync.NewCond(new(sync.Mutex)),
		ccond:          sync.NewCond(new(sync.Mutex)),
		cwait:          0,
		pwait:          0,
	}, nil
}

func (this *buffer) ID() int64 {
	return this.id
}

func (this *buffer) Close() error {
	atomic.StoreInt64(&this.done, 1)

	this.pcond.L.Lock()
	this.pcond.Broadcast()
	this.pcond.L.Unlock()

	this.pcond.L.Lock()
	this.ccond.Broadcast()
	this.pcond.L.Unlock()

	return nil
}

func (this *buffer) Len() int {
	cpos := this.cseq.get()
	ppos := this.pseq.get()
	return int(ppos - cpos)
}

func (this *buffer) ReadFrom(r io.Reader) (int64, error) {
	defer func() {
		if r := recover(); r != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("ReadFrom from panic: %v", r)
			})
		}
		Log.Infoc(func() string {
			return fmt.Sprintf("ReadFrom::::defer::::close")
		})
		this.Close()
	}()

	total := int64(0)
	cnt_ := 1 //每次从conn中读取数据的字节数
	for {
		Log.Infoc(func() string {
			return fmt.Sprintf("ReadFrom开始读取", total)
		})
		if this.isDone() {
			fmt.Println("ReadFrom isDone!")
			return total, io.EOF
		}
		b := make([]byte, int64(5))
		n, err := r.Read(b[0:1])
		if err != nil {
			return total, err
			//time.Sleep(2 * time.Millisecond)
			//continue
		}
		if n > 0 {
			total += int64(n)
		}
		cnt := 1
		// Let's read enough bytes to get the message header (msg type, remaining length)
		for {
			if this.isDone() {
				return total, io.EOF
			}
			// If we have read 5 bytes and still not done, then there's a problem.
			if cnt > 4 {
				return 0, fmt.Errorf("sendrecv/peekMessageSize: 4th byte of remaining length has continuation bit set")
			}
			_, err := r.Read(b[cnt:(cnt + 1)])

			//fmt.Println(b)
			if err != nil {
				time.Sleep(2 * time.Millisecond)
				continue
			}
			if b[cnt] >= 0x80 {
				cnt++
			} else {
				break
			}
		}
		remlen, m := binary.Uvarint(b[1 : cnt+1])
		remlen_64 := int64(remlen)
		total = remlen_64 + int64(1) + int64(m)
		b__ := make([]byte, 0, total)
		b__ = append(b__, b[0:1+m]...)
		nlen := int64(0)
		for nlen < remlen_64 {
			tmpm := remlen_64 - nlen

			var b_ []byte
			if tmpm < int64(cnt_) {
				b_ = make([]byte, tmpm)
			} else {
				b_ = make([]byte, cnt_)
			}

			//b_ := make([]byte, remlen)
			n, err = r.Read(b_[0:])

			if err != nil {
				if this.isDone() {
					return total, io.EOF
				}
				Log.Errorc(func() string {
					return fmt.Sprintf("从conn读取数据失败(%s)(0)", err)
				})
				time.Sleep(2 * time.Millisecond)
				continue
				//return total,err
			}
			b__ = append(b__, b_[0:]...)
			nlen += int64(n)
			total += int64(n)
		}
		fmt.Println("b=", b)
		fmt.Println("b__=", b__)
		/*if nlen == int64(0) {
			return total, err
		}*/

		//if this.buf[pstart] != nil {
		//	return total, errors.New("ringbuffer is not nil,it is readonly now")
		//}
		start, _, err := this.waitForWriteSpace(int(total) /*this.readblocksize*/)
		if err != nil {
			return total, err
		}
		pstart := start & this.mask
		//b__[len(b__)-1] = 0x1
		this.buf[pstart] = ByteArray{bArray: b__}
		_, err = this.WriteCommit(int(total) /*n*/)
		if err != nil {
			return total, err
		}
		Log.Infoc(func() string {
			return fmt.Sprintf("ReadFrom读取完成", total)
		})

	}
}

func (this *buffer) WriteTo(w io.Writer) (int64, error) {
	defer func() {
		Log.Infoc(func() string {
			return fmt.Sprintf("WriteTo::::defer::::close")
		})
		this.Close()
	}()

	total := int64(0)

	for {
		Log.Infoc(func() string {
			return fmt.Sprintf("WriteTo发送开始")
		})
		if this.isDone() {
			Log.Errorc(func() string {
				return fmt.Sprintf("WriteTo EOF")
			})
			return total, io.EOF
		}
		p, err := this.ReadPeek()
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("this.ReadPeek error(%s)", err)
			})
			return total, err
		}

		// There's some data, let's process it first
		if len(p) > 0 {

			n, err := w.Write(p)
			total += int64(n)
			//Log.Debugc(func() string{ return fmt.Sprintf("Wrote %d bytes, totaling %d bytes", n, total)})

			if err != nil {
				Log.Errorc(func() string {
					return fmt.Sprintf("w.Write(p) error(%s)", err)
				})
				return total, err
			}

			_, err = this.ReadCommit(n)
			if err != nil {
				Log.Errorc(func() string {
					return fmt.Sprintf("this.ReadCommit error(%s)", err)
				})
				return total, err
			}
		} else {
			this.pcond.L.Lock()
			this.pcond.Broadcast()
			this.pcond.L.Unlock()
		}

		if err != ErrBufferInsufficientData && err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("ErrBufferInsufficientData error(%s)", err)
			})
			return total, err
		}
		Log.Infoc(func() string {
			return fmt.Sprintf("WriteTo发送完成")
		})
	}
}

func (this *buffer) Read(p []byte) (int, error) {
	if this.isDone() && this.Len() == 0 {
		//Log.Debugc(func() string{ return fmt.Sprintf("isDone and len = %d", this.Len())})
		return 0, io.EOF
	}

	//pl := int64(len(p))

	for {
		cpos := this.cseq.get()
		ppos := this.pseq.get()
		cindex := cpos & this.mask

		if cpos < ppos {
			n := copy(p, this.buf[cindex].bArray)

			this.cseq.set(cpos + int64(1 /*n*/))
			this.pcond.L.Lock()
			this.pcond.Broadcast()
			this.pcond.L.Unlock()

			return n, nil
		}

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
}

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
	//total := ringCopy(*(this.buf[start]), p, int64(start)&this.mask)
	//p_ := make([]byte, 0, len(p))
	//p_ = append(p_, p[0:]...)
	this.buf[int64(start)&this.mask] = ByteArray{bArray: p}
	this.pseq.set(start + int64(1))
	this.ccond.L.Lock()
	this.ccond.Broadcast()
	this.ccond.L.Unlock()

	return len(p), nil
}*/

// Description below is copied completely from bufio.Peek()
//   http://golang.org/pkg/bufio/#Reader.Peek
// Peek returns the next n bytes without advancing the reader. The bytes stop being valid
// at the next read call. If Peek returns fewer than n bytes, it also returns an error
// explaining why the read is short. The error is bufio.ErrBufferFull if n is larger than
// b's buffer size.
// If there's not enough data to peek, error is ErrBufferInsufficientData.
// If n < 0, error is bufio.ErrNegativeCount
func (this *buffer) ReadPeek() ([]byte, error) {
	return this.ReadWait()
}

// Wait waits for for n bytes to be ready. If there's not enough data, then it will
// wait until there's enough. This differs from ReadPeek or Readin that Peek will
// return whatever is available and won't wait for full count.
func (this *buffer) ReadWait() ([]byte, error) {
	Log.Debugc(func() string {
		return fmt.Sprintf("ReadWait 开始执行")
	})
	cpos := this.cseq.get()
	ppos := this.pseq.get()

	// This is the magic read-to position. The producer position must be equal or
	// greater than the next position we read to.
	next := cpos + int64(1 /*n*/)

	// If there's no data, then let's wait until there is some data
	this.ccond.L.Lock()
	for ; next > ppos; ppos = this.pseq.get() {
		if this.isDone() {
			Log.Debugc(func() string {
				return fmt.Sprintf("ReadWait::>this.isDone()==true")
			})
			return nil, io.EOF
		}
		this.cwait++
		this.ccond.Wait()
	}
	this.ccond.L.Unlock()

	// If we are here that means we have at least n bytes of data available.
	cindex := cpos & this.mask

	// If cindex (index relative to buffer) + n is more than buffer size, that means
	// the data wrapped
	/*if cindex+int64(n) > this.size {
		// reset the tmp buffer
		this.tmp = this.tmp[0:0]

		l := len(this.buf[cindex:])
		this.tmp = append(this.tmp, this.buf[cindex:]...)
		this.tmp = append(this.tmp, this.buf[0:n-l]...)
		return this.tmp[:n], nil
	}*/
	array := this.buf[cindex].bArray
	return array, nil
}

// Commit moves the cursor forward by n bytes. It behaves like Read() except it doesn't
// return any data. If there's enough data, then the cursor will be moved forward and
// n will be returned. If there's not enough data, then the cursor will move forward
// as much as possible, then return the number of positions (bytes) moved.
func (this *buffer) ReadCommit(n int) (int, error) {
	//if int64(n) > this.size {
	//	return 0, bufio.ErrBufferFull
	//}
	//
	//if n < 0 {
	//	return 0, bufio.ErrNegativeCount
	//}

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
	if cpos < ppos {
		this.cseq.set(cpos + 1)
		//this.buf[cpos] = nil
		this.pcond.L.Lock()
		this.pcond.Broadcast()
		this.pcond.L.Unlock()
		return n, nil
	}

	return 0, ErrBufferInsufficientData
}

// WaitWrite waits for n bytes to be available in the buffer and then returns
// 1. the slice pointing to the location in the buffer to be filled
// 2. a boolean indicating whether the bytes available wraps around the ring
// 3. any errors encountered. If there's error then other return values are invalid
/*func (this *buffer) WriteWait(n int) ([]byte, bool, error) {
start, _, err := this.waitForWriteSpace(n */
/*n*/
/*)
if err != nil {
       return nil, false, err
}

pstart := start & this.mask
*/
/*if pstart+int64(cnt) > this.size {
       return this.buf[pstart:], true, nil
}

return this.buf[pstart : pstart+int64(cnt)], false, nil*/
/*
       return this.buf[pstart].bArray, false, nil
}*/

func (this *buffer) WriteCommit(n int) (int, error) {
	start, _, err := this.waitForWriteSpace(n /*n*/)
	if err != nil {
		return 0, err
	}

	// If we are here then there's enough bytes to commit
	//this.pseq.set(start + int64(cnt))
	this.pseq.set(start + int64(1))
	this.ccond.L.Lock()
	this.ccond.Broadcast()
	this.ccond.L.Unlock()

	return n /*cnt*/, nil
}

func (this *buffer) waitForWriteSpace(n int) (int64, int, error) {
	if this.isDone() {
		return 0, 0, io.EOF
	}

	// The current producer position, remember it's a forever inreasing int64,
	// NOT the position relative to the buffer
	ppos := this.pseq.get()

	// The next producer position we will get to if we write len(p)
	//next := ppos + int64(n)
	next := ppos + int64(1)
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
}

func (this *buffer) isDone() bool {
	if atomic.LoadInt64(&this.done) == 1 {
		return true
	}

	return false
}

/*func ringCopy(dst, src []byte, start int64) int {
	n := len(src)

	i, l := 0, 0
	tmp := make([]byte, n)
	for n > 0 {
		l = copy(tmp[0:], src[i:])
		i += l
		n -= l
		//
		//if n > 0 {
		//	start = 0
		//}
	}
	dst[start] = &tmp
	return i
}*/

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
