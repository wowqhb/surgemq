package service

import (
	"fmt"
	"github.com/surgemq/message"
	"net"
	//   "sync"
)

var (
	PendingQueue = make([]*message.PublishMessage, 65536, 65536)
	//   PendingProcessor = make(chan *message.PublishMessage, 65536)

	OfflineTopicMap            = make(map[string]*OfflineTopicQueue)
	OfflineTopicQueueProcessor = make(chan *message.PublishMessage, 2048)
	OfflineTopicCleanProcessor = make(chan string, 2048)
	OfflineTopicPayloadUseGzip bool

	ClientMap          = make(map[string]*net.Conn)
	ClientMapProcessor = make(chan ClientHash, 1024)

	PktId = uint32(1)

	NewMessagesQueue      = make(chan *message.PublishMessage, 16384)
	SubscribersSliceQueue = make(chan []interface{}, 2048)

	Max_message_queue int
)

type ClientHash struct {
	Name string
	Conn *net.Conn
}

// 定义一个离线消息队列的结构体，保存一个二维byte数组和一个位置
type OfflineTopicQueue struct {
	Q       [][]byte
	Pos     int
	Length  int
	Cleaned bool
	Gziped  bool
	//   lock  *sync.RWMutex
}

func NewOfflineTopicQueue(length int) (q *OfflineTopicQueue) {
	q = &OfflineTopicQueue{
		make([][]byte, length, length),
		0,
		length,
		true,
		OfflineTopicPayloadUseGzip,
		//     new(sync.RWMutex),
	}

	return q
}

// 向队列中添加消息
//NOTE 因为目前是在channel中操作，所以无需加锁。如果需要并发访问，则需要加锁了。
func (this *OfflineTopicQueue) Add(msg_bytes []byte) {
	//   this.lock.Lock()
	if this.Q == nil {
		this.Q = make([][]byte, this.Length, this.Length)
	}

	// 判断是否gzip，如果是，则压缩后再存入
	if this.Gziped {
		mb, err := Gzip(msg_bytes)
		if err != nil {
			Log.Errorc(func() string { return fmt.Sprintf("gzip failed. msg: %s, err: %s", msg_bytes, err) })
			return
		}

		this.Q[this.Pos] = mb
	} else {
		this.Q[this.Pos] = msg_bytes
	}
	this.Pos++

	if this.Pos >= this.Length {
		this.Pos = 0
	}

	if this.Cleaned {
		this.Cleaned = false
	}
	//   this.lock.Unlock()
}

// 清除队列中已有消息
func (this *OfflineTopicQueue) Clean() {
	//   this.lock.Lock()
	this.Q = nil
	this.Pos = 0
	this.Cleaned = true
	this.Gziped = OfflineTopicPayloadUseGzip
	//   this.lock.Unlock()
}

func (this *OfflineTopicQueue) GetAll() (msg_bytes [][]byte) {
	//   this.lock.RLock()
	if this.Cleaned {
		return nil
	} else {
		msg_bytes = make([][]byte, this.Length, this.Length)

		msg_bytes = this.Q[this.Pos:this.Length]
		msg_bytes = append(msg_bytes, this.Q[0:this.Pos]...)

		// 判断是否gzip存储，如果是，则解压后再取出
		if this.Gziped {
			for i, bytes := range msg_bytes {
				if bytes != nil {
					mb, err := Gunzip(bytes)
					if err != nil {
						Log.Errorc(func() string { return err.Error() })
					}

					msg_bytes[i] = mb
				}
			}
		}
		return msg_bytes
	}
	//   this.lock.RUnlock()
}

func (this *OfflineTopicQueue) ConvertToGzip() (err error) {
	if this.Cleaned {
		return nil
	} else if !this.Gziped {
		this.Gziped = true
		for i, bytes := range this.Q {
			if bytes != nil {
				go func() {
					mb, err := Gzip(bytes)
					if err != nil {
						Log.Errorc(func() string { return err.Error() })
					}

					this.Q[i] = mb
				}()
			}
		}
	}
	return nil
}

func (this *OfflineTopicQueue) ConvertToUnzip() (err error) {
	if this.Cleaned {
		return nil
	} else if this.Gziped {
		this.Gziped = false
		for i, bytes := range this.Q {
			if bytes != nil {
				go func() {
					mb, err := Gunzip(bytes)
					if err != nil {
						Log.Errorc(func() string { return err.Error() })
					}

					this.Q[i] = mb
				}()
			}
		}
	}
	return nil
}

func init() {
	go func() {
		for i := 0; i < 2048; i++ {
			sub_p := make([]interface{}, 1, 1)
			select {
			case SubscribersSliceQueue <- sub_p:
			default:
				sub_p = nil
				return
			}
		}
	}()

	go func() {
		for i := 0; i < 1024; i++ {
			tmp_msg := message.NewPublishMessage()
			tmp_msg.SetQoS(message.QosAtLeastOnce)

			select {
			case NewMessagesQueue <- tmp_msg:
			default:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case topic := <-OfflineTopicCleanProcessor:
				Log.Debugc(func() string {
					return fmt.Sprintf("clean offlie topic queue: %s", topic)
				})

				q := OfflineTopicMap[topic]
				if q != nil {
					q.Clean()
				}
			case msg := <-OfflineTopicQueueProcessor:
				//         _ = msg
				topic := string(msg.Topic())
				q := OfflineTopicMap[topic]
				if q == nil {
					q = NewOfflineTopicQueue(Max_message_queue)

					OfflieTopicRWmux.Lock()
					OfflineTopicMap[topic] = q
					OfflieTopicRWmux.Unlock()
				}
				q.Add(msg.Payload())
				msg.SetPayload(nil)
				// copy(aSlice, bSlice)

				Log.Debugc(func() string {
					return fmt.Sprintf("add offline message to the topic: %s", topic)
				})

			case client := <-ClientMapProcessor:
				client_id := client.Name
				client_conn := client.Conn

				if ClientMap[client_id] != nil {
					old_conn := *ClientMap[client_id]
					old_conn.Close()

					Log.Debugc(func() string {
						return fmt.Sprintf("client connected with same client_id: %s. close old connection.", client_id)
					})
				}
				ClientMap[client_id] = client_conn

			}
		}
	}()
}
