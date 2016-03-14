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

	// 由于这个需要一一对应，并且把结果传回去，所以不能带缓冲。
	OfflineTopicGetProcessor = make(chan string)
	OfflineTopicGetChannel   = make(chan [][]byte)

	ClientMap               = make(map[string]*net.Conn)
	ClientMapProcessor      = make(chan *ClientHash, 1024)
	ClientMapCleanProcessor = make(chan string)

	PkgIdProcessor = make(chan bool, 1024)
	PkgIdGenerator = make(chan uint16, 1024)
	PkgId          = uint16(1)

	NewMessagesQueue      = make(chan *message.PublishMessage, 2048)
	SubscribersSliceQueue = make(chan *[]interface{}, 2048)

	Max_message_queue int
)

type ClientHash struct {
	Name string
	Conn *net.Conn
}

// 定义一个离线消息队列的结构体，保存一个二维byte数组和一个位置
type OfflineTopicQueue struct {
	q      [][]byte
	pos    int
	length int
	clean  bool
	//   lock  *sync.RWMutex
}

func NewOfflineTopicQueue(length int) (q *OfflineTopicQueue) {
	q = &OfflineTopicQueue{
		make([][]byte, length, length),
		0,
		length,
		true,
		//     new(sync.RWMutex),
	}

	return q
}

// 向队列中添加消息
//NOTE 因为目前是在channel中操作，所以无需加锁。如果需要并发访问，则需要加锁了。
func (this *OfflineTopicQueue) Add(msg_bytes []byte) {
	//   this.lock.Lock()
	this.q[this.pos] = msg_bytes
	this.pos++

	if this.pos >= this.length {
		this.pos = 0
	}

	if this.clean {
		this.clean = false
	}
	//   this.lock.Unlock()
}

// 清除队列中已有消息
func (this *OfflineTopicQueue) Clean() {
	//   this.lock.Lock()
	this.q = nil
	this.q = make([][]byte, this.length, this.length)
	this.pos = 0
	this.clean = true
	//   this.lock.Unlock()
}

func (this *OfflineTopicQueue) GetAll() (msg_bytes [][]byte) {
	//   this.lock.RLock()
	if this.clean {
		return nil
	} else {
		msg_bytes = this.q[this.pos:this.length]
		msg_bytes = append(msg_bytes, this.q[0:this.pos]...)
		return msg_bytes
	}
	//   this.lock.RUnlock()
}

func init() {
	go func() {
		for i := 0; i < 2048; i++ {
			sub_p := make([]interface{}, 1, 1)
			select {
			case SubscribersSliceQueue <- &sub_p:
			default:
				sub_p = nil
				return
			}
		}
	}()

	go func() {
		for i := 0; i < 2048; i++ {
			tmp_msg := message.NewPublishMessage()
			tmp_msg.SetQoS(message.QosAtLeastOnce)

			select {
			case NewMessagesQueue <- tmp_msg:
			default:
				tmp_msg = nil
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case topic := <-OfflineTopicGetProcessor:
				q := OfflineTopicMap[topic]
				if q == nil {
					OfflineTopicGetChannel <- nil
				} else {
					OfflineTopicGetChannel <- q.GetAll()
				}
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
					OfflineTopicMap[topic] = q
				}
				q.Add(msg.Payload())
				Log.Debugc(func() string {
					return fmt.Sprintf("add offline message to the topic: %s", topic)
				})

			case client := <-ClientMapProcessor:
				client_id := client.Name
				client_conn := client.Conn

				if ClientMap[client_id] != nil {
					old_conn := ClientMap[client_id]
					(*old_conn).Close()
					ClientMap[client_id] = nil
					Log.Debugc(func() string {
						return fmt.Sprintf("client connected with same client_id: %s. close old connection.", client_id)
					})
				}
				ClientMap[client_id] = client_conn

			case client_id := <-ClientMapCleanProcessor:
				old_conn := ClientMap[client_id]
				if old_conn != nil {
					(*old_conn).Close()
				}
				ClientMap[client_id] = nil
			case _ = <-PkgIdProcessor:
				PkgIdGenerator <- PkgId
				PkgId++
			}
		}
	}()
}
