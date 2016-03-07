package service

import (
	"fmt"
	"github.com/surgemq/message"
	"net"
)

var (
	PendingQueue = make([]*message.PublishMessage, 65536, 65536)
	//   PendingProcessor = make(chan *message.PublishMessage, 65536)

	OfflineTopicQueue          = make(map[string][][]byte)
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
	SubscribersSliceQueue = make(chan []interface{}, 2048)

	Max_message_queue int
)

type ClientHash struct {
	Name string
	Conn *net.Conn
}

func init() {
	go func() {
		for i := 0; i < 2048; i++ {
			SubscribersSliceQueue <- make([]interface{}, 1, 1)
		}
	}()

	go func() {
		for {
			tmp_msg := message.NewPublishMessage()
			tmp_msg.SetPacketId(GetRandPkgId())
			tmp_msg.SetQoS(message.QosAtLeastOnce)
			NewMessagesQueue <- tmp_msg
		}
	}()

	go func() {
		for {
			select {
			case topic := <-OfflineTopicGetProcessor:
				OfflineTopicGetChannel <- OfflineTopicQueue[topic]
			case topic := <-OfflineTopicCleanProcessor:
				Log.Debugc(func() string {
					return fmt.Sprintf("clean offlie topic queue: %s", topic)
				})

				OfflineTopicQueue[topic] = nil
			case msg := <-OfflineTopicQueueProcessor:
				topic := string(msg.Topic())
				new_msg_queue := append(OfflineTopicQueue[topic], msg.Payload())
				length := len(new_msg_queue)
				if length > Max_message_queue {
					Log.Debugc(func() string {
						return fmt.Sprintf("add offline message to the topic: %s, and remove %d old messages.",
							topic,
							length-Max_message_queue,
						)
					})

					OfflineTopicQueue[topic] = new_msg_queue[length-Max_message_queue:]
				} else {
					Log.Debugc(func() string {
						return fmt.Sprintf("add offline message to the topic: %s", topic)
					})

					OfflineTopicQueue[topic] = new_msg_queue
				}

			case client := <-ClientMapProcessor:
				client_id := client.Name
				client_conn := client.Conn
				client = nil

				if ClientMap[client_id] != nil {
					old_conn := *ClientMap[client_id]
					old_conn.Close()
					old_conn = nil
					Log.Debugc(func() string {
						return fmt.Sprintf("client connected with same client_id: %s. close old connection.", client_id)
					})
				}
				ClientMap[client_id] = client_conn

			case client_id := <-ClientMapCleanProcessor:
				old_conn := *ClientMap[client_id]
				old_conn.Close()
				old_conn = nil
			case _ = <-PkgIdProcessor:
				PkgIdGenerator <- PkgId
				PkgId++
			}
		}
	}()
}
