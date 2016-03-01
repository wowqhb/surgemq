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
	"encoding/base64"
	"github.com/pquerna/ffjson/ffjson"
	//   "encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	//   "runtime/debug"
	"github.com/nagae-memooff/config"
	"github.com/nagae-memooff/surgemq/sessions"
	"github.com/nagae-memooff/surgemq/topics"
	"github.com/surgemq/message"
)

var (
	errDisconnect  = errors.New("Disconnect")
	MsgPendingTime time.Duration
)

// processor() reads messages from the incoming buffer and processes them
func (this *service) processor() {
	defer func() {
		// Let's recover from panic
		if r := recover(); r != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("(%s) Recovering from panic: %v", this.cid(), r)
			})
		}

		this.wgStopped.Done()
		this.stop()

		Log.Debugc(func() string {
			return fmt.Sprintf("(%s) Stopping processor", this.cid())
		})
	}()

	//   Log.Debugc(func() string{ return fmt.Sprintf("(%s) Starting processor", this.cid())})
	//   Log.Errorc(func() string{ return fmt.Sprintf("PendingQueue: %v", PendingQueue[0:10])})

	this.wgStarted.Done()

	for {
		// 1. Find out what message is next and the size of the message
		//     this.rmu.Lock()
		mtype, total, err := this.peekMessageSize()
		if err != nil {
			if err == io.EOF {
				Log.Debugc(func() string {
					return fmt.Sprintf("(%s) suddenly disconnect.", this.cid())
				})
			} else {
				Log.Errorc(func() string {
					return fmt.Sprintf("(%s) Error peeking next message size: %v", this.cid(), err)
				})
			}
			return
		}

		msg, n, err := this.peekMessage(mtype, total)
		if err != nil {
			if err == io.EOF {
				Log.Debugc(func() string {
					return fmt.Sprintf("(%s) suddenly disconnect.", this.cid())
				})
			} else {
				Log.Errorc(func() string {
					return fmt.Sprintf("(%s) Error peeking next message: %v", this.cid(), err)
				})
			}
			return
		}
		//     this.rmu.Unlock()

		//Log.Debugc(func() string{ return fmt.Sprintf("(%s) Received: %s", this.cid(), msg)})

		this.inStat.increment(int64(n))

		// 5. Process the read message
		err = this.processIncoming(msg)
		if err != nil {
			if err != errDisconnect {
				Log.Errorc(func() string {
					return fmt.Sprintf("(%s) Error processing %s: %v", this.cid(), msg.Name(), err)
				})
			} else {
				return
			}
		}

		// 7. We should commit the bytes in the buffer so we can move on
		_, err = this.in.ReadCommit(total)
		if err != nil {
			if err != io.EOF {
				Log.Errorc(func() string {
					return fmt.Sprintf("(%s) Error committing %d read bytes: %v", this.cid(), total, err)
				})
			}
			return
		}

		// 7. Check to see if done is closed, if so, exit
		if this.isDone() && this.in.Len() == 0 {
			return
		}

		//if this.inStat.msgs%1000 == 0 {
		//	Log.Debugc(func() string{ return fmt.Sprintf("(%s) Going to process message %d", this.cid(), this.inStat.msgs)})
		//}
	}
}

func (this *service) processIncoming(msg message.Message) error {
	var err error = nil
	//   Log.Errorc(func() string{ return fmt.Sprintf("this.subs is: %v,  count is %d, msg_type is %T", this.subs, len(this.subs), msg)})

	switch msg := (msg).(type) {
	case *message.PublishMessage:
		// For PUBLISH message, we should figure out what QoS it is and process accordingly
		// If QoS == 0, we should just take the next step, no ack required
		// If QoS == 1, we should send back PUBACK, then take the next step
		// If QoS == 2, we need to put it in the ack queue, send back PUBREC
		//     (*msg).SetPacketId(getRandPkgId())
		//     Log.Errorc(func() string{ return fmt.Sprintf("\n%T:%d==========\nmsg is %v\n=====================", *msg, msg.PacketId(), *msg)})
		err = this.processPublish(msg)

	case *message.PubackMessage:
		//     Log.Errorc(func() string{ return fmt.Sprintf("this.subs is: %v,  count is %d, msg_type is %T", this.subs, len(this.subs), msg)})
		// For PUBACK message, it means QoS 1, we should send to ack queue
		//     Log.Errorc(func() string{ return fmt.Sprintf("\n%T:%d==========\nmsg is %v\n=====================", *msg, msg.PacketId(), *msg)})
		go _process_ack(msg.PacketId())
		this.sess.Pub1ack.Ack(msg)
		this.processAcked(this.sess.Pub1ack)

	case *message.PubrecMessage:
		// For PUBREC message, it means QoS 2, we should send to ack queue, and send back PUBREL
		if err = this.sess.Pub2out.Ack(msg); err != nil {
			break
		}

		resp := message.NewPubrelMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = this.writeMessage(resp)

	case *message.PubrelMessage:
		// For PUBREL message, it means QoS 2, we should send to ack queue, and send back PUBCOMP
		if err = this.sess.Pub2in.Ack(msg); err != nil {
			break
		}

		this.processAcked(this.sess.Pub2in)

		resp := message.NewPubcompMessage()
		resp.SetPacketId(msg.PacketId())
		_, err = this.writeMessage(resp)

	case *message.PubcompMessage:
		// For PUBCOMP message, it means QoS 2, we should send to ack queue
		if err = this.sess.Pub2out.Ack(msg); err != nil {
			break
		}

		this.processAcked(this.sess.Pub2out)

	case *message.SubscribeMessage:
		// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
		return this.processSubscribe(msg)

	case *message.SubackMessage:
		// For SUBACK message, we should send to ack queue
		this.sess.Suback.Ack(msg)
		this.processAcked(this.sess.Suback)

	case *message.UnsubscribeMessage:
		// For UNSUBSCRIBE message, we should remove subscriber, then send back UNSUBACK
		return this.processUnsubscribe(msg)

	case *message.UnsubackMessage:
		// For UNSUBACK message, we should send to ack queue
		this.sess.Unsuback.Ack(msg)
		this.processAcked(this.sess.Unsuback)

	case *message.PingreqMessage:
		// For PINGREQ message, we should send back PINGRESP
		resp := message.NewPingrespMessage()
		_, err = this.writeMessage(resp)

	case *message.PingrespMessage:
		this.sess.Pingack.Ack(msg)
		this.processAcked(this.sess.Pingack)

	case *message.DisconnectMessage:
		// For DISCONNECT message, we should quit
		this.sess.Cmsg.SetWillFlag(false)
		return errDisconnect

	default:
		return fmt.Errorf("(%s) invalid message type %s.", this.cid(), msg.Name())
	}

	if err != nil {
		Log.Error("(%s) Error processing acked message: %v", this.cid(), err)
	}

	return err
}

func (this *service) processAcked(ackq *sessions.Ackqueue) {
	for _, ackmsg := range ackq.Acked() {
		// Let's get the messages from the saved message byte slices.
		msg, err := ackmsg.Mtype.New()
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("process/processAcked: Unable to creating new %s message: %v", ackmsg.Mtype, err)
			})
			continue
		}

		if _, err := msg.Decode(ackmsg.Msgbuf); err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("process/processAcked: Unable to decode %s message: %v", ackmsg.Mtype, err)
			})
			continue
		}

		ack, err := ackmsg.State.New()
		if err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("process/processAcked: Unable to creating new %s message: %v", ackmsg.State, err)
			})
			continue
		}

		if _, err := ack.Decode(ackmsg.Ackbuf); err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("process/processAcked: Unable to decode %s message: %v", ackmsg.State, err)
			})
			continue
		}

		//Log.Debugc(func() string{ return fmt.Sprintf("(%s) Processing acked message: %v", this.cid(), ack)})

		// - PUBACK if it's QoS 1 message. This is on the client side.
		// - PUBREL if it's QoS 2 message. This is on the server side.
		// - PUBCOMP if it's QoS 2 message. This is on the client side.
		// - SUBACK if it's a subscribe message. This is on the client side.
		// - UNSUBACK if it's a unsubscribe message. This is on the client side.
		switch ackmsg.State {
		case message.PUBREL:
			// If ack is PUBREL, that means the QoS 2 message sent by a remote client is
			// releassed, so let's publish it to other subscribers.
			if err = this.onPublish(msg.(*message.PublishMessage)); err != nil {
				Log.Errorc(func() string {
					return fmt.Sprintf("(%s) Error processing ack'ed %s message: %v", this.cid(), ackmsg.Mtype, err)
				})
			}

		case message.PUBACK, message.PUBCOMP, message.SUBACK, message.UNSUBACK, message.PINGRESP:
			Log.Debugc(func() string { return fmt.Sprintf("process/processAcked: %s", ack) })
			// If ack is PUBACK, that means the QoS 1 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PUBCOMP, that means the QoS 2 message sent by this service got
			// ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is SUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is UNSUBACK, that means the SUBSCRIBE message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			// If ack is PINGRESP, that means the PINGREQ message sent by this service
			// got ack'ed. There's nothing to do other than calling onComplete() below.

			err = nil

		default:
			Log.Errorc(func() string { return fmt.Sprintf("(%s) Invalid ack message type %s.", this.cid(), ackmsg.State) })
			continue
		}

		// Call the registered onComplete function
		if ackmsg.OnComplete != nil {
			onComplete, ok := ackmsg.OnComplete.(OnCompleteFunc)
			if !ok {
				Log.Errorc(func() string {
					return fmt.Sprintf("process/processAcked: Error type asserting onComplete function: %v", reflect.TypeOf(ackmsg.OnComplete))
				})
			} else if onComplete != nil {
				if err := onComplete(msg, ack, nil); err != nil {
					Log.Errorc(func() string { return fmt.Sprintf("process/processAcked: Error running onComplete(): %v", err) })
				}
			}
		}
	}
}

// For PUBLISH message, we should figure out what QoS it is and process accordingly
// If QoS == 0, we should just take the next step, no ack required
// If QoS == 1, we should send back PUBACK, then take the next step
// If QoS == 2, we need to put it in the ack queue, send back PUBREC
func (this *service) processPublish(msg *message.PublishMessage) error {
	switch msg.QoS() {
	case message.QosExactlyOnce:
		this.sess.Pub2in.Wait(msg, nil)

		resp := message.NewPubrecMessage()
		resp.SetPacketId(msg.PacketId())

		_, err := this.writeMessage(resp)

		err = this._process_publish(msg)
		return err

	case message.QosAtLeastOnce:
		resp := message.NewPubackMessage()
		resp.SetPacketId(msg.PacketId())

		if _, err := this.writeMessage(resp); err != nil {
			return err
		}

		err := this._process_publish(msg)
		return err
	case message.QosAtMostOnce:
		err := this._process_publish(msg)
		return err
	default:
		fmt.Printf("default: %d\n", msg.QoS())
	}

	return fmt.Errorf("(%s) invalid message QoS %d.", this.cid(), msg.QoS())
}

// For SUBSCRIBE message, we should add subscriber, then send back SUBACK
func (this *service) processSubscribe(msg *message.SubscribeMessage) error {
	resp := message.NewSubackMessage()
	resp.SetPacketId(msg.PacketId())

	// Subscribe to the different topics
	var retcodes []byte

	topics := msg.Topics()
	qos := msg.Qos()

	this.rmsgs = this.rmsgs[0:0]

	//   fmt.Printf("this.id: %d,  this.sess.ID(): %s\n", this.id, this.cid())
	for i, t := range topics {
		rqos, err := this.topicsMgr.Subscribe(t, qos[i], &this.onpub, this.sess.ID())
		//     rqos, err := this.topicsMgr.Subscribe(t, qos[i], &this)
		if err != nil {
			this.conn.Close()
			return err
		}
		this.sess.AddTopic(string(t), qos[i])

		retcodes = append(retcodes, rqos)

		// yeah I am not checking errors here. If there's an error we don't want the
		// subscription to stop, just let it go.
		this.topicsMgr.Retained(t, &this.rmsgs)
		//     Log.Debugc(func() string{ return fmt.Sprintf("(%s) topic = %s, retained count = %d", this.cid(), string(t), len(this.rmsgs))})
	}

	if err := resp.AddReturnCodes(retcodes); err != nil {
		return err
	}

	if _, err := this.writeMessage(resp); err != nil {
		return err
	}

	for _, rm := range this.rmsgs {
		if err := this.publish(rm, nil); err != nil {
			Log.Errorc(func() string {
				return fmt.Sprintf("service/processSubscribe: Error publishing retained message: %v", err)
			})
			return err
		}
	}

	for _, t := range topics {
		go this._process_offline_message(string(t))
	}

	return nil
}

// For UNSUBSCRIBE message, we should remove the subscriber, and send back UNSUBACK
func (this *service) processUnsubscribe(msg *message.UnsubscribeMessage) error {
	topics := msg.Topics()

	for _, t := range topics {
		this.topicsMgr.Unsubscribe(t, &this.onpub)
		this.sess.RemoveTopic(string(t))
	}

	resp := message.NewUnsubackMessage()
	resp.SetPacketId(msg.PacketId())

	_, err := this.writeMessage(resp)
	return err
}

// onPublish() is called when the server receives a PUBLISH message AND have completed
// the ack cycle. This method will get the list of subscribers based on the publish
// topic, and publishes the message to the list of subscribers.
func (this *service) onPublish(msg *message.PublishMessage) (err error) {
	//   if msg.Retain() {
	//     if err = this.topicsMgr.Retain(msg); err != nil {
	//       Log.Errorc(func() string{ return fmt.Sprintf("(%s) Error retaining message: %v", this.cid(), err)})
	//     }
	//   }

	//   var subs []interface{}
	subs := _get_temp_subs()
	err = this.topicsMgr.Subscribers(msg.Topic(), msg.QoS(), &subs, &this.qoss)
	if err != nil {
		Log.Errorc(func() string { return fmt.Sprintf("(%s) Error retrieving subscribers list: %v", this.cid(), err) })
		return err
	}

	msg.SetRetain(false)

	//   Log.Errorc(func() string{ return fmt.Sprintf("(%s) Publishing to topic %q and %d subscribers", this.cid(), string(msg.Topic()), len(this.subs))})
	//   fmt.Printf("value: %v\n", config.GetModel())
	go handlePendingMessage(msg)

	for _, s := range subs {
		if s != nil {
			fn, ok := s.(*OnPublishFunc)
			if !ok {
				Log.Errorc(func() string { return fmt.Sprintf("Invalid onPublish Function: %T", s) })
				return fmt.Errorf("Invalid onPublish Function")
			} else {
				(*fn)(msg)
				//         Log.Errorc(func() string{ return fmt.Sprintf("OfflineTopicQueue[%s]: %v, len is: %d\n", msg.Topic(), OfflineTopicQueue[string(msg.Topic())], len(OfflineTopicQueue[string(msg.Topic())]))})
			}
		}
	}

	return nil
}

type BroadCastMessage struct {
	Clients []string `json:"clients"`
	Payload string   `json:"payload"`
}
type BadgeMessage struct {
	Data int    `json:data`
	Type string `json:type`
}

func (this *service) onReceiveBadge(msg *message.PublishMessage) (err error) {
	var badge_message BadgeMessage

	datas := strings.Split(string(msg.Payload()), ":")
	//   datas := strings.Split(fmt.Sprintf("%s", msg.Payload()), ":")
	if len(datas) != 2 {
		return errors.New(fmt.Sprintf("invalid message payload: %s", msg.Payload()))
	}

	account_id := datas[0]
	payload_base64 := datas[1]

	if payload_base64 == "" {
		return errors.New(fmt.Sprintf("blank base64 payload, abort. %s", msg.Payload()))
	}

	payload_bytes, err := base64.StdEncoding.DecodeString(payload_base64)
	if err != nil {
		Log.Errorc(func() string { return fmt.Sprintf("can't decode payload: %s", payload_base64) })
	}

	err = ffjson.Unmarshal([]byte(payload_bytes), &badge_message)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("can't parse message json: account_id: %s, payload: %s", account_id, payload_bytes)
		})
		return
	}
	//   Log.Infoc(func() string{ return fmt.Sprintf("badge: %v, type: %T\n", badge_message.Data, badge_message.Data)})

	go handleBadge(account_id, badge_message)
	return
}

func (this *service) onGroupPublish(msg *message.PublishMessage) (err error) {
	var (
		broadcast_msg BroadCastMessage
		payload       []byte
	)

	Log.Infoc(func() string {
		return fmt.Sprintln("receive group msgs.")
	})

	err = ffjson.Unmarshal(msg.Payload(), &broadcast_msg)
	if err != nil {
		Log.Errorc(func() string { return fmt.Sprintf("can't parse message json: %s", msg.Payload()) })
		return
	}

	payload, err = base64.StdEncoding.DecodeString(broadcast_msg.Payload)
	if err != nil {
		Log.Errorc(func() string { return fmt.Sprintf("can't decode payload: %s", broadcast_msg.Payload) })
		return
	}

	for _, client_id := range broadcast_msg.Clients {
		topic := topics.GetUserTopic(client_id)
		if topic == "" {
			continue
		}

		go this._publish_to_topic(topic, payload)
	}

	return
}

// 处理publish类型的消息，如果是特殊频道特殊处理，否则正常处理
func (this *service) _process_publish(msg *message.PublishMessage) (err error) {
	switch string(msg.Topic()) {
	case config.Get("broadcast_channel"):
		go this.onGroupPublish(msg)
	case config.Get("s_channel"):
		go this.onReceiveBadge(msg)
	default:
		msg.SetPacketId(GetRandPkgId())
		go this.onPublish(msg)
	}
	return
}

//根据topic和payload 推送消息
func (this *service) _publish_to_topic(topic string, payload []byte) {
	Log.Debugc(func() string {
		return fmt.Sprintf("send msg to topic: %s", topic)
	})
	tmp_msg := _get_tmp_msg()
	tmp_msg.SetTopic([]byte(topic))
	tmp_msg.SetPayload(payload)
	this.onPublish(tmp_msg)
}

// 当某个topic被订阅，处理此topic所对应的、离线消息队列里的消息
func (this *service) _process_offline_message(topic string) (err error) {
	offline_msgs := getOfflineMsg(topic)
	if offline_msgs == nil {
		return nil
	}

	Log.Infoc(func() string {
		return fmt.Sprintf("send %d offline msgs to topic: %s", len(offline_msgs), topic)
	})

	for _, payload := range offline_msgs {
		this._publish_to_topic(topic, payload)
	}
	OfflineTopicCleanProcessor <- topic
	return nil
}

// 获取一个递增的pkgid
func GetRandPkgId() uint16 {
	PkgIdProcessor <- true
	return <-PkgIdGenerator
}

// 判断消息是否已读
func handlePendingMessage(msg *message.PublishMessage) {
	// 如果QOS=0,则无需等待直接返回
	if msg.QoS() == message.QosAtMostOnce {
		return
	}

	// 将msg按照pkt_id，存入pending队列
	// 如果指定时间后，msg仍然在队列中，说明未收到回包，需要将消息放到OfflineTopicQueueProcessor中处理
	pkt_id := msg.PacketId()
	PendingQueue[pkt_id] = msg

	time.Sleep(time.Second * MsgPendingTime)
	if PendingQueue[pkt_id] != nil {
		Log.Debugc(func() string {
			return fmt.Sprintf("receive ack timeout. send msg to offline msg queue.topic: %s", msg.Topic())
		})
		PendingQueue[pkt_id] = nil
		OfflineTopicQueueProcessor <- msg
	}
}

// 处理苹果设备的未读数，修改redis
func handleBadge(account_id string, badge_message BadgeMessage) {
	key := "badge_account:" + account_id
	_, err := topics.RedisDo("set", key, badge_message.Data)
	if err != nil {
		Log.Errorc(func() string {
			return fmt.Sprintf("can't set badge! account_id: %s, badge: %v", account_id, badge_message)
		})
	}
}

// 根据topic获取离线消息队列
// 由于不能并发读写，所以要借助channel来实现
func getOfflineMsg(topic string) (msg [][]byte) {
	OfflineTopicGetProcessor <- topic
	msg = <-OfflineTopicGetChannel
	return
}

//根据pkt_id，将pending队列里的该条消息移除
func _process_ack(pkg_id uint16) {
	PendingQueue[pkg_id] = nil

	Log.Debugc(func() string {
		return fmt.Sprintf("receive ack, remove msg from pending queue: %d", pkg_id)
	})
}

// 从池子里获取一个长度为1的slice，用于填充订阅队列
func _get_temp_subs() (subs []interface{}) {
	subs = <-SubscribersSliceQueue
	return
}

// 从池子里获取一个msg对象，用于打包
func _get_tmp_msg() (msg *message.PublishMessage) {
	msg = <-NewMessagesQueue
	return
}
