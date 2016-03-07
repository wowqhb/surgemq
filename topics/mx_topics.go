package topics

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	//   "github.com/nagae-memooff/config"
	//   "github.com/nagae-memooff/surgemq/topics"
	//   "github.com/nagae-memooff/surgemq/service"
	"github.com/surge/glog"
	"github.com/surgemq/message"
	"sync"
)

var (
	// MXMaxQosAllowed is the maximum QOS supported by this server
	MXMaxQosAllowed = message.QosAtLeastOnce
	RedisPool       *redis.Pool
	Channelcache    map[string]string
	cmux            sync.RWMutex
)

var _ TopicsProvider = (*mxTopics)(nil)

type mxTopics struct {
	// Sub/unsub mutex
	smu sync.RWMutex
	// Subscription tree
	//   sroot *mxsnode

	// subscription map
	//实际类型应该是： map[string]*onPublishFunc
	subscriber map[string]interface{}

	// Retained message mutex
	rmu sync.RWMutex
	// Retained messages topic tree
	rroot *mxrnode
}

func init() {
	Channelcache = make(map[string]string)
	RedisPool = newRedisPool()
	Register("mx", NewMXProvider())
}

// NewMemProvider returns an new instance of the mxTopics, which is implements the
// TopicsProvider interface. memProvider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistend so
// when the server goes, everything will be gone. Use with care.
func NewMXProvider() *mxTopics {
	return &mxTopics{
		//     sroot: newMXSNode(),
		subscriber: make(map[string]interface{}),
		rroot:      newMXRNode(),
	}
}

// FIXME 把clientid改为 某上层对象的指针，以便让它能断掉连接
//TODO: 去掉树形结构，就留一个subscribe函数即可
func (this *mxTopics) Subscribe(topic []byte, qos byte, sub interface{}, client_id string) (byte, error) {
	topic_str := string(topic)
	if !message.ValidQos(qos) {
		return message.QosFailure, fmt.Errorf("Invalid QoS %d", qos)
	}

	if sub == nil {
		return message.QosFailure, fmt.Errorf("Subscriber cannot be nil")
	}

	if !checkValidchannel(client_id, topic_str) {
		return message.QosFailure, fmt.Errorf("%s: Invalid Channel %s", client_id, topic_str)
	}

	this.smu.Lock()
	defer this.smu.Unlock()

	if qos > MXMaxQosAllowed {
		//     Log.Printf("invalid qos: %d\n", qos)
		qos = MXMaxQosAllowed
	}
	//   Log.Errorc(func() string{ return fmt.Sprintf("topic: %s, qos: %d,  client_id: %s\n", topic, qos, client_id)})

	this.subscriber[topic_str] = sub

	return qos, nil
}

func (this *mxTopics) Unsubscribe(topic []byte, sub interface{}) error {
	this.smu.Lock()
	defer this.smu.Unlock()

	this.subscriber[string(topic)] = nil
	return nil
	//   return this.sroot.sremove(topic, sub)
}

// Returned values will be invalidated by the next Subscribers call
func (this *mxTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	if !message.ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	this.smu.RLock()
	defer this.smu.RUnlock()

	(*subs)[0] = this.subscriber[string(topic)]
	*qoss = (*qoss)[0:0]
	return nil
}

func (this *mxTopics) Retain(msg *message.PublishMessage) error {
	this.rmu.Lock()
	defer this.rmu.Unlock()

	// So apparently, at least according to the MQTT Conformance/Interoperability
	// Testing, that a payload of 0 means delete the retain message.
	// https://eclipse.org/paho/clients/testing/
	if len(msg.Payload()) == 0 {
		return this.rroot.rremove(msg.Topic())
	}

	return this.rroot.rinsert(msg.Topic(), msg)
}

func (this *mxTopics) Retained(topic []byte, msgs *[]*message.PublishMessage) error {
	this.rmu.RLock()
	defer this.rmu.RUnlock()

	return this.rroot.rmatch(topic, msgs)
}

func (this *mxTopics) Close() error {
	this.subscriber = nil
	this.rroot = nil
	return nil
}

// subscrition nodes

// retained message nodes
type mxrnode struct {
	// If this is the end of the topic string, then add retained messages here
	msg *message.PublishMessage
	buf []byte

	// Otherwise add the next topic level here
	rnodes map[string]*mxrnode
}

func newMXRNode() *mxrnode {
	return &mxrnode{
		rnodes: make(map[string]*mxrnode),
	}
}

func (this *mxrnode) rinsert(topic []byte, msg *message.PublishMessage) error {
	// If there's no more topic levels, that means we are at the matching rnode.
	if len(topic) == 0 {
		l := msg.Len()

		// Let's reuse the buffer if there's enough space
		if l > cap(this.buf) {
			this.buf = make([]byte, l)
		} else {
			this.buf = this.buf[0:l]
		}

		if _, err := msg.Encode(this.buf); err != nil {
			return err
		}

		// Reuse the message if possible
		if this.msg == nil {
			this.msg = message.NewPublishMessage()
		}

		if _, err := this.msg.Decode(this.buf); err != nil {
			return err
		}

		return nil
	}

	// Not the last level, so let's find or create the next level snode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextMxTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add snode if it doesn't already exist
	n, ok := this.rnodes[level]
	if !ok {
		n = newMXRNode()
		this.rnodes[level] = n
	}

	return n.rinsert(rem, msg)
}

// Remove the retained message for the supplied topic
func (this *mxrnode) rremove(topic []byte) error {
	// If the topic is empty, it means we are at the final matching rnode. If so,
	// let's remove the buffer and message.
	if len(topic) == 0 {
		this.buf = nil
		this.msg = nil
		return nil
	}

	// Not the last level, so let's find the next level rnode, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := nextMxTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the rnode that matches the topic level
	n, ok := this.rnodes[level]
	if !ok {
		return fmt.Errorf("memtopics/rremove: No topic found")
	}

	// Remove the subscriber from the next level rnode
	if err := n.rremove(rem); err != nil {
		return err
	}

	// If there are no more rnodes to the next level we just visited let's remove it
	if len(n.rnodes) == 0 {
		delete(this.rnodes, level)
	}

	return nil
}

// rmatch() finds the retained messages for the topic and qos provided. It's somewhat
// of a reverse match compare to match() since the supplied topic can contain
// wildcards, whereas the retained message topic is a full (no wildcard) topic.
func (this *mxrnode) rmatch(topic []byte, msgs *[]*message.PublishMessage) error {
	// If the topic is empty, it means we are at the final matching rnode. If so,
	// add the retained msg to the list.
	if len(topic) == 0 {
		if this.msg != nil {
			*msgs = append(*msgs, this.msg)
		}
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextMxTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	if level == MWC {
		// If '#', add all retained messages starting this node
		this.allRetained(msgs)
	} else if level == SWC {
		// If '+', check all nodes at this level. Next levels must be matched.
		for _, n := range this.rnodes {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	} else {
		// Otherwise, find the matching node, go to the next level
		if n, ok := this.rnodes[level]; ok {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	}

	return nil
}

func (this *mxrnode) allRetained(msgs *[]*message.PublishMessage) {
	if this.msg != nil {
		*msgs = append(*msgs, this.msg)
	}

	for _, n := range this.rnodes {
		n.allRetained(msgs)
	}
}

// Returns topic level, remaining topic levels and any errors
func nextMxTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Multi-level wildcard found in topic and it's not at the last level")
			}

			if i == 0 {
				return []byte(SWC), topic[i+1:], nil
			}

			s = stateCHR
		//       return topic[:i], topic[i+1:], nil

		//		case '#':
		//			if i != 0 {
		//				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard character '#' must occupy entire topic level")
		//			}
		//
		//			s = stateMWC
		//
		//		case '+':
		//			if i != 0 {
		//				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard character '+' must occupy entire topic level")
		//			}
		//
		//			s = stateSWC
		//
		//		case '$':
		//			if i == 0 {
		//				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Cannot publish to $ topics")
		//			}
		//
		//			s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				return nil, nil, fmt.Errorf("memtopics/nextTopicLevel: Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = stateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	return topic, nil, nil
}

// The QoS of the payload messages sent in response to a subscription must be the
// minimum of the QoS of the originally published message (in this case, it's the
// qos parameter) and the maximum QoS granted by the server (in this case, it's
// the QoS in the topic tree).
//
// It's also possible that even if the topic matches, the subscriber is not included
// due to the QoS granted is lower than the published message QoS. For example,
// if the client is granted only QoS 0, and the publish message is QoS 1, then this
// client is not to be send the published message.

// func equal(k1, k2 interface{}) bool {
// 	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
// 		return false
// 	}
//
// 	if reflect.ValueOf(k1).Kind() == reflect.Func {
// 		return &k1 == &k2
// 	}
//
// 	if k1 == k2 {
// 		return true
// 	}
//
// 	switch k1 := k1.(type) {
// 	case string:
// 		return k1 == k2.(string)
//
// 	case int64:
// 		return k1 == k2.(int64)
//
// 	case int32:
// 		return k1 == k2.(int32)
//
// 	case int16:
// 		return k1 == k2.(int16)
//
// 	case int8:
// 		return k1 == k2.(int8)
//
// 	case int:
// 		return k1 == k2.(int)
//
// 	case float32:
// 		return k1 == k2.(float32)
//
// 	case float64:
// 		return k1 == k2.(float64)
//
// 	case uint:
// 		return k1 == k2.(uint)
//
// 	case uint8:
// 		return k1 == k2.(uint8)
//
// 	case uint16:
// 		return k1 == k2.(uint16)
//
// 	case uint32:
// 		return k1 == k2.(uint32)
//
// 	case uint64:
// 		return k1 == k2.(uint64)
//
// 	case uintptr:
// 		return k1 == k2.(uintptr)
// 	}
//
// 	return false
// }

func checkValidchannel(client_id, topic string) bool {
	if client_id == "master_8859" {
		//     Log.Printf("%s, %s",client_id, topic)
		return true
	}

	if GetUserTopic(client_id) == topic {
		//     Log.Printf("%v, %s", data, err)
		return true
	}
	return false
}

func GetUserTopic(client_id string) (topic string) {
	//   defer fmt.Printf("%s get topic: %s\n", client_id, topic)
	cmux.RLock()
	topic = Channelcache[client_id]
	cmux.RUnlock()
	if topic != "" {
		return
	}

	key := "channel:" + client_id
	data, err := RedisDo("get", key)
	if err != nil {
		glog.Errorln(err)
		topic = ""
		return
	}

	if data == "" {
		topic = ""
		return
	}

	topic = "/u/" + data
	//   glog.Errorln(Channelcache)
	cmux.Lock()
	Channelcache[client_id] = topic
	cmux.Unlock()
	return
}

func LoadChannelCache() {
	client_ids, channel_keys, err := GetClientIDandChannels()
	if err != nil {
		glog.Errorln(err)
		return
	}

	channels, err := RedisDoGetMulti("mget", channel_keys...)
	if err != nil {
		glog.Errorln(err)
		return
	}

	if len(channels) != len(channel_keys) {
		glog.Errorln("长度不一致，不缓存了！")
	} else {
		for i := 0; i < len(channels); i++ {
			Channelcache[client_ids[i]] = "/u/" + channels[i]
		}
	}
}
