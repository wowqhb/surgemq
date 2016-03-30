package topics

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/nagae-memooff/config"
	"strings"
)

func NewRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:   config.GetInt("redis_max_idle"),
		MaxActive: config.GetInt("redis_concurrent"), // max number of connections
		Dial: func() (redis.Conn, error) {
			var (
				c   redis.Conn
				err error
			)
			redis_host := fmt.Sprintf("%s:%s", config.GetMulti("redis_host", "redis_port")...)

			redis_passwd := config.Get("redis_passwd")
			if redis_passwd != "" {
				pwd := redis.DialPassword(redis_passwd)
				c, err = redis.Dial("tcp", redis_host, pwd)
			} else {
				c, err = redis.Dial("tcp", redis_host)
			}

			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}

func RedisDo(cmd string, args ...interface{}) (data string, err error) {
	c := RedisPool.Get()
	defer c.Close()

	data_origin, err := c.Do(cmd, args...)
	if err != nil {
		fmt.Println(err)
		return
	}

	data_byte, ok := data_origin.([]byte)
	if !ok {
		return
	}

	return (string)(data_byte), nil
}

func GetClientIDandChannels() (client_ids []string, channels []interface{}, err error) {
	c := RedisPool.Get()
	defer c.Close()

	data_origin, err := c.Do("keys", "channel:*")
	if err != nil {
		fmt.Println(err)
		return
	}

	data_ary, ok := data_origin.([]interface{})
	if !ok {
		return
	}

	for _, v := range data_ary {
		value, ok := v.([]uint8)
		if ok {
			channel := string(value)
			channels = append(channels, channel)

			client_id := strings.Split(channel, ":")[1]
			client_ids = append(client_ids, client_id)
		} else {
			fmt.Printf("invalid client_id: type:%T, value: %v\n", v, v)
		}
	}
	return
}

func RedisDoGetMulti(cmd string, args ...interface{}) (data []string, err error) {
	c := RedisPool.Get()
	defer c.Close()

	//   data_origin, err := c.Do("keys", "channel:*")
	data_origin, err := c.Do(cmd, args...)
	if err != nil {
		fmt.Println(err)
		return
	}

	data_ary, ok := data_origin.([]interface{})
	if !ok {
		return
	}

	for _, v := range data_ary {
		value, ok := v.([]uint8)
		if ok {
			data = append(data, string(value))
		} else {
			fmt.Printf("invalid data: type:%T, value: %v\n", v, v)
			//       data = append(data, "")
		}
	}
	return
}
