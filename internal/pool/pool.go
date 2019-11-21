package pool

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-redis/redis"
	. "github.com/logrusorgru/aurora"
	c "github.com/sireax/Emmet-Go-Server/config"
	h "github.com/sireax/Emmet-Go-Server/internal/hub"
	p "github.com/sireax/Emmet-Go-Server/internal/packet"
)

var (
	config = c.GetConfig()
)

// Pool ...
type Pool struct {
	Hub          *h.Hub
	Redis        *redis.Client
	WorkersCount int
}

// NewPool ...
func NewPool(hub *h.Hub, workers int) *Pool {
	return &Pool{
		Hub: hub,
		Redis: redis.NewClient(&redis.Options{
			Addr: config.Redis.Host + ":" + config.Redis.Port,
		}),
		WorkersCount: workers,
	}
}

// Run ...
func (pool *Pool) Run() {
	flow := pool.Redis.Subscribe("messages").Channel()
	for i := 0; i < pool.WorkersCount; i++ {
		log.Println(Green("worker"), i, Green("is running"))
		go pool.worker(flow)
	}

}

func (pool *Pool) worker(flow <-chan *redis.Message) {
	type RedisMessage struct {
		Tunnel string
		Data   json.RawMessage
	}

	// Listening for new Messages
	for {
		select {
		case data := <-flow:

			// Trying to read message from pub/sub broker
			var message *RedisMessage
			messageRaw := data.Payload
			err := json.Unmarshal([]byte(messageRaw), &message)
			if err != nil {
				continue
			}
			fmt.Printf("%+v", message)

			// Getting tunnel from Hub's map
			tunnel, err := pool.Hub.FindTunnel(message.Tunnel)
			if err != nil {
				continue
			}

			// Creating message packet for broadcast
			var packet *p.Packet
			err = json.Unmarshal([]byte(message.Data), &packet)
			if err != nil {
				continue
			}

			// Broadcasting message
			for subscriber := range tunnel.Subscribers {
				subscriber.WriteJSON(packet)
			}
		}
	}
}
