package broker

import (
	"log"

	"github.com/go-redis/redis"
)

// Broker structure represents message broker, connected with API
// and fan
type Broker struct {
	Redis *redis.Client // redis client
}

// NewBroker function initializes Message Broker
func NewBroker() *Broker {
	broker := &Broker{
		Redis: redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
	}
	_, err := broker.Redis.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}
	return broker
}
