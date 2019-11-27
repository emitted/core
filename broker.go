package main

import (
	"log"

	"github.com/go-redis/redis"
	errs "github.com/sireax/Emmet-Go-Server/internal/errors"
	"github.com/sireax/Emmet-Go-Server/internal/tunnel"
)

// Broker structure represents message broker, connected with API
// and fan
type Broker struct {
	Redis *redis.Client // redis client
	Pool  *Pool
}

// NewBroker function initializes Message Broker
func NewBroker() *Broker {
	broker := &Broker{
		Redis: redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
		Pool: NewPool(config.Server.Workers),
	}
	_, err := broker.Redis.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}
	return broker
}

// Run ...
func (broker *Broker) Run() {
	broker.Pool.Run()
}

// FindTunnel ...
func (broker *Broker) FindTunnel(key string) (*tunnel.Tunnel, error) {
	results, _ := broker.Redis.Get("tunnels:maps:" + key).Result()
	if results == "" {
		return nil, errs.NewErrTunnelNotFound()
	}

	model, _ := broker.Redis.HVals("tunnels:" + results).Result()
	if len(model) == 0 {
		return nil, errs.NewErrTunnelNotFound()
	}

	tunnel := tunnel.NewTunnel(model[0], model[1], 100)
	return tunnel, nil
}
