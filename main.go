package main

import (
	"github.com/bmizerany/pat"
	"log"
	"net/http"
	"time"
)

var (
	node = NewNode(Config{
		Version:                          "sdf",
		Name:                             "sdfsdfsdf",
		ClientChannelLimit:               1000,
		ClientStaleCloseDelay:            15,
		NodeInfoMetricsAggregateInterval: 5,
		LogLevel:                         LogLevelDebug,
		LogHandler:                       nil,
	})
	wsHandler = NewWebsocketHandler(WebsocketConfig{MessageSizeLimit: 100, PingInterval: time.Second}, node)

	app = &App{
		Key:      "Njc4ZGYyZDgtN2ZjMC00ZjAwLWI2OWYtZTZhMGQxYjU4YjQ4",
		Secret:   "8788369a-227f-11ea-8cb8-1ee2210c29b2",
		Clients:  make(map[string]*Client, 0),
		Channels: make(map[string]*Channel, 0),
		Options: &AppOptions{
			MaxConnections: 1500,
		},
		Stats: &AppStats{
			Connections: 0,
			Messages:    0,
		},
	}
)

func main() {

	err := node.Run()
	if err != nil {
		log.Fatal(err)
	}

	mux := pat.New()

	mux.Get("/ws/:secret", http.HandlerFunc(wsHandler.ServeHTTP))

	http.Handle("/", mux)

	err = http.ListenAndServe("localhost:8000", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)

	}
}
