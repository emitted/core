package main

import (
	"log"
	"net/http"
	"time"

	"github.com/bmizerany/pat"
	"github.com/gorilla/websocket"
)

var (
	// getting configuration file
	config = GetConfig()
	// creating an instance of Hub - storage for tunnels
	hub = NewHub()
	// creating an instace of Pool that keeps workers
	broker = NewBroker()
	// upgrader
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	// wsHandler is used to handle ws connections
	wsHandler = &WebsocketHandler{
		config: WebsocketConfig{
			MessageSizeLimit: 100,
			PingInterval:     time.Second * 5,
		},
	}

	tunnel = &Tunnel{
		Key:       "PLSB987JB6",
		Clients:   make(map[uint32]*Client),
		Connected: 0,
		Options: &TunnelOptions{
			Presence:       true,
			MaxConnections: 500,
		},
	}
)

func main() {

	broker.Run()

	mux := pat.New()

	mux.Get("/ws/:key", http.HandlerFunc(wsHandler.ServeHTTP))

	http.Handle("/", mux)

	err := http.ListenAndServe(config.Server.Host+":"+config.Server.Port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
