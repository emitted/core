package main

import (
	"log"
	"net/http"

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
)

func main() {

	go broker.Run()

	mux := pat.New()

	mux.Get("/ws/:key", http.HandlerFunc(handleConnections))

	http.Handle("/", mux)

	err := http.ListenAndServe(config.Server.Host+":"+config.Server.Port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {

	params := r.URL.Query()
	key := params.Get(":key")
	log.Println(key)

	// Checking tunnel's existance
	tunnel := &Tunnel{
		Key:       "PLSB987JB6",
		Clients:   make(map[uint32]*Client),
		Connected: 0,
	}

	conn, _ := upgrader.Upgrade(w, r, nil)

	client := NewClient(conn, tunnel)
	client.Connect(tunnel)

	defer func() {
		client.Terminate()
	}()

	// connecting subscriber to tunnel

	for {
		// type Message struct {
		// 	Message []byte
		// }
		// var message *Message
		// conn.ReadJSON(message)
	}
}
