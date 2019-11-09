package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/bmizerany/pat"
	"github.com/gorilla/websocket"

	h "github.com/sireax/emitted/internal/hub"
	p "github.com/sireax/emitted/internal/packet"
)

var (
	hub = h.NewHub() // hub variable

	// upgrader
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func main() {

	mux := pat.New()

	mux.Get("/ws/:secret", http.HandlerFunc(handleConnections))

	http.Handle("/", mux)

	log.Println("http server started on :8000")
	err := http.ListenAndServe("localhost:8000", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {

	params := r.URL.Query()
	secret := params.Get(":secret")

	// Checking tunnel's existance
	tunnel, err := hub.GetOrCreateTunnel(secret)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	fmt.Println("A client subscribed to:", secret)

	ws, _ := upgrader.Upgrade(w, r, nil)
	defer ws.Close()

	// connecting subscriber to tunnel
	tunnel.ConnectSubscriber(ws)

	defer tunnel.DisconnectSubscriber(ws)

	for {
		var packet *p.Packet

		err := ws.ReadJSON(&packet)

		if err != nil {
			val, _ := json.Marshal(err)
			ws.WriteJSON(p.NewPacket("error", val))
			fmt.Println("invalid packet structure")
			return
		}

	}
}
