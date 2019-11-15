package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/bmizerany/pat"
	"github.com/gorilla/websocket"
	. "github.com/logrusorgru/aurora"

	h "github.com/sireax/Emmet-Go-Server/internal/hub"
	p "github.com/sireax/Emmet-Go-Server/internal/packet"
)

var (
	// hub variable
	hub = h.NewHub()
	// upgrader
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func main() {

	go hub.Run()

	mux := pat.New()

	mux.Get("/ws/:secret", http.HandlerFunc(handleConnections))

	http.Handle("/", mux)

	log.Println(Green("http server started on :8000"))
	err := http.ListenAndServe("localhost:8000", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {

	params := r.URL.Query()
	secret := params.Get(":secret")

	// Checking tunnel's existance
	tunnel, err := hub.GetTunnel(secret)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	fmt.Println("A client subscribed to:", secret)

	ws, _ := upgrader.Upgrade(w, r, nil)
	defer func() {
		tunnel.DisconnectSubscriber(ws)
		ws.Close()
	}()

	// connecting subscriber to tunnel
	tunnel.ConnectSubscriber(ws)
	data, _ := json.Marshal(`Authentication succeded`)
	ws.WriteJSON(p.NewPacket("auth_succeded", data))

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
