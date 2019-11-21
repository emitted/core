package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/bmizerany/pat"
	"github.com/gorilla/websocket"
	. "github.com/logrusorgru/aurora"
	errs "github.com/sireax/Emmet-Go-Server/internal/errors"

	Config "github.com/sireax/Emmet-Go-Server/config"
	Hub "github.com/sireax/Emmet-Go-Server/internal/hub"
	p "github.com/sireax/Emmet-Go-Server/internal/packet"
	Pool "github.com/sireax/Emmet-Go-Server/internal/pool"
)

var (
	// creating an instance of Hub - storage for tunnels
	hub = Hub.NewHub()
	// creating an instace of Pool that keeps workers
	pool = Pool.NewPool(hub, 15)
	// upgrader
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	config = Config.GetConfig()
)

func main() {

	pool.Run()

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

	// Checking tunnel's existance
	tunnel, err := hub.GetTunnel(key)
	if err != nil {
		switch err.(type) {

		case *errs.ErrTunnelNotFound:
			w.WriteHeader(http.StatusForbidden)

		case *errs.ErrConnLimitReached:
			w.WriteHeader(http.StatusUnauthorized)

		case *errs.ErrMessagesLimitReached:
			w.WriteHeader(http.StatusUnauthorized)
		}

		return
	}

	log.Println(Green("A client subscribed to:"), Bold(Cyan(key)))

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
			ws.WriteJSON(p.NewPacket("error", []byte(err.Error())))
			return
		}

	}
}
