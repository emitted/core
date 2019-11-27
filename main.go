package main

import (
	"log"
	"net/http"

	"github.com/bmizerany/pat"
	"github.com/gorilla/websocket"
	. "github.com/logrusorgru/aurora"
	errs "github.com/sireax/Emmet-Go-Server/internal/errors"

	c "github.com/sireax/Emmet-Go-Server/internal/client"
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

	broker.Run()

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
			w.WriteHeader(http.StatusEarlyHints)

		case *errs.ErrConnLimitReached:
			w.WriteHeader(http.StatusUnauthorized)

		case *errs.ErrMessagesLimitReached:
			w.WriteHeader(http.StatusUnauthorized)
		}

		return
	}

	log.Println(Green("A client subscribed to:"), Bold(Cyan(key)))

	conn, _ := upgrader.Upgrade(w, r, nil)
	client := c.NewClient(conn)

	defer func() {
		tunnel.DisconnectClient(client)
		conn.Close()
	}()

	// connecting subscriber to tunnel
	tunnel.ConnectClient(client)

	for {

	}
}
