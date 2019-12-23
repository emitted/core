package main

import (
	"log"
	"net/http"
	"time"

	"github.com/bmizerany/pat"
	"github.com/sireax/Emmet-Go-Server/internal/api"
)

var (
	// getting configuration file
	config = GetConfig()
	//
	node = NewNode(NodeConfig{})
	// wsHandler is used to handle ws connections
	wsHandler = &WebsocketHandler{
		config: WebsocketConfig{
			MessageSizeLimit: 100,
			PingInterval:     time.Second,
		},
	}

	app = &App{
		Key:      "Njc4ZGYyZDgtN2ZjMC00ZjAwLWI2OWYtZTZhMGQxYjU4YjQ4",
		Secret:   "8788369a-227f-11ea-8cb8-1ee2210c29b2",
		Cluster:  "US",
		Clients:  make(map[uint32]*Client, 0),
		Channels: make(map[string]*Channel, 0),
		Options: &AppOptions{
			MaxConnections: 1500,
		},
		Stats: &AppStats{
			Connections: 0,
			Messages:    0,
		},
	}

	channel = NewChannel(app, "chat", &ChannelOptions{})

	channel2 = NewChannel(app, "nofitications", &ChannelOptions{})
)

func main() {

	node.Run()

	mux := pat.New()

	mux.Get("/ws/:secret", http.HandlerFunc(wsHandler.ServeHTTP))

	mux.Get("/api/:secret/get", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		type Res struct {
			App         string `json:"app_secret"`
			Connections int    `json:"connections"`
		}

		params := r.URL.Query()
		secret := params.Get(":secret")

		application, err := node.hub.FindApp(secret)

		if err != nil {
			w.WriteHeader(403)
			w.Write([]byte("Application does not exist or there is no connections"))
			return
		}

		data := api.NewAppInfoResponse(application.Key, application.Secret, application.Stats.Connections, 0).Marshal()

		w.WriteHeader(200)
		w.Write(data)

	}))

	http.Handle("/", mux)

	err := http.ListenAndServe(config.Server.Host+":"+config.Server.Port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
