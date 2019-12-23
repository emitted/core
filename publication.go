package main

import "encoding/json"
import "log"

// Message ...
type Message struct {
	Channel string `json:"channel"`
	Event string `json:"event"`
	Data  string `json:"data"`
}

// Encode ...
func (m *Message) Encode() []byte {
	payload, err := json.Marshal(m)
	if err != nil {
		log.Fatalln(err)
	}
	return payload
}

type ServiceMessage struct {
	Event string `json:"event"`
	Data string `json:"data"`
}

// Encode ...
func (m *ServiceMessage) Encode() []byte {
	payload, err := json.Marshal(m)
	if err != nil {
		log.Fatalln(err)
	}
	return payload
}

var (
	// AuthenticatedMessageResponse is usually sent when user is successfully connected to a channel
	AuthenticatedMessageResponse = &ServiceMessage{
		Event: "emmet:authenticated",
		Data:  "authentication succeeded",
	}
)
