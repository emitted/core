package main

import "encoding/json"
import "log"

// Message ...
type Message struct {
	Channel string `json:"channel"`
	Event string `json:"event"`
	Data  string `json:"data"`
}

type ClientMessage struct {
	Event string `json:"event"`
	Channel string `json:"channel"`
	Data json.RawMessage `json:"data"`
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

func NewSubscriptionTimeoutMessage() *ServiceMessage {
	return &ServiceMessage{
		Event: "emmet:connection_timeout",
		Data:  `{"code": 4003,"message": "connection not authorized within timeout"}`,
	}
}

func NewSubscriptionSucceededMessage(channelName string) *ServiceMessage {
	return &ServiceMessage{
		Event: "emmet:subscription_succeeded",
		Data:  `{"channel": "`+channelName+`"}`,
	}
}

func NewAuthenticationSucceededMessage(socketId uint32) *ServiceMessage {
	return &ServiceMessage{
		Event: "emmet:authentication_succeeded",
		Data:  `{"socket_id": "`+string(socketId)+`"}`,
	}
}

// Encode ...
func (m *ServiceMessage) Encode() []byte {
	payload, err := json.Marshal(m)
	if err != nil {
		log.Fatalln(err)
	}
	return payload
}
