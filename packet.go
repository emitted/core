package main

import (
	"encoding/json"
	"log"
)

// Packet structure represents an instance
// of a data, that client sends and recieves.
type Packet struct {
	Event string `json:"event"`
	Data  string `json:"data"`
}

// NewPacket creates a packet instance
func NewPacket(event string, data string) *Packet {
	return &Packet{
		Event: event,
		Data:  data,
	}
}

// Encode ...
func (p *Packet) Encode() []byte {
	packet, _ := json.Marshal(p)
	return packet
}

// Publication ...
type Publication struct {
	Tunnel string
	Data   json.RawMessage
}

// ToPacket ...
func (p Publication) ToPacket() (*Packet, error) {
	var packet *Packet

	pData, err := p.Data.MarshalJSON()
	if err != nil {
		log.Fatalln("err marshaling publication to packet: ", err)
	}

	data := string(pData)

	packet = &Packet{
		Event: "message",
		Data:  data,
	}

	return packet, nil
}

// Unmarshal ...
func (p *Publication) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, p)
	if err != nil {
		return err
	}

	return nil
}
