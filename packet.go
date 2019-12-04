package main

import (
	"encoding/json"
)

// Packet structure represents an instance
// of a data, that client sends and recieves.
type Packet struct {
	Event string `json:"event"`
	Data  []byte `json:"data"`
}

// NewPacket creates a packet instance
func NewPacket(event string, data []byte) *Packet {
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
	var packet Packet
	err := json.Unmarshal([]byte(p.Data), &packet)
	if err != nil {
		return nil, err
	}

	return &packet, nil
}

// Unmarshal ...
func (p *Publication) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, p)
	if err != nil {
		return err
	}

	return nil
}
