package packet

import (
	"encoding/json"
)

// Packet structure represents an instance
// of a data, that client sends.
type Packet struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

// NewPacket creates a packet instance
func NewPacket(event string, data json.RawMessage) *Packet {
	return &Packet{
		Event: event,
		Data:  data,
	}
}
