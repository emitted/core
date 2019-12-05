package main

// Hub struct contains all data and structs of clients,tunnels etc.
type Hub struct {
	Subs    map[string]*Tunnel // tunnels map
	Clients map[uint32]*Client
}

// NewHub is a constructor method for the Hub struct
func NewHub() *Hub {
	return &Hub{
		Subs:    make(map[string]*Tunnel, 0),
		Clients: make(map[uint32]*Client, 0),
	}
}

// AddSub ...
func (hub *Hub) AddSub(tunnel *Tunnel) (bool, error) {
	_, ok := hub.Subs[tunnel.Key]
	if !ok {
		hub.Subs[tunnel.Key] = tunnel
	}

	if !ok {
		return true, nil
	}
	return false, nil
}

// BroadcastMessage ...
func (hub *Hub) BroadcastMessage(tunnel string, publication Publication) {
	for _, client := range hub.Subs[tunnel].Clients {
		packet, _ := publication.ToPacket()
		client.MessageWriter.enqueue(packet.Encode())
	}
}

// Tunnels ...
func (hub *Hub) Tunnels() []string {
	tunnels := make([]string, 0)
	for t := range hub.Subs {
		tunnels = append(tunnels, t)
	}

	return tunnels
}
