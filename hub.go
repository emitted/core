package core

import (
	"context"
	"github.com/sireax/core/common/proto/clientproto"
	"sync"
)

type Hub struct {
	node *Node

	mu    sync.RWMutex
	apps  map[string]*App
	conns map[string]*Client
}

func NewHub(n *Node) *Hub {
	return &Hub{
		node: n,
		apps: make(map[string]*App),
	}
}

func (h *Hub) AddApp(app *App) {
	h.mu.RLock()
	_, ok := h.apps[app.ID]
	if !ok {
		h.apps[app.ID] = app
	}
	h.mu.RUnlock()
}

func (h *Hub) BroadcastPublication(appKey string, channelName string, pub *clientproto.Publication) {

	h.mu.RLock()
	defer h.mu.RUnlock()

	app, ok := h.apps[appKey]
	if !ok {
		h.node.logger.log(NewLogEntry(LogLevelError, "error broadcasting publication", map[string]interface{}{"error": "app is not found"}))
		return
	}

	data, _ := pub.Marshal()

	push := &clientproto.Event{
		Type: clientproto.EventType_PUBLICATION,
		Data: data,
	}

	payload, _ := push.Marshal()

	for _, client := range h.apps[appKey].Channels[channelName].Clients {
		client.messageWriter.enqueue(payload)
	}

	app.Stats.Messages++

}

func (h *Hub) BroadcastJoin(appKey string, join *clientproto.Join) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	app, ok := h.apps[appKey]
	if !ok {
		h.node.logger.log(NewLogEntry(LogLevelError, "error broadcasting join", map[string]interface{}{"error": "app is not found"}))
		return
	}

	data, err := join.Marshal()
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error marshaling join", map[string]interface{}{"error": err.Error()}))
	}

	push := &clientproto.Event{
		Type: clientproto.EventType_JOIN,
		Data: data,
	}

	payload, err := push.Marshal()
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error marshaling push", map[string]interface{}{"error": err.Error()}))
	}

	_, ok = h.apps[appKey].Channels[join.Channel]
	if !ok {
		h.node.logger.log(NewLogEntry(LogLevelError, "error broadcasting join: channel does not exist"))
		return
	}

	for _, client := range h.apps[appKey].Channels[join.Channel].Clients {
		client.messageWriter.enqueue(payload)
	}

	app.Stats.Join++
	//	todo

}

func (h *Hub) BroadcastLeave(appKey string, leave *clientproto.Leave) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	h.mu.Lock()
	app, ok := h.apps[appKey]
	h.mu.Unlock()
	if !ok {
		h.node.logger.log(NewLogEntry(LogLevelError, "error broadcasting leave", map[string]interface{}{"error": "app is not found"}))
		return
	}

	data, err := leave.Marshal()
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error marshaling leave", map[string]interface{}{"error": err.Error()}))
	}

	push := &clientproto.Event{
		Type: clientproto.EventType_LEAVE,
		Data: data,
	}

	payload, err := push.Marshal()
	if err != nil {
		h.node.logger.log(newLogEntry(LogLevelError, "error marshaling leave", map[string]interface{}{"error": err.Error()}))
	}

	_, ok = h.apps[appKey].Channels[leave.Channel]
	if !ok {
		h.node.logger.log(NewLogEntry(LogLevelError, "error broadcasting leave: channel does not exist"))
		return
	}

	for _, client := range h.apps[appKey].Channels[leave.Channel].Clients {
		client.messageWriter.enqueue(payload)
	}

	h.node.logger.log(NewLogEntry(LogLevelDebug, "broadcasting leave", map[string]interface{}{"app": appKey, "channel": leave.Channel}))

	app.Stats.Leave++

}

func (h *Hub) shutdown(ctx context.Context) error {
	advice := DisconnectShutdown

	sem := make(chan struct{}, 128)

	clients := make([]*Client, 0, len(h.conns))
	h.mu.Lock()
	for _, client := range h.conns {
		clients = append(clients, client)
	}
	h.mu.Unlock()

	closeFinishedCh := make(chan struct{}, len(clients))
	finished := 0

	if len(clients) == 0 {
		return nil
	}

	for _, client := range clients {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		go func(cc *Client) {
			defer func() { <-sem }()
			defer func() { closeFinishedCh <- struct{}{} }()
			cc.Close(advice)
		}(client)
	}

	for {
		select {
		case <-closeFinishedCh:
			finished++
			if finished == len(clients) {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *Hub) Channels() []string {
	channels := make([]string, 0)
	h.mu.Lock()
	for _, app := range h.apps {
		for ch := range app.Channels {
			channels = append(channels, ch)
		}
	}
	h.mu.Unlock()

	return channels
}

func (h *Hub) NumSubscribers(app, ch string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	_, ok := h.apps[app]
	if !ok {
		return 0
	}
	conns := h.apps[app].Channels[ch].Clients
	return len(conns)
}
