package core

import (
	"context"
	"github.com/emitted/core/common/proto/clientproto"
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
		node:  n,
		apps:  make(map[string]*App),
		conns: make(map[string]*Client),
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

	for _, client := range h.apps[appKey].channels[channelName].clients {
		client.messageWriter.enqueue(payload)

		app.stats.deltaMessages++
	}

}

func (h *Hub) addSub(uid string, c *Client) {
	h.mu.Lock()
	if _, ok := h.conns[uid]; ok {
		h.mu.Unlock()
		return
	}
	h.conns[uid] = c
	h.mu.Unlock()
}

func (h *Hub) remSub(uid string) {
	h.mu.Lock()
	_, ok := h.conns[uid]
	if !ok {
		h.mu.Unlock()
		return
	}
	delete(h.conns, uid)
	h.mu.Unlock()
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

	_, ok = h.apps[appKey].channels[join.Channel]
	if !ok {
		h.node.logger.log(NewLogEntry(LogLevelError, "error broadcasting join: channel does not exist"))
		return
	}

	for _, client := range h.apps[appKey].channels[join.Channel].clients {
		client.messageWriter.enqueue(payload)

		app.stats.deltaMessages++
	}

}

func (h *Hub) BroadcastLeave(appKey string, leave *clientproto.Leave) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	app, ok := h.apps[appKey]
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

	_, ok = h.apps[appKey].channels[leave.Channel]
	if !ok {
		h.node.logger.log(NewLogEntry(LogLevelError, "error broadcasting leave: channel does not exist"))
		return
	}

	for _, client := range h.apps[appKey].channels[leave.Channel].clients {
		client.messageWriter.enqueue(payload)

		app.stats.deltaMessages++
	}

	h.node.logger.log(NewLogEntry(LogLevelDebug, "broadcasting leave", map[string]interface{}{"app": appKey, "channel": leave.Channel}))

}

func (h *Hub) shutdown(ctx context.Context) error {

	sem := make(chan struct{}, 128)

	apps := make([]*App, 0, len(h.apps))
	h.mu.Lock()
	for _, app := range h.apps {
		apps = append(apps, app)
	}
	h.mu.Unlock()

	closeFinishedCh := make(chan struct{}, len(apps))
	finished := 0

	if len(apps) == 0 {
		return nil
	}

	for _, app := range apps {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		go func(aapp *App) {
			defer func() { <-sem }()
			defer func() { closeFinishedCh <- struct{}{} }()
			aapp.Shutdown()
		}(app)
	}

	for {
		select {
		case <-closeFinishedCh:
			finished++
			if finished == len(apps) {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *Hub) Channels() []string {
	channels := make([]string, 0)
	for _, app := range h.apps {
		for ch := range app.channels {
			channels = append(channels, ch)
		}
	}

	return channels
}

func (h *Hub) NumSubscribers(app, ch string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	_, ok := h.apps[app]
	if !ok {
		return 0
	}
	conns := h.apps[app].channels[ch].clients
	return len(conns)
}
