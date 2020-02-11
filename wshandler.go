package core

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/fasthttp/websocket"
)

type websocketTransportOptions struct {
	pingInterval time.Duration
	writeTimeout time.Duration
}

type websocketTransport struct {
	mu        sync.RWMutex
	conn      *websocket.Conn
	closed    bool
	closeCh   chan struct{}
	options   websocketTransportOptions
	pingTimer *time.Timer
}

func newWebsocketTransport(conn *websocket.Conn, options websocketTransportOptions, closeCh chan struct{}) *websocketTransport {
	transport := &websocketTransport{
		conn:    conn,
		closeCh: closeCh,
		options: options,
	}
	if options.pingInterval > 0 {
		transport.addPing()
	}
	return transport
}

func (t *websocketTransport) ping() {
	select {
	case <-t.closeCh:
		return
	default:
		deadline := time.Now().Add(t.options.pingInterval / 2)
		err := t.conn.WriteControl(websocket.PingMessage, []byte("ping"), deadline)
		if err != nil {
			t.Close(DisconnectServerError)
			return
		}
		t.addPing()
	}
}

func (t *websocketTransport) addPing() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.pingTimer = time.AfterFunc(t.options.pingInterval, t.ping)
	t.mu.Unlock()
}

// Write ...
func (t *websocketTransport) Write(data []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		if t.options.writeTimeout > 0 {
			_ = t.conn.SetWriteDeadline(time.Now().Add(t.options.writeTimeout))
		}

		var messageType = websocket.BinaryMessage

		err := t.conn.WriteMessage(messageType, data)
		if err != nil {
			return err
		}

		if t.options.writeTimeout > 0 {
			_ = t.conn.SetWriteDeadline(time.Time{})
		}
		return nil
	}
}

func (t *websocketTransport) Close(disconnect *Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()

	if disconnect != nil {
		reason, err := json.Marshal(disconnect)
		if err != nil {
			return err
		}
		msg := websocket.FormatCloseMessage(disconnect.Code, string(reason))
		err = t.conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(time.Second))
		if err != nil {
			return t.conn.Close()
		}

		// Wait for closing handshake completion.
		t.conn.Close()
	}
	t.conn.Close()

	return nil
}

const (
	// DefaultWebsocketPingInterval ...
	DefaultWebsocketPingInterval = 25 * time.Second

	// DefaultWebsocketWriteTimeout ...
	DefaultWebsocketWriteTimeout = 1 * time.Second

	// DefaultWebsocketMessageSizeLimit ...
	DefaultWebsocketMessageSizeLimit = 65536 // 64KB
)

// WebsocketConfig ...
type WebsocketConfig struct {
	Compression        bool
	CompressionLevel   int
	CompressionMinSize int
	ReadBufferSize     int
	WriteBufferSize    int
	CheckOrigin        func(r *http.Request) bool
	PingInterval       time.Duration
	WriteTimeout       time.Duration
	MessageSizeLimit   int
}

type WebsocketHandler struct {
	config WebsocketConfig
	node   *Node
}

func NewWebsocketHandler(c WebsocketConfig, n *Node) *WebsocketHandler {
	return &WebsocketHandler{
		config: c,
		node:   n,
	}
}

func (s *WebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {

	compression := s.config.Compression
	compressionLevel := s.config.CompressionLevel

	upgrader := websocket.Upgrader{
		ReadBufferSize:    s.config.ReadBufferSize,
		WriteBufferSize:   s.config.WriteBufferSize,
		EnableCompression: s.config.Compression,
	}
	if s.config.CheckOrigin != nil {
		upgrader.CheckOrigin = s.config.CheckOrigin
	} else {
		upgrader.CheckOrigin = func(r *http.Request) bool {
			// Allow all connections.
			return true
		}
	}

	conn, err := upgrader.Upgrade(rw, r, nil)
	if err != nil {
		log.Fatalln(err)
		return
	}

	if compression {
		err := conn.SetCompressionLevel(compressionLevel)
		if err != nil {
			log.Fatalln(err)
		}
	}

	pingInterval := s.config.PingInterval
	if pingInterval == 0 {
		pingInterval = DefaultWebsocketPingInterval
	}
	writeTimeout := s.config.WriteTimeout
	if writeTimeout == 0 {
		writeTimeout = DefaultWebsocketWriteTimeout
	}
	//messageSizeLimit := s.config.MessageSizeLimit
	//if messageSizeLimit == 0 {
	messageSizeLimit := DefaultWebsocketMessageSizeLimit
	//}

	conn.SetReadLimit(int64(messageSizeLimit))
	if pingInterval > 0 {
		pongWait := pingInterval * 10 / 9
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})
	}

	go func() {
		opts := websocketTransportOptions{
			pingInterval: pingInterval,
			writeTimeout: writeTimeout,
		}
		closeCh := make(chan struct{}, 1)
		transport := newWebsocketTransport(conn, opts, closeCh)

		select {
		case <-s.node.NotifyShutdown():
			transport.Close(DisconnectShutdown)
			return
		default:
		}

		ctxCh := make(chan struct{})
		defer close(ctxCh)

		client, err := NewClient(s.node, newCustomCancelContext(r.Context(), ctxCh), transport)
		if err != nil {
			return
		}

		defer func() {
			client.Close(nil)
		}()

		for {

			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}

			client.Handle(data)

		}
	}()

}
