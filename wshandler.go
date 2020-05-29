package core

import (
	"encoding/json"
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
	protocol  string
	closed    bool
	closeCh   chan struct{}
	options   websocketTransportOptions
	pingTimer *time.Timer
}

func newWebsocketTransport(conn *websocket.Conn, protocol string, options websocketTransportOptions, closeCh chan struct{}) *websocketTransport {
	transport := &websocketTransport{
		conn:     conn,
		protocol: protocol,
		closeCh:  closeCh,
		options:  options,
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
			err := t.Close(DisconnectServerError)
			if err != nil {

			}
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
		err = t.conn.Close()
		if err != nil {

		}
	}
	err := t.conn.Close()
	if err != nil {

	}

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

var (
	ProtocolList = []string{
		"1.0",
		"1.5",
		"2.0",
	}
)

func validateProtocolVersion(protocol string) bool {
	if protocol == "" {
		return false
	}

	for _, b := range ProtocolList {
		if b == protocol {
			return true
		}
	}
	return false
}

func (s *WebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {

	protocol := r.URL.Query().Get("protocol")
	if validateProtocolVersion(protocol) == false {
		protocol = "6.0.0"
	}

	secret := r.URL.Query().Get("secret")
	if secret == "" {
		rw.WriteHeader(http.StatusForbidden)
		return
	}

	app, err := s.node.getApp(secret)
	if err != nil {
		rw.WriteHeader(http.StatusForbidden)
		s.node.logger.log(NewLogEntry(LogLevelError, "client error connecting to app", map[string]interface{}{"error": err.Error()}))
		return
	}

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
		s.node.logger.log(NewLogEntry(LogLevelError, "error upgrading http connection", map[string]interface{}{"error": err.Error()}))
		return
	}

	if compression {
		err := conn.SetCompressionLevel(compressionLevel)
		if err != nil {
			s.node.logger.log(NewLogEntry(LogLevelError, "error setting compression level", map[string]interface{}{"error": err.Error()}))
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
	messageSizeLimit := s.config.MessageSizeLimit
	if messageSizeLimit == 0 {
		messageSizeLimit = DefaultWebsocketMessageSizeLimit
	}

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
		transport := newWebsocketTransport(conn, protocol, opts, closeCh)

		select {
		case <-s.node.NotifyShutdown():
			err := transport.Close(DisconnectShutdown)
			if err != nil {
				s.node.logger.log(NewLogEntry(LogLevelError, "error closing transport"))
			}
			return
		default:
		}

		ctxCh := make(chan struct{})
		defer close(ctxCh)

		client, err := NewClient(s.node, newCustomCancelContext(r.Context(), ctxCh), transport)
		if err != nil {
			return
		}

		client.Connect(app)

		defer func() {
			close(closeCh)
			err := client.Close(nil)
			if err != nil {
				client.node.logger.log(newLogEntry(LogLevelError, "error closing client"))
			}
		}()

		var handleMu sync.RWMutex
		var msgCount int

		ticker := time.NewTicker(time.Second)
		go func() {
			for {
				select {
				case <-ticker.C:
					msgCount = 0
				case <-s.node.NotifyShutdown():
					ticker.Stop()
				}
			}
		}()

		for {

			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}

			go func() {
				handleMu.RLock()
				defer handleMu.RUnlock()

				if msgCount >= 10 {
					s.node.logger.log(NewLogEntry(LogLevelDebug, "client sent more than 10 messages in one second", map[string]interface{}{"client": client.uid}))
					return
				}
				client.Handle(data)
				msgCount++
			}()
		}
	}()

}
