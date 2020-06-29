package core

import (
	"bitbucket.org/sireax/core/common/proto/clientproto"
	"bitbucket.org/sireax/core/common/proto/nodeproto"
	"bitbucket.org/sireax/core/common/uuid"
	"context"
	"github.com/FZambia/eagle"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"sync"
	"time"
)

// Node ...
type Node struct {
	mu        sync.RWMutex
	uid       string
	startedAt int64

	hub *Hub

	mongo  *Mongo
	broker *Broker
	//webhooks *webhookManager

	config Config
	nodes  *nodeRegistry

	shutdown   bool
	shutdownCh chan struct{}
	logger     *logger
	subLocks   map[int]*sync.Mutex

	protoEncoder nodeproto.Encoder
	protoDecoder nodeproto.Decoder

	metricsMu       sync.Mutex
	metricsExporter *eagle.Eagle
	metricsSnapshot *eagle.Metrics
	metrics         *NodeMetrics
}

type NodeMetrics struct {
	clients  int
	channels int
}

const (
	numSubLocks = 16384
)

func (n *Node) NotifyShutdown() chan struct{} {
	return n.shutdownCh
}

func NewNode(c Config, brokerConfig *BrokerConfig, mongoConfig MongoConfig) *Node {
	uid := uuid.Must(uuid.NewV4()).String()

	subLocks := make(map[int]*sync.Mutex, numSubLocks)
	for i := 0; i < numSubLocks; i++ {
		subLocks[i] = &sync.Mutex{}
	}

	n := &Node{
		uid:          uid,
		nodes:        newNodeRegistry(uid),
		config:       c,
		startedAt:    time.Now().Unix(),
		shutdownCh:   make(chan struct{}),
		logger:       nil,
		protoEncoder: nodeproto.NewProtobufEncoder(),
		protoDecoder: nodeproto.NewProtobufDecoder(),
		metrics: &NodeMetrics{
			clients:  0,
			channels: 0,
		},
		subLocks: subLocks,
	}

	if c.LogHandler != nil {
		n.logger = newLogger(c.LogLevel, c.LogHandler)
	}

	broker, err := NewBroker(n, brokerConfig)
	if err != nil {
		log.Fatal(err)
	}

	n.broker = broker

	n.hub = NewHub(n)

	n.mongo = NewMongo(n, mongoConfig)

	//n.webhooks = NewWebhookManager(n, kafkaConfig)

	return n
}

func (n *Node) Run() error {

	err := n.broker.Run()
	if err != nil {
		return err
	}

	//err = n.webhooks.Run()
	//if err != nil {
	//	return err
	//}

	err = n.mongo.Run()
	if err != nil {
		return err
	}

	err = n.initMetrics()
	if err != nil {
		return err
	}

	go n.sendNodePing()
	go n.cleanNodeInfo()
	go n.updateMetrics()

	return nil
}

func (n *Node) updateMetrics() {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-n.NotifyShutdown():
			ticker.Stop()
			return
		case <-ticker.C:
			//n.updateGauges()
		}
	}
}

func (n *Node) Shutdown(ctx context.Context) error {
	n.mu.RLock()
	if n.shutdown {
		n.mu.Unlock()
		return nil
	}
	n.shutdown = true
	close(n.shutdownCh)
	n.mu.RUnlock()

	return n.hub.shutdown(ctx)
}

func (n *Node) initMetrics() error {

	metricsSink := make(chan eagle.Metrics)
	n.metricsExporter = eagle.New(eagle.Config{
		Gatherer: prometheus.DefaultGatherer,
		Interval: 5 * time.Second,
		Sink:     metricsSink,
	})
	metrics, err := n.metricsExporter.Export()
	if err != nil {
		return err
	}

	n.metricsMu.Lock()
	n.metricsSnapshot = &metrics
	n.metricsMu.Unlock()
	go func() {
		for {
			select {
			case <-n.NotifyShutdown():
				return
			case metrics := <-metricsSink:
				n.metricsMu.Lock()
				n.metricsSnapshot = &metrics
				n.metricsMu.Unlock()
			}
		}
	}()

	return nil
}

func (n *Node) sendNodePing() {
	ticker := time.NewTicker(time.Second * 3)
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			err := n.pubNode()
			if err != nil {
				n.logger.log(newLogEntry(LogLevelError, "error publishing node control command", map[string]interface{}{"error": err.Error()}))
			}
		}
	}
}

func (n *Node) cleanNodeInfo() {
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-time.After(time.Second * 9):
			n.mu.RLock()
			delay := time.Duration(5)
			n.mu.RUnlock()
			n.nodes.clean(delay)
		}
	}
}

func (n *Node) getApp(secret string) (*App, error) {

	app, err := n.mongo.GetAppBySecret(secret)
	if err != nil {
		return nil, err
	}

	okApp, ok := n.hub.apps[app.ID]
	if !ok {

		app.node = n
		app.Clients = make(map[string]*Client)
		app.Channels = make(map[string]*Channel)

		app.stats = AppStats{
			connections:      0,
			deltaConnections: 0,
			messages:         0,
			deltaMessages:    0,
		}
		n.hub.AddApp(app)

		go app.run()

		return app, nil
	}

	return okApp, nil
}

func (n *Node) Publish(chId string, clientInfo *clientproto.ClientInfo, r *clientproto.PublishRequest) error {
	return n.broker.Publish(chId, clientInfo, r)
}

func (n *Node) AddPresence(ch, uid string, clientInfo *clientproto.ClientInfo) error {
	return n.broker.AddPresence(ch, uid, clientInfo)
}

func (n *Node) RemovePresence(ch, uid string) error {
	return n.broker.RemovePresence(ch, uid)
}

func (n *Node) Presence(ch string) (map[string]*clientproto.ClientInfo, error) {
	return n.broker.Presence(ch)
}

func (n *Node) GetPresence(ch, uid string) (*clientproto.ClientInfo, error) {
	return n.broker.GetPresence(ch, uid)
}

func (n *Node) UpdateAppStats(app string, conns, msgs int) error {
	return n.broker.UpdateStats(app, conns, msgs)
}

func (n *Node) RetrieveStats(app string) (int, int, error) {
	return n.broker.RetrieveStats(app)
}

func (n *Node) Channels(app string) ([]string, error) {
	return n.broker.Channels(app)
}

func (n *Node) pubNode() error {

	n.mu.RLock()

	node := &nodeproto.Node{
		UID:     n.uid,
		Name:    n.config.Name,
		Version: n.config.Version,
		Uptime:  uint32(time.Now().Unix() - n.startedAt),
	}

	n.metricsMu.Lock()
	if n.metricsSnapshot != nil {
		node.Metrics = n.getMetrics(*n.metricsSnapshot)
	}

	n.metricsSnapshot = nil
	n.metricsMu.Unlock()

	n.mu.RUnlock()

	params, _ := n.protoEncoder.EncodeNode(node)

	cmd := &nodeproto.Command{
		UID:    n.uid,
		Method: nodeproto.MethodTypeNode,
		Params: params,
	}

	err := n.nodeCmd(node)
	if err != nil {
		log.Fatal(err)
	}

	return n.publishNode(cmd)

	return nil
}

func (n *Node) nodeCmd(node *nodeproto.Node) error {
	n.nodes.add(node)
	return nil
}

func (n *Node) publishNode(cmd *nodeproto.Command) error {
	messagesSentCountControl.Inc()
	data, err := n.protoEncoder.EncodeCommand(cmd)
	if err != nil {
		return err
	}

	return n.broker.PublishNode(data)
}

func (n *Node) getMetrics(metrics eagle.Metrics) *nodeproto.Metrics {
	return &nodeproto.Metrics{
		Interval: n.config.NodeInfoMetricsAggregateInterval.Seconds(),
		Items:    metrics.Flatten("."),
	}
}

type nodeRegistry struct {
	mu         sync.RWMutex
	currentUID string
	nodes      map[string]nodeproto.Node
	updates    map[string]int64
}

func newNodeRegistry(currentUID string) *nodeRegistry {
	return &nodeRegistry{
		currentUID: currentUID,
		nodes:      make(map[string]nodeproto.Node),
		updates:    make(map[string]int64),
	}
}

func (r *nodeRegistry) list() []nodeproto.Node {
	r.mu.RLock()
	nodes := make([]nodeproto.Node, len(r.nodes))
	i := 0
	for _, info := range r.nodes {
		nodes[i] = info
		i++
	}
	r.mu.RUnlock()
	return nodes
}

func (r *nodeRegistry) get(uid string) nodeproto.Node {
	r.mu.RLock()
	info := r.nodes[uid]
	r.mu.RUnlock()
	return info
}

func (r *nodeRegistry) add(info *nodeproto.Node) {
	r.mu.Lock()
	if node, ok := r.nodes[info.UID]; ok {
		if info.Metrics != nil {
			r.nodes[info.UID] = *info
		} else {
			node.Version = info.Version
			node.NumClients = info.NumClients
			node.Uptime = info.Uptime
			r.nodes[info.UID] = node
		}
	} else {
		r.nodes[info.UID] = *info
	}
	r.updates[info.UID] = time.Now().Unix()
	r.mu.Unlock()
}

func (n *Node) handleNodeInfo(data []byte) {
	cmd, err := n.protoDecoder.DecodeCommand(data)
	if err != nil {
		return
	}

	if cmd.UID == n.uid {
		return
	}

	method := cmd.Method
	params := cmd.Params

	switch method {
	case nodeproto.MethodTypeNode:
		info, err := n.protoDecoder.DecodeNode(params)
		if err != nil {

		}

		n.nodes.add(info)
	default:
		n.logger.log(NewLogEntry(LogLevelError, "invalid node method"))
	}
}

func (r *nodeRegistry) clean(delay time.Duration) {
	r.mu.Lock()
	for uid := range r.nodes {
		if uid == r.currentUID {
			// No need to clean info for current node.
			continue
		}
		updated, ok := r.updates[uid]
		if !ok {
			// As we do all operations with nodes under lock this should never happen.
			delete(r.nodes, uid)
			continue
		}
		if time.Now().Unix()-updated > int64(delay.Seconds()) {
			// Too many seconds since this node have been last seen - remove it from map.
			delete(r.nodes, uid)
			delete(r.updates, uid)
		}
	}
	r.mu.Unlock()
}
