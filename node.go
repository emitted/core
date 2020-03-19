package core

import (
	"context"
	"github.com/FZambia/eagle"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sireax/core/internal/proto/nodeproto"
	"github.com/sireax/core/internal/uuid"
	"log"
	"sync"
	"time"
)

// Node ...
type Node struct {
	mu         sync.RWMutex
	uid        string
	startedAt  int64
	broker     *Broker
	hub        *Hub
	webhook    *webhookManager
	config     Config
	nodes      *nodeRegistry
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

// NewNode ...
func NewNode(c Config, brokerConfig *BrokerConfig) *Node {
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
	n.webhook = NewWebhookManager(n, DefaultWebhookConfig)

	return n
}

func (n *Node) Run() error {

	err := n.broker.Run()
	if err != nil {
		return err
	}

	err = n.webhook.Run()
	if err != nil {
		return err
	}

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
			//n.metrics.channels = n.hub.numConnections
			//n.metrics.clients = n.hub.numClients
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

func (n *Node) AddPresence(ch, uid string, clientInfo []byte) error {
	return n.broker.AddPresence(ch, uid, clientInfo)
}

func (n *Node) RemovePresence(ch, uid string) error {
	return n.broker.RemovePresence(ch, uid)
}

func (n *Node) Presence(ch string) (map[string][]byte, error) {
	return n.broker.Presence(ch)
}

func (n *Node) GetPresence(ch, uid string) ([]byte, error) {
	return n.broker.GetPresence(ch, uid)
}

func (n *Node) UpdateAppStats(app string, stats AppStats) error {
	return n.broker.UpdateStats(app, stats)
}

func (n *Node) pubNode() error {

	n.mu.RLock()

	node := &nodeproto.Node{
		UID:     n.uid,
		Name:    n.config.Name,
		Version: n.config.Version,
		//NumClients:  uint32(n.hub.NumClients()),
		//NumChannels: uint32(n.hub.NumChannels()),
		Uptime: uint32(time.Now().Unix() - n.startedAt),
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
	//messagesSentCountControl.Inc()
	_, err := n.protoEncoder.EncodeCommand(cmd)
	if err != nil {
		return err
	}
	//return n.broker.PublishNode(data)
	return nil
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

func (r *nodeRegistry) clean(delay time.Duration) {
	r.mu.Lock()
	for uid := range r.nodes {
		if uid == r.currentUID {
			// No need to clean info for current nodeproto.
			continue
		}
		updated, ok := r.updates[uid]
		if !ok {
			// As we do all operations with nodes under lock this should never happen.
			delete(r.nodes, uid)
			continue
		}
		if time.Now().Unix()-updated > int64(delay.Seconds()) {
			// Too many seconds since this nodeproto have been last seen - remove it from map.
			delete(r.nodes, uid)
			delete(r.updates, uid)
		}
	}
	r.mu.Unlock()
}
