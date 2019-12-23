package main

import (
	"github.com/centrifugal/centrifuge"
	"log"
	"math/rand"
	"sync"
	"time"
)

// NodeConfig ...
type NodeConfig struct {
}

// Node ...
type Node struct {
	mu         sync.RWMutex
	uid        string
	startedAt  int64
	broker 	   *Broker
	hub 	   *Hub
	config     NodeConfig
	nodes      *nodeRegistry
	shutdown   bool
	shutdownCh chan struct{}
	logger     *logger
	subLocks   map[int]*sync.Mutex
	metrics *NodeMetrics
}

type NodeMetrics struct {
	clients int
	channels int
}

const (
	numSubLocks = 16384
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

// NewNode ...
func NewNode(c NodeConfig) *Node {
	uid := string(rand.Uint64())

	subLocks := make(map[int]*sync.Mutex, numSubLocks)
	for i := 0; i < numSubLocks; i++ {
		subLocks[i] = &sync.Mutex{}
	}

	n := &Node{
		uid:        uid,
		nodes:      newNodeRegistry(uid),
		config:     c,
		startedAt:  time.Now().Unix(),
		shutdownCh: make(chan struct{}),
		logger:     nil,
		metrics: &NodeMetrics{
			clients:  0,
			channels: 0,
		},
		subLocks:   subLocks,
	}

	n.broker = NewBroker()
	n.hub = NewHub()

	return n
}

type nodeRegistry struct {
	// mu allows to synchronize access to node registry.
	mu sync.RWMutex
	// currentUID keeps uid of current node
	currentUID string
	// nodes is a map with information about known nodes.
	nodes map[string]Node
	// updates track time we last received ping from node. Used to clean up nodes map.
	updates map[string]int64
}

func newNodeRegistry(currentUID string) *nodeRegistry {
	return &nodeRegistry{
		currentUID: currentUID,
		nodes:      make(map[string]Node),
		updates:    make(map[string]int64),
	}
}

func (node *Node) Run() error  {

	err := node.broker.Run()
	if err != nil {
		return err
	}

	go node.updateMetrics()

	return nil
}

func (node *Node) updateMetrics() {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <- ticker.C:
			node.metrics.channels = node.hub.numConnections
			node.metrics.clients = node.hub.numClients
		}
	}
}
