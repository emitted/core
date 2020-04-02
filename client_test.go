package core

import (
	"context"
	"github.com/sireax/core/internal/proto/clientproto"
	"github.com/stretchr/testify/require"
	"testing"
)

func testReplyWriter(replies *[]*clientproto.Reply) *replyWriter {
	return &replyWriter{
		write: func(reply *clientproto.Reply) error {
			*replies = append(*replies, reply)
			return nil
		},
		flush: func() error {
			return nil
		},
	}
}

func newTestTransport() *websocketTransport {
	return &websocketTransport{
		conn:      nil,
		protocol:  "",
		closed:    false,
		closeCh:   make(chan struct{}),
		options:   websocketTransportOptions{},
		pingTimer: nil,
	}
}

func newTestNode() *Node {
	return NewNode(Config{}, &BrokerConfig{
		Shards: []BrokerShardConfig{
			{
				Host:             "localhost",
				Port:             6379,
				Password:         "",
				DB:               0,
				UseTLS:           false,
				TLSSkipVerify:    false,
				MasterName:       "",
				IdleTimeout:      0,
				PubSubNumWorkers: 10,
				ReadTimeout:      0,
				WriteTimeout:     0,
				ConnectTimeout:   0,
			},
		},
	})
}

func TestNewClient(t *testing.T) {
	node := newTestNode()
	transport := newTestTransport()

	client, err := NewClient(node, context.Background(), transport)

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClientClose(t *testing.T) {
	node := newTestNode()
	defer node.Shutdown(context.Background())

	transport := newTestTransport()
	ctx := context.Background()

	client, _ := NewClient(node, ctx, transport)

	err := client.Close(DisconnectShutdown)
	require.True(t, transport.closed)
	require.Nil(t, err)
}

func TestReceiveMalformedCommand(t *testing.T) {
	node := newTestNode()
	transport := newTestTransport()
	ctx := context.Background()

	client, err := NewClient(node, ctx, transport)

	require.NoError(t, err)
	require.NotNil(t, client)

	var replies []*clientproto.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.HandleCommand(&clientproto.Command{
		Type: 10000000,
		Data: []byte("{}"),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorMethodNotFound, replies[0].Error)

	replies = nil
	disconnect = client.HandleCommand(&clientproto.Command{
		Type: 2,
		Data: []byte("{}"),
	}, rw.write, rw.flush)
	require.Equal(t, DisconnectBadRequest, disconnect)
}
