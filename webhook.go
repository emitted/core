package core

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/sireax/core/internal/proto/webhooks"
	"github.com/sireax/core/internal/timers"
	"time"
)

type (
	Webhook = webhooks.Webhook
)

const (
	WebhookEventChannelOccupied = webhooks.Event_CHANNEL_OCCUPIED
	WebhookEventChannelVacated  = webhooks.Event_CHANNEL_VACATED
	WebhookEventJoin            = webhooks.Event_PRESENCE_ADDED
	WebhookEventLeave           = webhooks.Event_PRESENCE_REMOVED
	WebhookEventPublication     = webhooks.Event_PUBLICATION
)

type webhookConfig struct {
	protocol, address, topic string
	partition                int

	writeTimeout int
}

var DefaultWebhookConfig = webhookConfig{
	protocol:  "tcp",
	address:   "localhost:9092",
	topic:     "emitted-channels-webhook",
	partition: 0,

	writeTimeout: 10,
}

type webhookManager struct {
	node   *Node
	config webhookConfig

	pubCh chan webhookRequest

	producer sarama.AsyncProducer
}

func NewWebhookManager(node *Node, config webhookConfig) *webhookManager {
	return &webhookManager{
		node:   node,
		config: config,

		pubCh: make(chan webhookRequest),
	}
}

func (w *webhookManager) Run() error {

	config := sarama.NewConfig()
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Timeout = time.Second * 5
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		w.node.logger.log(NewLogEntry(LogLevelError, "error setting up kafka", map[string]interface{}{"error": err.Error()}))
	}

	w.producer = producer

	go runForever(func() {
		w.runProducePipeline()
	})

	w.node.logger.log(newLogEntry(LogLevelInfo, "webhook dispatcher has been started"))

	return nil
}

func (w *webhookManager) runProducePipeline() {

	select {
	case <-w.node.NotifyShutdown():
		return
	default:
	}

	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case r := <-w.pubCh:

					w.producer.Input() <- &sarama.ProducerMessage{
						Topic:     "emitted-server-webhooks",
						Value:     sarama.ByteEncoder(r.data),
						Timestamp: time.Now(),
					}

					w.node.logger.log(NewLogEntry(LogLevelInfo, "just produced webhook ;)"))

				}
			}
		}()
	}
}

func (w *webhookManager) Enqueue(wh webhookRequest) error {
	select {
	case w.pubCh <- wh:
	default:
		timer := timers.SetTimer(time.Second * 5)
		defer timers.ReleaseTimer(timer)
		select {
		case w.pubCh <- wh:
		case <-timer.C:
			return errors.New("kafka webhook producing timeout")
		}
	}

	return nil
}

type webhookRequest struct {
	data []byte
}
