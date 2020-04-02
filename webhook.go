package core

import (
	"github.com/Shopify/sarama"
	"github.com/sireax/core/internal/proto/webhooks"
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
	topic:     "emitted-server-webhooks",
	partition: 0,
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
	//config.Producer.Compression = sarama.CompressionSnappy
	//config.Producer.Timeout = time.Second * 5
	//config.Producer.Return.Successes = true
	//config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		return err
	}

	w.producer = producer

	go w.runProducePipeline()

	w.node.logger.log(newLogEntry(LogLevelInfo, "webhook dispatcher has been started"))

	return nil
}

func (w *webhookManager) runProducePipeline() {

	select {
	case <-w.node.NotifyShutdown():
		return
	default:
	}

	ticker := time.NewTicker(time.Second * 5)
	pingMsg := &sarama.ProducerMessage{
		Topic: "emitted-server-webhooks-ping",
		Value: sarama.StringEncoder("PING"),
	}

	go func() {
		for {
			select {
			case <-ticker.C:

				w.producer.Input() <- pingMsg

			case r := <-w.pubCh:

				msg := &sarama.ProducerMessage{
					Topic:     "emitted-server-webhooks",
					Value:     sarama.ByteEncoder(r.data),
					Timestamp: time.Now(),
				}

				select {
				case w.producer.Input() <- msg:
				}

			}

		}
	}()

	for {
		select {
		case err := <-w.producer.Errors():
			w.node.logger.log(NewLogEntry(LogLevelError, "failed to produce webhook", map[string]interface{}{"error": err.Error()}))
		}
	}
}

func (w *webhookManager) Enqueue(wh webhookRequest) error {
	select {
	case w.pubCh <- wh:
	default:

	}

	return nil
}

type webhookRequest struct {
	data []byte
}
