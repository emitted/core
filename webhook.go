package core

import (
	"github.com/Shopify/sarama"
	"github.com/emitted/core/common/proto/webhooks"
	"time"
)

type (
	Webhook = webhooks.Webhook
)

type KafkaConfig struct {
	Protocol, Address, Topic string
	Partition                int

	WriteTimeout int
}

var DefaultKafkaConfig = KafkaConfig{
	Protocol:     "tcp",
	Address:      "localhost:9092",
	Topic:        "emitted-server-webhooks",
	Partition:    0,
	WriteTimeout: 1,
}

type webhookManager struct {
	node   *Node
	config KafkaConfig

	pubCh chan webhookRequest

	producer sarama.AsyncProducer
}

func NewWebhookManager(node *Node, config KafkaConfig) *webhookManager {
	return &webhookManager{
		node:   node,
		config: config,

		pubCh: make(chan webhookRequest),
	}
}

func (w *webhookManager) Run() error {

	config := sarama.NewConfig()

	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Timeout = 5 * time.Second
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer([]string{w.config.Address}, config)
	if err != nil {
		return nil
	}

	w.producer = producer

	go w.runProducePipeline()

	return nil
}

func (w *webhookManager) runProducePipeline() {

	go func() {
		for {
			select {

			case <-w.node.NotifyShutdown():
				return

			case r := <-w.pubCh:

				msg := &sarama.ProducerMessage{
					Topic:     "emitted-server-webhooks",
					Value:     sarama.ByteEncoder(r.data),
					Timestamp: time.Now(),
				}

				select {
				case w.producer.Input() <- msg:
					w.node.logger.log(NewLogEntry(LogLevelDebug, "producing webhooks", map[string]interface{}{"msg": msg}))
				}

			}

		}
	}()

	for {
		select {
		case err := <-w.producer.Errors():
			w.node.logger.log(NewLogEntry(LogLevelError, "failed to produce webhooks", map[string]interface{}{"error": err.Error()}))
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
