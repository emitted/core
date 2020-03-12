package core

import (
	//"github.com/pkg/errors"
	"github.com/Shopify/sarama"
	"github.com/sireax/core/internal/proto/webhooks"
	//"github.com/sireax/core/internal/timers"
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

	//dialer := &kafka.Dialer{
	//	Timeout:  10 * time.Second,
	//	ClientID: w.node.uid,
	//}

	//config := kafka.WriterConfig{
	//	Brokers:          []string{w.config.address},
	//	Topic:            w.config.topic,
	//	Balancer:         &kafka.LeastBytes{},
	//	Dialer:           dialer,
	//	WriteTimeout:     10 * time.Second,
	//	ReadTimeout:      10 * time.Second,
	//	CompressionCodec: snappy.NewCompressionCodec(),
	//}

	config := sarama.NewConfig()
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

	//pingTicker := time.NewTicker(time.Second)
	//defer pingTicker.Stop()

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

					r.done(nil)
				}
			}
		}()
	}
}

func (w *webhookManager) Enqueue(wh webhookRequest) error {
	select {
	case w.pubCh <- wh:
	default:

	}

	return wh.result()
}

type webhookRequest struct {
	data []byte
	err  chan error
}

func (r *webhookRequest) done(err error) {
	r.err <- err
}

func (r *webhookRequest) result() error {
	return <-r.err
}

func newChannelOccupiedWebhook(appId, signature, url string, data []byte) Webhook {
	return Webhook{
		Event:     WebhookEventChannelOccupied,
		Id:        23412,
		AppId:     appId,
		Signature: signature,
		Data:      data,
	}
}

func newChannelVacatedWebhook(appId, signature, url string, data []byte) Webhook {
	return Webhook{
		Event:     WebhookEventChannelVacated,
		Id:        23412,
		AppId:     appId,
		Signature: signature,
		Data:      data,
	}
}

func newJoinWebhook(appId, signature, url string, data []byte) Webhook {
	return Webhook{
		Event:     WebhookEventJoin,
		Id:        23412,
		AppId:     appId,
		Signature: signature,
		Data:      data,
	}
}

func newPresenceRemovedWebhook(appId, signature, url string, data []byte) Webhook {
	return Webhook{
		Event:     WebhookEventJoin,
		Id:        23412,
		AppId:     appId,
		Signature: signature,
		Data:      data,
	}
}

func newPublicationWebhook(appId, signature, url string, data []byte) Webhook {
	return Webhook{
		Event:     WebhookEventPublication,
		Id:        23412,
		AppId:     appId,
		Signature: signature,
		Data:      data,
	}
}
