package core

import (
	"context"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
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

	writer *kafka.Writer
}

func NewWebhookManager(node *Node, config webhookConfig) *webhookManager {
	return &webhookManager{
		node:   node,
		config: config,

		pubCh: make(chan webhookRequest),
	}
}

func (w *webhookManager) Run() error {

	dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: w.node.uid,
	}

	config := kafka.WriterConfig{
		Brokers:          []string{w.config.address},
		Topic:            w.config.topic,
		Balancer:         &kafka.LeastBytes{},
		Dialer:           dialer,
		WriteTimeout:     10 * time.Second,
		ReadTimeout:      10 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(),
	}
	w.writer = kafka.NewWriter(config)

	go runForever(func() {
		w.runProducePipeline()
	})

	w.node.logger.log(newLogEntry(LogLevelInfo, "webhook dispatcher has been started"))

	return nil
}

func (w *webhookManager) runProducePipeline() {
	var whpr []webhookRequest

	//pingTicker := time.NewTicker(time.Second)
	//defer pingTicker.Stop()

	select {
	case <-w.node.NotifyShutdown():
		return
	default:
	}

	for {
		select {
		case r := <-w.pubCh:

			whpr = append(whpr, r)

		loop:
			for len(whpr) < 512 {
				select {
				case r := <-w.pubCh:
					whpr = append(whpr, r)
				default:
					break loop
				}
			}
			for i := range whpr {

				err := w.writer.WriteMessages(context.Background(), kafka.Message{
					Key:   nil,
					Value: whpr[i].data,
					Time:  time.Now(),
				})

				if err != nil {
					w.node.logger.log(NewLogEntry(LogLevelError, "error producing webhook", map[string]interface{}{"error": err.Error()}))
					whpr[i].done(err)
					continue
				}

				w.node.logger.log(NewLogEntry(LogLevelInfo, "just produced webhook ;)"))

				whpr[i].done(nil)
			}
			whpr = nil
		}
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
