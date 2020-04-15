package core

import "github.com/prometheus/client_golang/prometheus"

var metricsNamespace = "emitted"

var (
	messagesSentCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "messages_sent_count",
	}, []string{"type"})

	messagesReceivedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "messages_received_count",
	}, []string{"type"})

	numClientsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "num_clients",
	})

	numChannelsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "num_channels",
	})
)

var (
	messagesReceivedCountPublication prometheus.Counter
	messagesReceivedCountJoin        prometheus.Counter
	messagesReceivedCountLeave       prometheus.Counter
	messagesReceivedCountControl     prometheus.Counter

	messagesSentCountPublication prometheus.Counter
	messagesSentCountJoin        prometheus.Counter
	messagesSentCountLeave       prometheus.Counter
	messagesSentCountControl     prometheus.Counter
)

func init() {
	prometheus.MustRegister(messagesSentCount)
	prometheus.MustRegister(messagesReceivedCount)
	prometheus.MustRegister(numClientsGauge)
	prometheus.MustRegister(numChannelsGauge)

	messagesReceivedCountPublication = messagesReceivedCount.WithLabelValues("publication")
	messagesReceivedCountJoin = messagesReceivedCount.WithLabelValues("join")
	messagesReceivedCountLeave = messagesReceivedCount.WithLabelValues("leave")
	messagesReceivedCountControl = messagesReceivedCount.WithLabelValues("control")

	messagesSentCountPublication = messagesSentCount.WithLabelValues("publication")
	messagesSentCountJoin = messagesReceivedCount.WithLabelValues("join")
	messagesSentCountLeave = messagesReceivedCount.WithLabelValues("leave")
	messagesSentCountControl = messagesReceivedCount.WithLabelValues("control")
}
