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

func init() {
	prometheus.MustRegister(messagesSentCount)
	prometheus.MustRegister(messagesReceivedCount)
	prometheus.MustRegister(numClientsGauge)
	prometheus.MustRegister(numChannelsGauge)

}
