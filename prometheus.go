package core

import "github.com/prometheus/client_golang/prometheus"

const MetricsCommandTypeConnect = "connect"
const MetricsCommandTypePublish = "publish"
const MetricsCommandTypeSubscribe = "subscribe"
const MetricsCommandTypeUnsubscribe = "unsubscribe"
const MetricsCommandTypePresence = "presence"
const MetricsCommandTypePing = "ping"

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

	commandsDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "commands_duration_seconds",
			Buckets: []float64{1, 2, 5, 10, 20, 60},
		},
		[]string{"command_type"},
	)
)

var (
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

	prometheus.MustRegister(commandsDurationHistogram)

	messagesSentCountPublication = messagesSentCount.WithLabelValues("publication")
	messagesSentCountJoin = messagesReceivedCount.WithLabelValues("join")
	messagesSentCountLeave = messagesReceivedCount.WithLabelValues("leave")
	messagesSentCountControl = messagesReceivedCount.WithLabelValues("control")
}
