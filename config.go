package core

import "time"

type Config struct {
	Version               string
	Name                  string
	ClientChannelLimit    int
	ClientStaleCloseDelay time.Duration

	ChannelMaxLength   int
	ClientQueueMaxSize int

	NodeInfoMetricsAggregateInterval time.Duration

	LogLevel   LogLevel
	LogHandler LogHandler

	BrokerShardUnavailabilityTimeout time.Duration
	BrokerShardHealthCheckInterval   time.Duration
}

var DefaultConfig = Config{
	Name: "emitted",

	NodeInfoMetricsAggregateInterval: 60 * time.Second,

	ClientStaleCloseDelay: 25 * time.Second,
	ClientChannelLimit:    128,
}
