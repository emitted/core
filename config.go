package main

import "time"

type Config struct {
	Version               string
	Name                  string
	ClientChannelLimit    int
	ClientStaleCloseDelay time.Duration

	NodeInfoMetricsAggregateInterval time.Duration

	LogLevel   LogLevel
	LogHandler LogHandler
}

func (c *Config) Validate() {

}

var DefaultConfig = Config{
	Name: "emitted",

	NodeInfoMetricsAggregateInterval: 60 * time.Second,

	ClientStaleCloseDelay: 25 * time.Second,
	ClientChannelLimit:    128,
}
