package main

import (
	"strings"
	"time"
)

func parseChId(id string) (string, string) {

	arr := strings.Split(id, ":")
	appKey := arr[0]
	channelName := arr[1]

	return appKey, channelName
}

func makeChId(appKey string, chName string) string  {
	return appKey + ":" + chName
}

func ChannelID(key string) string {
	switch key {
	case "PING_TEST_CHANNEL":
		return "ping"
	default:
		return key
	}
}

func runForever(fn func())  {
	for {
		fn()

		time.Sleep(300 * time.Millisecond)
	}
}
