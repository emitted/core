package core

import (
	"hash/fnv"
	"strings"
	"time"
)

func parseChId(id string) (string, string) {

	arr := strings.Split(id, ":")
	app := arr[0]
	channelName := arr[1]

	return app, channelName
}

func makeChId(app string, chName string) string {
	return app + ":" + chName
}

func ChannelID(key string) string {
	switch key {
	case "pingchannel":
		return "ping"
	case "nodesysinfo":
		return "nodeproto"
	default:
		return key
	}
}

func getChannelType(ch string) string {

	switch true {
	case strings.HasPrefix(ch, "private-"):
		return "private"
	case strings.HasPrefix(ch, "presence-"):
		return "presence"
	default:
		return "public"
	}
}

func runForever(fn func()) {
	for {
		fn()

		time.Sleep(300 * time.Millisecond)
	}
}

func consistentIndex(s string, numBuckets int) int {

	hash := fnv.New64a()
	hash.Write([]byte(s))
	key := hash.Sum64()

	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int(b)
}
