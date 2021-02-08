package validation

import (
	"github.com/emitted/core/common/proto/clientproto"
	"regexp"
)

func ChannelName(ch string) bool {
	return regexp.MustCompile(`^[a-z0-9_-]{3,16}$`).MatchString(ch) && ch != ""
}

func TopicName(topic string) bool {
	return regexp.MustCompile(`^[a-z0-9!_-]{3,16}$`).MatchString(topic) && topic != ""
}

func ClientData(data *clientproto.ClientInfo) bool {
	return data.Id != "" && data.Data != nil
}
