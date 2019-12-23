package api

import (
	"encoding/json"
	"log"
)

// AppInfoResponse ...
type AppInfoResponse struct {
	Key    string `json:"key"`
	Secret string `json:"secret"`

	Stats stats `json:"stats"`
}

// NewAppInfoResponse ...
func NewAppInfoResponse(key string, secret string, connections int, messages int) *AppInfoResponse {
	return &AppInfoResponse{
		Key:    key,
		Secret: secret,
		Stats: stats{
			Connections: connections,
			Messages:    messages,
		},
	}
}

func (res *AppInfoResponse) Marshal() []byte  {
	data, err := json.Marshal(res)
	if err != nil {
		log.Fatal("something wrong with the api...")
	}

	return data
}

type stats struct {
	Connections int `json:"connections"`
	Messages    int `json:"messages"`
}

// ChannelInfoResponse ...
type ChannelInfoResponse struct {
	Name  string
	Stats stats
}
