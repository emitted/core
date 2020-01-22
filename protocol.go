package main

import protocol "github.com/sireax/emitted/internal/proto"

type (
	Raw = protocol.Raw

	Error       = protocol.Error
	Publication = protocol.Publication
	Join        = protocol.Join
	Leave       = protocol.Leave
	Push        = protocol.Push

	Command = protocol.Command
	Reply   = protocol.Reply

	PublishRequest     = protocol.PublishRequest
	SubscribeRequest   = protocol.SubscribeRequest
	UnsubscribeRequest = protocol.UnsubscribeRequest

	ClientInfo = protocol.ClientInfo
)

const (
	PushTypePublication = protocol.PushType_PUBLICATION
	PushTypeJoin        = protocol.PushType_JOIN
	PushTypeLeave       = protocol.PushType_LEAVE

	MethodTypePublish     = protocol.MethodType_PUBLISH
	MethodTypeSubscribe   = protocol.MethodType_SUBSCRIBE
	MethodTypeUnsubscribe = protocol.MethodType_UNSUBSCRIBE
	MethodTypePresence    = protocol.MethodType_PRESENCE
	MethodTypePing        = protocol.MethodType_PING
)
