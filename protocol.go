package core

import "github.com/sireax/core/internal/proto"

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
	PresenceRequest    = protocol.PresenceRequest
	ConnectRequest     = protocol.ConnectRequest

	ClientInfo = protocol.ClientInfo

	SubscribeResult   = protocol.SubscribeResult
	UnsubscribeResult = protocol.UnsubscribeResult
	PublishResult     = protocol.PublishResult
	PresenceResult    = protocol.PresenceResult
	ConnectResult     = protocol.ConnectResult
	PingResult        = protocol.PingResult
)

const (
	PushTypePublication = protocol.PushType_PUBLICATION
	PushTypeJoin        = protocol.PushType_JOIN
	PushTypeLeave       = protocol.PushType_LEAVE

	ClientTypeJs    = protocol.ClientType_JS
	ClientTypeSwift = protocol.ClientType_SWIFT

	MethodTypeConnect     = protocol.MethodType_CONNECT
	MethodTypePublish     = protocol.MethodType_PUBLISH
	MethodTypeSubscribe   = protocol.MethodType_SUBSCRIBE
	MethodTypeUnsubscribe = protocol.MethodType_UNSUBSCRIBE
	MethodTypePresence    = protocol.MethodType_PRESENCE
	MethodTypePing        = protocol.MethodType_PING
)
