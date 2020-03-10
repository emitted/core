package core

import (
	"github.com/sireax/core/internal/proto/clientproto"
)

type (
	Raw = clientproto.Raw

	Error       = clientproto.Error
	Publication = clientproto.Publication
	Join        = clientproto.Join
	Leave       = clientproto.Leave
	Push        = clientproto.Push

	Command = clientproto.Command
	Reply   = clientproto.Reply

	PublishRequest     = clientproto.PublishRequest
	SubscribeRequest   = clientproto.SubscribeRequest
	UnsubscribeRequest = clientproto.UnsubscribeRequest
	PresenceRequest    = clientproto.PresenceRequest
	ConnectRequest     = clientproto.ConnectRequest

	ClientInfo = clientproto.ClientInfo

	SubscribeResult   = clientproto.SubscribeResult
	UnsubscribeResult = clientproto.UnsubscribeResult
	PublishResult     = clientproto.PublishResult
	PresenceResult    = clientproto.PresenceResult
	ConnectResult     = clientproto.ConnectResult
	PingResult        = clientproto.PingResult
)

const (
	PushTypePublication = clientproto.PushType_PUBLICATION
	PushTypeJoin        = clientproto.PushType_JOIN
	PushTypeLeave       = clientproto.PushType_LEAVE

	ClientTypeJs    = clientproto.ClientType_JS
	ClientTypeSwift = clientproto.ClientType_SWIFT

	MethodTypeConnect     = clientproto.MethodType_CONNECT
	MethodTypePublish     = clientproto.MethodType_PUBLISH
	MethodTypeSubscribe   = clientproto.MethodType_SUBSCRIBE
	MethodTypeUnsubscribe = clientproto.MethodType_UNSUBSCRIBE
	MethodTypePresence    = clientproto.MethodType_PRESENCE
	MethodTypePing        = clientproto.MethodType_PING
)
