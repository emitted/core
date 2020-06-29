package core

import (
	"bitbucket.org/sireax/core/common/proto/clientproto"
	"errors"
)

var (
	ErrorInternal = &clientproto.Error{
		Code:    100,
		Message: "internal server error",
	}
	// ErrUnauthorized says that request is unauthorized.
	ErrorUnauthorized = &clientproto.Error{
		Code:    101,
		Message: "unauthorized",
	}
	ErrorAlreadyAuthorized = &clientproto.Error{
		Code:    102,
		Message: "the connect command has already been sent",
	}
	// ErrorPermissionDenied means that access to resource not allowed.
	ErrorPermissionDenied = &clientproto.Error{
		Code:    103,
		Message: "permission denied",
	}
	// ErrorMethodNotFound means that method sent in command does not exist.
	ErrorMethodNotFound = &clientproto.Error{
		Code:    104,
		Message: "method not found",
	}
	// ErrorAlreadySubscribed returned when clientproto wants to subscribe on channel
	// it already subscribed to.
	ErrorAlreadySubscribed = &clientproto.Error{
		Code:    105,
		Message: "already subscribed",
	}

	ErrorChannelNotFound = &clientproto.Error{
		Code:    106,
		Message: "clientproto is not subscribed to the channel",
	}
	// ErrorLimitExceeded says that some sort of limit exceeded, server logs should
	// give more detailed information.
	ErrorMessageLimitExceeded = &clientproto.Error{
		Code:    107,
		Message: "limit exceeded",
	}
	// give more detailed information.
	ErrorChannelLimitExceeded = &clientproto.Error{
		Code:    108,
		Message: "limit exceeded",
	}
	// ErrorBadRequest says that server can not process received
	// data because it is malformed.
	ErrorBadRequest = &clientproto.Error{
		Code:    109,
		Message: "bad request",
	}

	ErrorInvalidSignature = &clientproto.Error{
		Code:    110,
		Message: "invalid signature provided",
	}

	ErrorChannelNotPresence = &clientproto.Error{
		Code:    111,
		Message: "trying to get presence on non-presence channel",
	}
)

var RedisWriteTimeoutError = errors.New("redis write timeout")
