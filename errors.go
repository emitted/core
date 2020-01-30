package main

var (
	ErrorInternal = &Error{
		Code:    100,
		Message: "internal server error",
	}
	// ErrUnauthorized says that request is unauthorized.
	ErrorUnauthorized = &Error{
		Code:    101,
		Message: "unauthorized",
	}
	// ErrorPermissionDenied means that access to resource not allowed.
	ErrorPermissionDenied = &Error{
		Code:    102,
		Message: "permission denied",
	}
	// ErrorMethodNotFound means that method sent in command does not exist.
	ErrorMethodNotFound = &Error{
		Code:    103,
		Message: "method not found",
	}
	// ErrorAlreadySubscribed returned when client wants to subscribe on channel
	// it already subscribed to.
	ErrorAlreadySubscribed = &Error{
		Code:    104,
		Message: "already subscribed",
	}

	ErrorChannelNotFound = &Error{
		Code:    105,
		Message: "client is not subscribed to the channel",
	}
	// ErrorLimitExceeded says that some sort of limit exceeded, server logs should
	// give more detailed information.
	ErrorMessageLimitExceeded = &Error{
		Code:    106,
		Message: "limit exceeded",
	}
	// give more detailed information.
	ErrorConnectionLimitExceeded = &Error{
		Code:    107,
		Message: "limit exceeded",
	}
	// ErrorBadRequest says that server can not process received
	// data because it is malformed.
	ErrorBadRequest = &Error{
		Code:    108,
		Message: "bad request",
	}

	ErrorInvalidSignature = &Error{
		Code:    109,
		Message: "invalid signature provided",
	}
)
