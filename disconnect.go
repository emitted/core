package main

// Disconnect type makes it easy to disconnect
type Disconnect struct {
	Code      int    `json:"-"`
	Reason    string `json:"reason"`
	Reconnect bool   `json:"reconnect"`
}

var (
	// DisconnectNormal is clean disconnect when client cleanly closed connection.
	DisconnectNormal = &Disconnect{
		Code:      3000,
		Reason:    "normal",
		Reconnect: true,
	}
	// DisconnectShutdown sent when node is going to shut down.
	DisconnectShutdown = &Disconnect{
		Code:      3001,
		Reason:    "shutdown",
		Reconnect: true,
	}
	// DisconnectTunnelNotFound sent when client came with invalid token.
	DisconnectTunnelNotFound = &Disconnect{
		Code:      3002,
		Reason:    "tunnel is not found",
		Reconnect: false,
	}
	// DisconnectBadRequest sent when client uses malformed protocol
	// frames or wrong order of commands.
	DisconnectBadRequest = &Disconnect{
		Code:      3003,
		Reason:    "bad request",
		Reconnect: false,
	}
	// DisconnectServerError sent when internal error occurred on server.
	DisconnectServerError = &Disconnect{
		Code:      3004,
		Reason:    "internal server error",
		Reconnect: true,
	}
	// DisconnectExpired sent when client connection expired.
	DisconnectExpired = &Disconnect{
		Code:      3005,
		Reason:    "expired",
		Reconnect: true,
	}
	// DisconnectSubExpired sent when client subscription expired.
	DisconnectSubExpired = &Disconnect{
		Code:      3006,
		Reason:    "subscription expired",
		Reconnect: true,
	}
	// DisconnectStale sent to close connection that did not become
	// authenticated in configured interval after dialing.
	DisconnectStale = &Disconnect{
		Code:      3007,
		Reason:    "stale",
		Reconnect: false,
	}
	// DisconnectSlow sent when client can't read subMessages fast enough.
	DisconnectSlow = &Disconnect{
		Code:      3008,
		Reason:    "slow",
		Reconnect: true,
	}
	// DisconnectWriteError sent when an error occurred while writing to
	// client connection.
	DisconnectWriteError = &Disconnect{
		Code:      3009,
		Reason:    "write error",
		Reconnect: true,
	}
	// DisconnectInsufficientState sent when server detects wrong client
	// position in channel Publication stream. Disconnect allows client
	// to restore missed publications on reconnect.
	DisconnectInsufficientState = &Disconnect{
		Code:      3010,
		Reason:    "insufficient state",
		Reconnect: true,
	}
	// DisconnectForceReconnect sent when server forcely disconnects connection.
	DisconnectForceReconnect = &Disconnect{
		Code:      3011,
		Reason:    "force reconnect",
		Reconnect: true,
	}
	// DisconnectForceNoReconnect sent when server forcely disconnects connection
	// and asks it to not reconnect again.
	DisconnectForceNoReconnect = &Disconnect{
		Code:      3012,
		Reason:    "force disconnect",
		Reconnect: false,
	}
)
