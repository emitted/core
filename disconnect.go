package core

// Disconnect type makes it easy to disconnect
type Disconnect struct {
	Code      int    `json:"-"`
	Reason    string `json:"reason"`
	Reconnect bool   `json:"reconnect"`
}

var (
	// DisconnectNormal is clean disconnect when clientproto cleanly closed connection.
	DisconnectNormal = &Disconnect{
		Code:      3000,
		Reason:    "normal",
		Reconnect: true,
	}
	// DisconnectShutdown sent when nodeproto is going to shut down.
	DisconnectShutdown = &Disconnect{
		Code:      3001,
		Reason:    "shutdown",
		Reconnect: true,
	}
	// DisconnectBadRequest sent when clientproto uses malformed protocol
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
	// DisconnectExpired sent when clientproto connection expired.
	DisconnectExpired = &Disconnect{
		Code:      3005,
		Reason:    "expired",
		Reconnect: true,
	}
	// DisconnectStale sent to close connection that did not become
	// authenticated in configured interval after dialing.
	DisconnectStale = &Disconnect{
		Code:      3007,
		Reason:    "stale",
		Reconnect: false,
	}
	// DisconnectWriteError sent when an error occurred while writing to
	// clientproto connection.
	DisconnectWriteError = &Disconnect{
		Code:      3009,
		Reason:    "write error",
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

	DisconnectLimitExceeded = &Disconnect{
		Code:      3013,
		Reason:    "connections limit has been reached",
		Reconnect: false,
	}

	DisconnectAppInactive = &Disconnect{
		Code:      3014,
		Reason:    "this app is unavailable to connect",
		Reconnect: false,
	}
)
