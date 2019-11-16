package errors

// ErrTunnelNotFound is an error
type ErrTunnelNotFound struct {
	message string
}

// NewErrTunnelNotFound ...
func NewErrTunnelNotFound(message string) *ErrTunnelNotFound {
	return &ErrTunnelNotFound{
		message: message,
	}
}

func (err *ErrTunnelNotFound) Error() string {
	return err.message
}

// ErrConnLimitReached ...
type ErrConnLimitReached struct {
	message string
}

// NewErrConnLimitReached ...
func NewErrConnLimitReached(message string) *ErrConnLimitReached {
	return &ErrConnLimitReached{
		message: message,
	}
}

func (err *ErrConnLimitReached) Error() string {
	return err.message
}

// ErrMessagesLimitReached ...
type ErrMessagesLimitReached struct {
	message string
}

// NewErrMessagesLimitReached ...
func NewErrMessagesLimitReached(message string) *ErrMessagesLimitReached {
	return &ErrMessagesLimitReached{
		message: message,
	}
}

func (err *ErrMessagesLimitReached) Error() string {
	return err.message
}
