package errors

// ErrTunnelNotFound is an error
type ErrTunnelNotFound struct {
	message string
}

// NewErrTunnelNotFound ...
func NewErrTunnelNotFound() *ErrTunnelNotFound {
	return &ErrTunnelNotFound{
		message: "tunnel is not found",
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
func NewErrConnLimitReached() *ErrConnLimitReached {
	return &ErrConnLimitReached{
		message: "tunnel connections limit reached",
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
func NewErrMessagesLimitReached() *ErrMessagesLimitReached {
	return &ErrMessagesLimitReached{
		message: "tunnel messages limit reached",
	}
}

func (err *ErrMessagesLimitReached) Error() string {
	return err.message
}
