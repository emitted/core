package nodeproto

type Encoder interface {
	EncodeCommand(*Command) ([]byte, error)
	EncodeNode(*Node) ([]byte, error)
	EncodeUnsubscribe(*Unsubscribe) ([]byte, error)
	EncodeDisconnect(*Disconnect) ([]byte, error)
}

type ProtobufEncoder struct {
}

func NewProtobufEncoder() *ProtobufEncoder {
	return &ProtobufEncoder{}
}

func (p *ProtobufEncoder) EncodeCommand(cmd *Command) ([]byte, error) {
	return cmd.Marshal()
}

func (p *ProtobufEncoder) EncodeNode(cmd *Node) ([]byte, error) {
	return cmd.Marshal()
}

func (p *ProtobufEncoder) EncodeUnsubscribe(cmd *Unsubscribe) ([]byte, error) {
	return cmd.Marshal()
}

func (p *ProtobufEncoder) EncodeDisconnect(cmd *Disconnect) ([]byte, error) {
	return cmd.Marshal()
}
