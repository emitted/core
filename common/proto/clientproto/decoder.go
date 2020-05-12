package clientproto

import (
	"encoding/binary"
	"io"
	"log"
)

// EventDecoder ...
type EventDecoder interface {
	Decode([]byte) (*Event, error)
	DecodePublication([]byte) (*Publication, error)
	DecodeJoin([]byte) (*Join, error)
	DecodeLeave([]byte) (*Leave, error)
}

type ProtobufEventDecoder struct {
}

// Decode ...
func (e *ProtobufEventDecoder) Decode(data []byte) (*Event, error) {
	var m Event
	err := m.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DecodePublication ...
func (e *ProtobufEventDecoder) DecodePublication(data []byte) (*Publication, error) {
	var m Publication
	err := m.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DecodeJoin ...
func (e *ProtobufEventDecoder) DecodeJoin(data []byte) (*Join, error) {
	var m Join
	err := m.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DecodeLeave  ...
func (e *ProtobufEventDecoder) DecodeLeave(data []byte) (*Leave, error) {
	var m Leave
	err := m.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// CommandDecoder ...
type CommandDecoder interface {
	Reset([]byte) error
	Decode() (*Command, error)
}

// ProtobufCommandDecoder ...
type ProtobufCommandDecoder struct {
	data   []byte
	offset int
}

// NewProtobufCommandDecoder ...
func NewProtobufCommandDecoder(data []byte) *ProtobufCommandDecoder {
	return &ProtobufCommandDecoder{
		data: data,
	}
}

// Reset ...
func (d *ProtobufCommandDecoder) Reset(data []byte) error {
	d.data = data
	d.offset = 0
	return nil
}

// Decode ...
func (d *ProtobufCommandDecoder) Decode() (*Command, error) {
	if d.offset < len(d.data) {
		var c Command
		l, n := binary.Uvarint(d.data[d.offset:])
		cmdBytes := d.data[d.offset+n : d.offset+n+int(l)]
		err := c.Unmarshal(cmdBytes)
		if err != nil {
			return nil, err
		}
		d.offset = d.offset + n + int(l)
		return &c, nil
	}
	return nil, io.EOF
}

// ParamsDecoder ...
type ParamsDecoder interface {
	DecodeSubscribe([]byte) (*SubscribeRequest, error)
	DecodeUnsubscribe([]byte) (*UnsubscribeRequest, error)
	DecodePublish([]byte) (*PublishRequest, error)
	//DecodePresence([]byte) (*PresenceRequest, error)
	//DecodePresenceStats([]byte) (*PresenceStatsRequest, error)
	DecodePing([]byte) (*PingRequest, error)
}

type ProtobufParamsDecoder struct{}

func NewProtobufParamsDecoder() *ProtobufParamsDecoder {
	return &ProtobufParamsDecoder{}
}

func (d *ProtobufParamsDecoder) DecodeConnect(data []byte) (*ConnectRequest, error) {
	var p ConnectRequest
	if data != nil {
		err := p.Unmarshal(data)
		if err != nil {
			return nil, err
		}
	}
	return &p, nil
}

// DecodeSubscribe ...
func (d *ProtobufParamsDecoder) DecodeSubscribe(data []byte) (*SubscribeRequest, error) {
	var p SubscribeRequest
	err := p.Unmarshal(data)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return &p, nil
}

// DecodeUnsubscribe ...
func (d *ProtobufParamsDecoder) DecodeUnsubscribe(data []byte) (*UnsubscribeRequest, error) {
	var p UnsubscribeRequest
	err := p.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// DecodePublish ...
func (d *ProtobufParamsDecoder) DecodePublish(data []byte) (*PublishRequest, error) {
	var p PublishRequest
	err := p.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

//DecodePresence ...
func (d *ProtobufParamsDecoder) DecodePresence(data []byte) (*PresenceRequest, error) {
	var p PresenceRequest
	err := p.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// DecodePresenceStats ...
//func (d *ProtobufParamsDecoder) DecodePresenceStats(data []byte) (*PresenceStatsRequest, error) {
//	var p PresenceStatsRequest
//	err := p.Unmarshal(data)
//	if err != nil {
//		return nil, err
//	}
//	return &p, nil
//}

// DecodePing ...
func (d *ProtobufParamsDecoder) DecodePing(data []byte) (*PingRequest, error) {
	var p PingRequest
	err := p.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return &p, nil
}
