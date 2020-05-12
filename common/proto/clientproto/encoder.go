package clientproto

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/proto"
)

// EventEncoder ...
type EventEncoder interface {
	Encode(*Event) ([]byte, error)
	EncodePublication(*Publication) ([]byte, error)
	EncodeJoin(*Join) ([]byte, error)
	EncodeLeave(*Leave) ([]byte, error)
}

// JSONEventEncoder ...
type JSONEventEncoder struct {
}

// NewJSONEventEncoder ...
func NewJSONEventEncoder() *JSONEventEncoder {
	return &JSONEventEncoder{}
}

// Encode ...
func (e *JSONEventEncoder) Encode(message *Event) ([]byte, error) {
	return json.Marshal(message)
}

// EncodePublication ...
func (e *JSONEventEncoder) EncodePublication(message *Publication) ([]byte, error) {
	return json.Marshal(message)
}

// EncodeJoin ...
func (e *JSONEventEncoder) EncodeJoin(message *Join) ([]byte, error) {
	return json.Marshal(message)
}

// EncodeLeave ...
func (e *JSONEventEncoder) EncodeLeave(message *Leave) ([]byte, error) {
	return json.Marshal(message)
}

// ProtobufEventEncoder ...
type ProtobufEventEncoder struct {
}

// NewProtobufEventEncoder ...
func NewProtobufEventEncoder() *ProtobufEventEncoder {
	return &ProtobufEventEncoder{}
}

// Encode ...
func (e *ProtobufEventEncoder) Encode(message *Event) ([]byte, error) {
	return message.Marshal()
}

// EncodePublication ...
func (e *ProtobufEventEncoder) EncodePublication(message *Publication) ([]byte, error) {
	return message.Marshal()
}

// EncodeJoin ...
func (e *ProtobufEventEncoder) EncodeJoin(message *Join) ([]byte, error) {
	return message.Marshal()
}

// EncodeLeave ...
func (e *ProtobufEventEncoder) EncodeLeave(message *Leave) ([]byte, error) {
	return message.Marshal()
}

// ReplyEncoder ...
type ReplyEncoder interface {
	Reset()
	Encode(*Reply) error
	Finish() []byte
}

// ProtobufReplyEncoder ...
type ProtobufReplyEncoder struct {
	buffer bytes.Buffer
}

// NewProtobufReplyEncoder ...
func NewProtobufReplyEncoder() *ProtobufReplyEncoder {
	return &ProtobufReplyEncoder{}
}

// Encode ...
func (e *ProtobufReplyEncoder) Encode(r *Reply) error {
	replyBytes, err := r.Marshal()
	if err != nil {
		return err
	}
	bs := make([]byte, 8)
	n := binary.PutUvarint(bs, uint64(len(replyBytes)))
	e.buffer.Write(bs[:n])
	e.buffer.Write(replyBytes)
	return nil
}

// Reset ...
func (e *ProtobufReplyEncoder) Reset() {
	e.buffer.Reset()
}

// Finish ...
func (e *ProtobufReplyEncoder) Finish() []byte {
	data := e.buffer.Bytes()
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return dataCopy
}

// ResultEncoder ...
type ResultEncoder interface {
	EncodeConnectResult(*ConnectResult) ([]byte, error)
	EncodeSubscribeResult(*SubscribeResult) ([]byte, error)
	EncodeUnsubscribeResult(*UnsubscribeResult) ([]byte, error)
	EncodePublishResult(*PublishResult) ([]byte, error)
	EncodePresenceResult(*PresenceResult) ([]byte, error)
	EncodePingResult(*PingResult) ([]byte, error)
}

// ProtobufResultEncoder ...
type ProtobufResultEncoder struct{}

// NewProtobufResultEncoder ...
func NewProtobufResultEncoder() *ProtobufResultEncoder {
	return &ProtobufResultEncoder{}
}

// EncodeConnectResult ...
func (e *ProtobufResultEncoder) EncodeConnectResult(res *ConnectResult) ([]byte, error) {
	return res.Marshal()
}

// EncodeSubscribeResult ...
func (e *ProtobufResultEncoder) EncodeSubscribeResult(res *SubscribeResult) ([]byte, error) {
	return res.Marshal()
}

// EncodeUnsubscribeResult ...
func (e *ProtobufResultEncoder) EncodeUnsubscribeResult(res *UnsubscribeResult) ([]byte, error) {
	return res.Marshal()
}

// EncodePublishResult ...
func (e *ProtobufResultEncoder) EncodePublishResult(res *PublishResult) ([]byte, error) {
	return res.Marshal()
}

// EncodePresenceResult ...
func (e *ProtobufResultEncoder) EncodePresenceResult(res *PresenceResult) ([]byte, error) {
	return res.Marshal()
}

// EncodePingResult ...
func (e *ProtobufResultEncoder) EncodePingResult(res *PingResult) ([]byte, error) {
	return res.Marshal()
}

// CommandEncoder ...
type CommandEncoder interface {
	Encode(cmd *Command) ([]byte, error)
}

// JSONCommandEncoder ...
type JSONCommandEncoder struct {
}

// NewJSONCommandEncoder ...
func NewJSONCommandEncoder() *JSONCommandEncoder {
	return &JSONCommandEncoder{}
}

// Encode ...
func (e *JSONCommandEncoder) Encode(cmd *Command) ([]byte, error) {
	return json.Marshal(cmd)
}

// ProtobufCommandEncoder ...
type ProtobufCommandEncoder struct {
}

// NewProtobufCommandEncoder ...
func NewProtobufCommandEncoder() *ProtobufCommandEncoder {
	return &ProtobufCommandEncoder{}
}

// Encode ...
func (e *ProtobufCommandEncoder) Encode(cmd *Command) ([]byte, error) {
	commandBytes, err := cmd.Marshal()
	if err != nil {
		return nil, err
	}
	bs := make([]byte, 8)
	n := binary.PutUvarint(bs, uint64(len(commandBytes)))
	var buf bytes.Buffer
	buf.Write(bs[:n])
	buf.Write(commandBytes)
	return buf.Bytes(), nil
}

// ParamsEncoder ...
type ParamsEncoder interface {
	Encode(request interface{}) ([]byte, error)
}

// JSONParamsEncoder ...
type JSONParamsEncoder struct{}

// NewJSONParamsEncoder ...
func NewJSONParamsEncoder() *JSONParamsEncoder {
	return &JSONParamsEncoder{}
}

// Encode ...
func (d *JSONParamsEncoder) Encode(r interface{}) ([]byte, error) {
	return json.Marshal(r)
}

// ProtobufParamsEncoder ...
type ProtobufParamsEncoder struct{}

// NewProtobufParamsEncoder ...
func NewProtobufParamsEncoder() *ProtobufParamsEncoder {
	return &ProtobufParamsEncoder{}
}

// Encode ...
func (d *ProtobufParamsEncoder) Encode(r interface{}) ([]byte, error) {
	m, ok := r.(proto.Marshaler)
	if !ok {
		return nil, fmt.Errorf("can not marshal type %T to Protobuf", r)
	}
	return m.Marshal()
}
