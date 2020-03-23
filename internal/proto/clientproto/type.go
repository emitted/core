package clientproto

import "sync"

var protobufReplyEncoderPool sync.Pool

func GetReplyEncoder() ReplyEncoder {
	e := protobufReplyEncoderPool.Get()
	if e == nil {
		return NewProtobufReplyEncoder()
	}
	protoTypeoder := e.(ReplyEncoder)
	protoTypeoder.Reset()
	return protoTypeoder
}

// PutReplyEncoder ...
func PutReplyEncoder(e ReplyEncoder) {
	protobufReplyEncoderPool.Put(e)
}

// GetCommandDecoder ...
func GetCommandDecoder(data []byte) CommandDecoder {
	return NewProtobufCommandDecoder(data)
}

// PutCommandDecoder ...
func PutCommandDecoder(e CommandDecoder) {
	return
}

// GetResultEncoder ...
func GetResultEncoder() ResultEncoder {
	return NewProtobufResultEncoder()
}

// PutResultEncoder ...
func PutResultEncoder(e ReplyEncoder) {
	return
}

// GetParamsDecoder ...
func GetParamsDecoder() ParamsDecoder {
	return NewProtobufParamsDecoder()
}

// PutParamsDecoder ...
func PutParamsDecoder(e ParamsDecoder) {
	return
}
