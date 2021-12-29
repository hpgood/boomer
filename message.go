package boomer

import (
	"github.com/ugorji/go/codec"
)

var (
	mh codec.MsgpackHandle
)

type message struct {
	Type   string                 `codec:"type"`
	Data   map[string]interface{} `codec:"data"`
	NodeID string                 `codec:"node_id"`
}
type messageV2 struct {
	Type   string `codec:"type"`
	Data   string `codec:"data"`
	NodeID string `codec:"node_id"`
}

func newMessage(t string, data map[string]interface{}, nodeID string) (msg *message) {
	return &message{
		Type:   t,
		Data:   data,
		NodeID: nodeID,
	}
}

func (m *message) serialize() (out []byte, err error) {
	mh.StructToArray = true
	enc := codec.NewEncoderBytes(&out, &mh)
	if m.Type == "client_ready" {
		err = enc.Encode(messageV2{m.Type, defaultBoomer.version, m.NodeID})
	} else {
		err = enc.Encode(m)
	}

	return out, err
}

func newMessageFromBytes(raw []byte) (newMsg *message, err error) {
	mh.StructToArray = true
	dec := codec.NewDecoderBytes(raw, &mh)
	newMsg = &message{}
	err = dec.Decode(newMsg)
	return newMsg, err
}
