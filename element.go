package kv

import (
	"encoding/json"
	"fmt"
	strutil "github.com/vela-ssoc/vela-kit/auxlib"
	"github.com/vela-ssoc/vela-kit/mime"
	"github.com/vela-ssoc/vela-kit/vela"
)

type element struct {
	Size uint64 `json:"size"`
	Mime string `json:"mime"`
	Text string `json:"data"`
}

func (elem *element) set(name string, chunk []byte) {
	elem.Mime = name
	elem.Size = uint64(len(name))
	elem.Text = strutil.B2S(chunk)
}

func (elem *element) Byte() []byte {
	data, _ := json.Marshal(elem)
	return data
}

func (elem *element) String() string {
	return string(elem.Byte())
}

func (elem *element) Ref(v interface{}) error {
	chunk, name, err := mime.Encode(v)
	if err != nil {
		return err
	}
	elem.set(name, chunk)
	return nil
}

func (elem *element) Unref() (interface{}, error) {
	return elem.Decode()
}

func (elem *element) Unmarshal(v interface{}) error {

	return json.Unmarshal(strutil.S2B(elem.Text), v)
}

func (elem *element) Decode() (interface{}, error) {
	if elem.Mime == "" {
		return nil, fmt.Errorf("not found mime type")
	}

	if elem.IsNil() {
		return nil, nil
	}

	return mime.Decode(elem.Mime, strutil.S2B(elem.Text))
}

func (elem *element) IsNil() bool {
	return elem.Size == 0 || elem.Mime == vela.NIL
}
