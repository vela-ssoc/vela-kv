package kv

import (
	"bytes"
	"encoding/json"
	"github.com/vela-ssoc/vela-kit/hashmap"
	"github.com/vela-ssoc/vela-kit/kind"
	"github.com/vela-ssoc/vela-kit/lua"
	"net/http"
	"time"
)

type Bucket struct {
	Name  string        `json:"name"`
	Audit bool          `json:"audit"`
	TTL   time.Duration `json:"ttl"`
	Reply bool          `json:"reply"`
	Codec string        `json:"codec"`
	Max   int           `json:"max"`
}

func (b *Bucket) H() http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	return h
}

func (b *Bucket) fetch(uri string, body []byte, reply bool) *Response {
	reader := bytes.NewReader(body)
	resp := &Response{}

	r, err := xEnv.Fetch(uri, reader, b.H())
	if err != nil {
		resp.Err = err
		return resp
	}

	if reply {
		err = json.NewDecoder(r.Body).Decode(resp)
		if err != nil {
			resp.Err = err
		}

		return resp
	}

	return &Response{Element: lua.LNil}

}

func (b *Bucket) incr(key string, n int) *Response {
	uri := "/api/v1/shared/strings/incr"
	data := b.EncodeBody(func(body *kind.JsonEncoder) {
		body.KV("key", key)
		body.KV("n", n)
		body.KV("audit", b.Audit)
		body.KV("reply", b.Reply)
		body.KV("lifetime", int64(b.TTL))
	})

	return b.fetch(uri, data, b.Reply)
}

func (b *Bucket) Get(key string) *Response {
	uri := "/api/v1/shared/strings/get"
	data := b.EncodeBody(func(body *kind.JsonEncoder) {
		body.KV("key", key)
	})

	return b.fetch(uri, data, true)
}

func (b *Bucket) Set(key string, v interface{}) *Response {
	uri := "/api/v1/shared/strings/set"

	elem := &element{}
	err := elem.Ref(v)
	if err != nil {
		return &Response{Err: err}
	}

	data := b.EncodeBody(func(body *kind.JsonEncoder) {
		body.KV("key", key)
		body.Raw("value", elem.Byte())
		body.KV("lifetime", int64(b.TTL))
		body.KV("audit", b.Audit)
		body.KV("reply", b.Reply)
	})

	return b.fetch(uri, data, b.Reply)
}

func (b *Bucket) SetNoReply(key string, v interface{}) *Response {
	uri := "/api/v1/shared/strings/set"

	elem := &element{}
	err := elem.Ref(v)
	if err != nil {
		return &Response{Err: err}
	}

	data := b.EncodeBody(func(body *kind.JsonEncoder) {
		body.KV("key", key)
		body.Raw("value", elem.Byte())
		body.KV("lifetime", int64(b.TTL))
		body.KV("audit", b.Audit)
		body.KV("reply", false)
	})

	return b.fetch(uri, data, b.Reply)
}

func (b *Bucket) Store(key string, v interface{}) *Response {
	uri := "/api/v1/shared/strings/store"

	elem := &element{}
	err := elem.Ref(v)
	if err != nil {
		return &Response{Err: err}
	}

	data := b.EncodeBody(func(body *kind.JsonEncoder) {
		body.KV("key", key)
		body.Raw("value", elem.Byte())
		body.KV("lifetime", int64(b.TTL))
		body.KV("audit", b.Audit)
		body.KV("reply", b.Reply)
	})

	return b.fetch(uri, data, b.Reply)
}

func (b *Bucket) HMap(key string, tab hashmap.HMap) hashmap.HMap {
	data := b.EncodeBody(func(body *kind.JsonEncoder) {
		body.KV("key", key)
	})

	r := b.fetch("/api/v1/shared/strings/get", data, true)
	if !r.Ok() || r.Value == nil {
		b.SetNoReply(key, tab)
		return tab
	}

	var hm hashmap.HMap
	err := r.Value.Unmarshal(&hm)
	if err != nil {
		b.SetNoReply(key, tab)
		return tab
	}

	hm.Merge(tab)
	b.SetNoReply(key, hm)
	return hm
}

func (b *Bucket) Delete(key string) bool {
	if len(key) == 0 {
		return false
	}

	uri := "/api/v1/shared/strings/del"
	data := b.EncodeBody(func(body *kind.JsonEncoder) {
		body.KV("key", key)
	})

	_, err := xEnv.Fetch(uri, bytes.NewReader(data), b.H())
	if err != nil {
		return false
	}

	return true
}

func (b *Bucket) Release() bool {

	uri := "/api/v1/shared/strings/del"
	data := b.EncodeBody(func(body *kind.JsonEncoder) {
		//
	})

	_, err := xEnv.Fetch(uri, bytes.NewReader(data), b.H())
	if err != nil {
		return false
	}

	return true
}
