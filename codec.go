package kv

import "github.com/vela-ssoc/vela-kit/kind"

func (b *Bucket) EncodeBody(callback ...kind.EncodeJsonCallback) []byte {
	body := kind.NewJsonEncoder()
	body.Tab("")
	body.KV("bucket", b.Name)
	for _, fn := range callback {
		fn(body)
	}
	body.End("}")
	return body.Bytes()
}
