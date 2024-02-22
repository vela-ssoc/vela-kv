package kv

import (
	"encoding/json"
	strutil "github.com/vela-ssoc/vela-kit/auxlib"
	"github.com/vela-ssoc/vela-kit/kind"
	"github.com/vela-ssoc/vela-kit/lua"
)

type List []string

func (l List) String() string {
	chunk, _ := json.Marshal(l)
	return strutil.B2S(chunk)
}

func (l List) Type() lua.LValueType                   { return lua.LTObject }
func (l List) AssertFloat64() (float64, bool)         { return 0, false }
func (l List) AssertString() (string, bool)           { return "", false }
func (l List) AssertFunction() (*lua.LFunction, bool) { return l.ToFunc(), true }
func (l List) Peek() lua.LValue                       { return l }

func (l List) ToFunc() *lua.LFunction {
	return lua.NewFunction(l.Call)
}

func (l List) Call(L *lua.LState) int {
	idx := L.IsInt(1)

	item, ok := l.Get(idx)
	if ok {
		L.Push(lua.S2L(item))
	} else {
		L.Push(lua.LNil)
	}
	return 1
}

func (l List) Get(i int) (string, bool) {
	n := len(l)
	if i < 1 || i > n {
		return "", false
	}

	return l[i], true
}

func ListEncode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func ListDecode(data []byte) (interface{}, error) {
	var arr []string
	err := json.Unmarshal(data, &arr)
	return arr, err
}

func (l List) Have(item string) bool {
	n := len(l)
	if n == 0 {
		return false
	}

	for i := 0; i < n; i++ {
		if l[i] == item {
			return true
		}
	}
	return false
}

func (b *Bucket) List(key string, item string) List {
	data := b.EncodeBody(func(body *kind.JsonEncoder) {
		body.KV("key", key)
	})

	var arr List
	r := b.fetch("/api/v1/shared/strings/get", data, true)
	if !r.Ok() || r.Value == nil {
		arr = List{item}
		b.SetNoReply(key, arr)
		return arr
	}

	err := r.Value.Unmarshal(&arr)
	if err != nil {
		arr = List{item}
		b.SetNoReply(key, arr)
		return arr
	}

	if arr.Have(item) {
		return arr
	}

	arr = append(arr, item)
	b.SetNoReply(key, arr)

	return arr
}
